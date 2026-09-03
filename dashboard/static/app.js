"use strict";

// ---- config ----
const MAP_CENTER = [40.73, -73.94]; // NYC, framed to show all boroughs
const MAP_ZOOM = 11;
const MAX_ARRIVALS = 25;
const ARRIVING_THRESHOLD_S = 30;
const SEC_PER_MIN = 60;

const RECONNECT_BASE_MS = 800;
const RECONNECT_MAX_MS = 8000;

// NYC subway line colors, keyed by route_id. Bus routes fall back to a color
// derived from the route line geometry (/api/shapes) or a hash of the name.
const ROUTE_COLORS = {
  "1": "#EE352E", "2": "#EE352E", "3": "#EE352E",
  "4": "#00933C", "5": "#00933C", "6": "#00933C",
  "7": "#B933AD",
  "A": "#0039A6", "C": "#0039A6", "E": "#0039A6",
  "B": "#FF6319", "D": "#FF6319", "F": "#FF6319", "M": "#FF6319",
  "N": "#FCCC0A", "Q": "#FCCC0A", "R": "#FCCC0A", "W": "#FCCC0A",
  "G": "#6CBE45",
  "J": "#996633", "Z": "#996633",
  "L": "#A7A9AC",
  "S": "#808183",
};
const DEFAULT_ROUTE_COLOR = "#3aa0ff";

// Colors learned from the shapes feed (route_short -> hex), plus a stable hash
// fallback so every bus route gets a consistent color.
const shapeColors = {};

function hashColor(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) | 0;
  const hue = Math.abs(h) % 360;
  return `hsl(${hue}, 70%, 55%)`;
}

function routeColor(routeShort) {
  const id = String(routeShort || "").toUpperCase();
  if (ROUTE_COLORS[id]) return ROUTE_COLORS[id];
  if (shapeColors[id]) return shapeColors[id];
  if (id) return hashColor(id);
  return DEFAULT_ROUTE_COLOR;
}

function textColorFor(hex) {
  if (!hex || hex[0] !== "#") return "#ffffff"; // hsl() -> assume light text ok
  const c = hex.replace("#", "");
  const r = parseInt(c.slice(0, 2), 16);
  const g = parseInt(c.slice(2, 4), 16);
  const b = parseInt(c.slice(4, 6), 16);
  const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return luminance > 0.62 ? "#000000" : "#ffffff";
}

// ---- filter state ----
const filters = {
  subway: true,
  bus: true,
  route: "",       // "" = all routes; else a route_short
  showShapes: true,
};
const knownRoutes = { subway: new Set(), bus: new Set() };

function trainVisible(t) {
  if (t.mode === "subway" && !filters.subway) return false;
  if (t.mode === "bus" && !filters.bus) return false;
  if (filters.route && String(t.route_short).toUpperCase() !== filters.route.toUpperCase()) return false;
  return true;
}

// ---- map setup ----
const map = L.map("map", {center: MAP_CENTER, zoom: MAP_ZOOM, zoomControl: true});
L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
  attribution:
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
  subdomains: "abcd",
  maxZoom: 19,
}).addTo(map);

const shapeLayer = L.layerGroup().addTo(map);   // route-line polylines
const vehicleLayer = L.layerGroup().addTo(map); // train/bus markers
const alertLayer = L.layerGroup().addTo(map);   // alert markers
const proposedLayer = L.layerGroup().addTo(map); // route-designer proposal

// trip_key -> {marker, mode, route} so we can move markers instead of redrawing
// (smooth movement, the way a transit map should feel).
const vehicleMarkers = new Map();
// route_short -> [polyline,...]
const shapePolylines = {};

// ---- icons ----
function subwayIcon(routeShort) {
  const bg = routeColor(routeShort);
  const fg = textColorFor(bg);
  const label = String(routeShort || "?").slice(0, 2);
  return L.divIcon({
    className: "veh-icon",
    html: `<span class="bullet subway" style="background:${bg};color:${fg}">${label}</span>`,
    iconSize: [22, 22],
    iconAnchor: [11, 11],
  });
}

function busIcon(routeShort, bearing) {
  const bg = routeColor(routeShort);
  const fg = textColorFor(bg);
  const label = String(routeShort || "?").slice(0, 4);
  const hasBearing = typeof bearing === "number" && bearing >= 0;
  const arrow = hasBearing
    ? `<span class="bus-arrow" style="transform:rotate(${bearing}deg)">▲</span>`
    : "";
  return L.divIcon({
    className: "veh-icon",
    html:
      `<span class="bullet bus" style="background:${bg};color:${fg};border-color:${bg}">` +
      `${arrow}<span class="bus-label">${label}</span></span>`,
    iconSize: [34, 20],
    iconAnchor: [17, 10],
  });
}

function alertIcon(type) {
  const cls = (type || "").toUpperCase() === "GAP" ? "gap" : "bunching";
  return L.divIcon({
    className: "alert-marker",
    html: `<span class="alert-tri ${cls}">▲</span>`,
    iconSize: [26, 26],
    iconAnchor: [13, 13],
  });
}

// ---- websocket ----
let reconnectDelay = RECONNECT_BASE_MS;

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => { reconnectDelay = RECONNECT_BASE_MS; setConn("LIVE", "online"); };
  ws.onmessage = e => { try { render(JSON.parse(e.data)); } catch (_) {} };
  ws.onclose = () => {
    setConn("RECONNECTING", "offline");
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  };
  ws.onerror = () => ws.close();
}

function setConn(text, cls) {
  const conn = document.getElementById("conn");
  conn.textContent = "● " + text;
  conn.className = "conn " + cls;
}

// ---- route-line shapes (loaded once) ----
async function loadShapes() {
  let fc;
  try {
    const resp = await fetch("/api/shapes");
    fc = await resp.json();
  } catch (_) { return; }
  for (const f of fc.features || []) {
    const p = f.properties || {};
    const rs = String(p.route_short || p.route_id || "").toUpperCase();
    if (p.color) shapeColors[rs] = p.color;
    const coords = (f.geometry && f.geometry.coordinates) || [];
    const latlngs = coords.map(c => [c[1], c[0]]); // geojson lon,lat -> lat,lon
    if (latlngs.length < 2) continue;
    const line = L.polyline(latlngs, {
      color: p.color || routeColor(rs),
      weight: 3,
      opacity: 0.55,
      className: `route-line mode-${p.mode || "subway"}`,
    });
    line._mode = p.mode || "subway";
    line._route = rs;
    (shapePolylines[rs] = shapePolylines[rs] || []).push(line);
  }
  redrawShapes();
}

function redrawShapes() {
  shapeLayer.clearLayers();
  if (!filters.showShapes) return;
  for (const rs of Object.keys(shapePolylines)) {
    for (const line of shapePolylines[rs]) {
      if (line._mode === "subway" && !filters.subway) continue;
      if (line._mode === "bus" && !filters.bus) continue;
      if (filters.route && rs !== filters.route.toUpperCase()) continue;
      line.addTo(shapeLayer);
    }
  }
}

// ---- render on each snapshot ----
function render(s) {
  setConn(s.connection_error ? s.connection_error : "LIVE", s.connection_error ? "error" : "online");
  renderCounts(s.counts || {});
  renderUpdated(s.updated_ts);
  renderVehicles(s.trains || []);
  renderMapAlerts(s.alerts || [], s.trains || []);
  renderArrivals(s.arrivals || []);
  renderAlerts(s.alerts || []);
  renderRecommendations(s.recommendations || []);
}

function renderCounts(counts) {
  document.getElementById("stat-trains").textContent = counts.trains ?? "--";
  document.getElementById("stat-buses").textContent = counts.buses ?? "--";
  document.getElementById("stat-alerts").textContent = counts.alerts ?? "--";
  document.getElementById("stat-routes").textContent = counts.routes ?? "--";
  document.getElementById("alerts-tag").textContent = (counts.alerts ?? 0) + " active";
}

function renderUpdated(ts) {
  const el = document.getElementById("updated");
  el.textContent = ts ? "updated " + new Date(ts).toLocaleTimeString() : "";
}

function renderVehicles(trains) {
  let routesChanged = false;
  const seen = new Set();

  for (const t of trains) {
    if (!t.lat || !t.lon) continue;
    const rs = String(t.route_short || t.route_id || "");
    const set = t.mode === "bus" ? knownRoutes.bus : knownRoutes.subway;
    if (rs && !set.has(rs)) { set.add(rs); routesChanged = true; }

    if (!trainVisible(t)) continue;

    const key = `${t.mode}:${t.trip_id}`;
    seen.add(key);
    const ll = [t.lat, t.lon];
    let entry = vehicleMarkers.get(key);

    if (!entry) {
      const icon = t.mode === "bus" ? busIcon(rs, t.bearing) : subwayIcon(rs);
      const marker = L.marker(ll, {icon, keyboard: false})
        .bindTooltip(vehicleTooltip(t), {direction: "top", className: "train-tt", offset: [0, -8]});
      marker.addTo(vehicleLayer);
      vehicleMarkers.set(key, {marker, mode: t.mode, route: rs, bearing: t.bearing});
    } else {
      entry.marker.setLatLng(ll);
      if (entry.route !== rs) {
        entry.marker.setIcon(t.mode === "bus" ? busIcon(rs, t.bearing) : subwayIcon(rs));
        entry.route = rs;
      } else if (t.mode === "bus" && t.bearing !== entry.bearing) {
        updateBusBearing(entry.marker, t.bearing);
      }
      entry.bearing = t.bearing;
      entry.marker.setTooltipContent(vehicleTooltip(t));
    }
  }

  for (const [key, entry] of vehicleMarkers) {
    if (!seen.has(key)) { vehicleLayer.removeLayer(entry.marker); vehicleMarkers.delete(key); }
  }

  if (routesChanged) rebuildRouteFilter();
}

function updateBusBearing(marker, bearing) {
  const el = marker.getElement();
  if (!el) return;
  const arrow = el.querySelector(".bus-arrow");
  if (arrow && typeof bearing === "number" && bearing >= 0) {
    arrow.style.transform = `rotate(${bearing}deg)`;
  }
}

function vehicleTooltip(t) {
  const rs = t.route_short || t.route_id || "?";
  const where = t.stop_name || t.stop_id || "";
  const dir = t.mode === "bus" ? "" : " " + directionLabel(t.direction);
  return `<b>${rs}</b>${dir}<br/>${where}<br/><span class="tt-status">${t.status || ""}</span>`;
}

// Place a pulsing warning at the involved train's live position (curr_trip).
function renderMapAlerts(alerts, trains) {
  alertLayer.clearLayers();
  const pos = new Map();
  for (const t of trains) if (t.trip_id) pos.set(t.trip_id, [t.lat, t.lon]);
  for (const a of alerts) {
    const ll = pos.get(a.curr_trip);
    if (!ll || !ll[0]) continue;
    L.marker(ll, {icon: alertIcon(a.alert_type), interactive: true, zIndexOffset: 1000})
      .bindTooltip(
        `<b>${(a.alert_type || "ALERT").toUpperCase()}</b> · ${a.route_id || ""}<br/>` +
        `${a.stop_name || a.stop_id || ""}<br/>headway ${a.headway_seconds ?? "--"}s`,
        {direction: "top", className: "train-tt", offset: [0, -10]}
      )
      .addTo(alertLayer);
  }
}

function renderArrivals(arrivals) {
  const body = document.getElementById("board-body");
  const rows = arrivals
    .filter(a => typeof a.eta_seconds === "number" && a.eta_seconds >= 0)
    .sort((a, b) => a.eta_seconds - b.eta_seconds)
    .slice(0, MAX_ARRIVALS);
  if (!rows.length) {
    body.innerHTML = `<tr class="empty-row"><td colspan="4">Waiting for data...</td></tr>`;
    return;
  }
  body.innerHTML = rows.map(a =>
    `<tr><td>${routeChip(a.route_id)}</td>` +
    `<td>${directionLabel(a.direction)}</td>` +
    `<td class="station">${a.stop_name || a.stop_id || ""}</td>` +
    `<td class="eta-col">${formatEta(a.eta_seconds)}</td></tr>`
  ).join("");
}

function renderAlerts(alerts) {
  const feed = document.getElementById("alerts-feed");
  if (!alerts.length) { feed.innerHTML = emptyItem("No active alerts"); return; }
  const sorted = [...alerts].sort((a, b) => (b.ts || 0) - (a.ts || 0));
  feed.innerHTML = sorted.map(a => {
    const type = (a.alert_type || "").toUpperCase();
    const typeClass = type === "GAP" ? "gap" : "bunching";
    return `<li class="alert-item ${typeClass}"><div class="alert-head">` +
      `${routeChip(a.route_id)}<span class="alert-type ${typeClass}">${type || "ALERT"}</span>` +
      `<span class="alert-time">${formatClock(a.ts)}</span></div>` +
      `<div class="alert-body"><span class="station">${a.stop_name || a.stop_id || ""}</span>` +
      `<span class="headway">headway ${a.headway_seconds ?? "--"}s</span></div></li>`;
  }).join("");
}

function renderRecommendations(recs) {
  const feed = document.getElementById("recs-feed");
  if (!recs.length) { feed.innerHTML = emptyItem("No recommendations yet"); return; }
  const sorted = [...recs].sort((a, b) => (b.ts || 0) - (a.ts || 0));
  feed.innerHTML = sorted.map(r =>
    `<li class="rec-item"><div class="rec-head">` +
    `<span class="action-badge ${actionClass(r.action)}">${r.action || "MONITOR"}</span>` +
    `${routeChip(r.route_id)}<span class="station">${r.stop_name || r.stop_id || ""}</span>` +
    `<span class="alert-time">${formatClock(r.ts)}</span></div>` +
    `<p class="rec-note">${r.dispatcher_note || ""}</p>` +
    (r.rider_message ? `<p class="rider-msg">&ldquo;${r.rider_message}&rdquo;</p>` : "") +
    `</li>`
  ).join("");
}

// ---- route filter dropdown ----
function rebuildRouteFilter() {
  const sel = document.getElementById("route-filter");
  const subway = [...knownRoutes.subway];
  const bus = [...knownRoutes.bus];
  const cmp = (a, b) => a.localeCompare(b, undefined, {numeric: true});
  subway.sort(cmp); bus.sort(cmp);

  const current = sel.value;
  let html = `<option value="">All routes</option>`;
  if (subway.length) html += `<optgroup label="Subway">` +
    subway.map(r => `<option value="${r}">${r}</option>`).join("") + `</optgroup>`;
  if (bus.length) html += `<optgroup label="Bus">` +
    bus.map(r => `<option value="${r}">${r}</option>`).join("") + `</optgroup>`;
  sel.innerHTML = html;
  sel.value = current; // keep selection if still valid
}

// ---- helpers ----
function routeChip(routeId) {
  const id = routeId != null ? String(routeId) : "?";
  const bg = routeColor(id);
  const fg = textColorFor(bg);
  return `<span class="route-chip" style="background:${bg};color:${fg}">${id}</span>`;
}
function directionLabel(dir) {
  if (dir === "N") return "Uptown";
  if (dir === "S") return "Downtown";
  if (dir === "0") return "→";
  if (dir === "1") return "←";
  return dir || "--";
}
function formatEta(eta) {
  if (eta < ARRIVING_THRESHOLD_S) return `<span class="arriving">arriving</span>`;
  const m = Math.floor(eta / SEC_PER_MIN);
  const sec = Math.round(eta % SEC_PER_MIN);
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
}
function formatClock(ts) {
  if (!ts) return "";
  return new Date(ts).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit"});
}
function actionClass(action) { return (action || "MONITOR").toUpperCase().replace(/[^A-Z]+/g, "-"); }
function emptyItem(text) { return `<li class="empty-item">${text}</li>`; }

// ---- wire up controls ----
function wireControls() {
  const onModeChange = () => {
    filters.subway = document.getElementById("mode-subway").checked;
    filters.bus = document.getElementById("mode-bus").checked;
    // Drop now-hidden markers immediately (next snapshot re-adds visible ones).
    for (const [key, entry] of vehicleMarkers) {
      const hidden = (entry.mode === "subway" && !filters.subway) ||
                     (entry.mode === "bus" && !filters.bus);
      if (hidden) { vehicleLayer.removeLayer(entry.marker); vehicleMarkers.delete(key); }
    }
    redrawShapes();
  };
  document.getElementById("mode-subway").addEventListener("change", onModeChange);
  document.getElementById("mode-bus").addEventListener("change", onModeChange);

  document.getElementById("route-filter").addEventListener("change", e => {
    filters.route = e.target.value;
    for (const [key, entry] of vehicleMarkers) {
      if (filters.route && entry.route.toUpperCase() !== filters.route.toUpperCase()) {
        vehicleLayer.removeLayer(entry.marker); vehicleMarkers.delete(key);
      }
    }
    redrawShapes();
  });

  document.getElementById("show-shapes").addEventListener("change", e => {
    filters.showShapes = e.target.checked;
    redrawShapes();
  });
}

// ---- AI agents console ----
function wireAgents() {
  // tab switching
  document.querySelectorAll(".agent-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      const name = tab.dataset.agent;
      document.querySelectorAll(".agent-tab").forEach(t => t.classList.toggle("active", t === tab));
      document.querySelectorAll(".agent-pane").forEach(p =>
        p.classList.toggle("active", p.id === `pane-${name}`));
    });
  });

  document.querySelectorAll(".agent-run").forEach(btn => {
    btn.addEventListener("click", () => runAgent(btn.dataset.agent, btn));
  });
}

async function runAgent(name, btn) {
  const out = document.getElementById(`out-${name}`);
  const val = id => (document.getElementById(id)?.value || "").trim();

  let url, body;
  if (name === "advisor") {
    url = "/api/advisor";
    body = {origin: val("adv-origin"), destination: val("adv-dest"), question: val("adv-q")};
  } else if (name === "operator") {
    url = "/api/operator";
    body = {question: val("op-q")};
  } else {
    url = "/api/route-designer";
    body = {origin: val("rt-origin"), destination: val("rt-dest"), constraints: val("rt-constraints")};
  }

  btn.disabled = true;
  out.className = "agent-out loading";
  out.textContent = "Thinking over the live feed...";
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!data.ok) {
      out.className = "agent-out error";
      out.textContent = data.error || "Agent request failed.";
    } else if (name === "route") {
      renderProposal(out, data.proposal);
    } else {
      out.className = "agent-out";
      out.textContent = data.answer || "(no answer)";
    }
  } catch (err) {
    out.className = "agent-out error";
    out.textContent = "Request failed: " + err;
  } finally {
    btn.disabled = false;
  }
}

function renderProposal(out, p) {
  out.className = "agent-out";
  const stops = (p.stops || []).join(" → ");
  const conns = (p.connections || []).join(", ");
  out.innerHTML =
    `<div class="route-title">${escapeHtml(p.name || "Proposed route")}</div>` +
    `<div>${escapeHtml(p.rationale || "")}</div>` +
    (stops ? `<div class="route-meta">Stops: ${escapeHtml(stops)}</div>` : "") +
    (conns ? `<div class="route-meta">Connects: ${escapeHtml(conns)}</div>` : "") +
    `<div class="route-meta">Drawn on the map in cyan (dashed).</div>`;
  drawProposedRoute(p);
}

function drawProposedRoute(p) {
  proposedLayer.clearLayers();
  const wp = p.waypoints || [];
  if (wp.length < 2) return;
  const line = L.polyline(wp, {
    color: "#4cc9f0", weight: 5, opacity: 0.9, dashArray: "10 8",
  }).addTo(proposedLayer);
  wp.forEach((ll, i) => {
    const first = i === 0, last = i === wp.length - 1;
    L.circleMarker(ll, {
      radius: first || last ? 7 : 4,
      color: "#4cc9f0", fillColor: first ? "#1fc77b" : last ? "#ff4d4d" : "#4cc9f0",
      fillOpacity: 1, weight: 2,
    }).addTo(proposedLayer);
  });
  map.fitBounds(line.getBounds(), {padding: [40, 40], maxZoom: 14});
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c =>
    ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}

wireControls();
wireAgents();
loadShapes();
connect();
