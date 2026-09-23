"use strict";

const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const APP_JS = path.join(__dirname, "..", "dashboard", "static", "app.js");

function makeChain() {
  const target = function () {};
  const chain = new Proxy(target, {
    get: () => chain,
    apply: () => chain,
    construct: () => chain,
  });
  return chain;
}

const sandbox = {
  console,
  setTimeout: () => {},
  clearTimeout: () => {},
  location: { protocol: "http:", host: "localhost:8000" },
  WebSocket: class { constructor() {} },
  fetch: () => Promise.resolve({ json: () => Promise.resolve({ features: [] }) }),
  L: makeChain(),
  document: makeChain(),
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;

const source = fs.readFileSync(APP_JS, "utf-8") +
  "\n;globalThis.__t = { formatEta, selectArrivalsForBoard, filters, MAX_ARRIVALS };";
vm.createContext(sandbox);
vm.runInContext(source, sandbox, { filename: "app.js" });

const { formatEta, selectArrivalsForBoard, filters, MAX_ARRIVALS } = sandbox.__t;

function run(name, fn) {
  try {
    fn();
    console.log(`  ok - ${name}`);
  } catch (err) {
    console.error(`  FAIL - ${name}`);
    throw err;
  }
}

console.log("arrivals board frontend tests:");

run("MAX_ARRIVALS is 8 for compact board", () => {
  assert.strictEqual(MAX_ARRIVALS, 8);
});

run("formatEta shows arriving for < 30s", () => {
  assert.strictEqual(formatEta(0), `<span class="arriving">arriving</span>`);
  assert.strictEqual(formatEta(15), `<span class="arriving">arriving</span>`);
  assert.strictEqual(formatEta(29), `<span class="arriving">arriving</span>`);
});

run("formatEta shows seconds for < 1m", () => {
  assert.strictEqual(formatEta(30), `30s`);
  assert.strictEqual(formatEta(45), `45s`);
  assert.strictEqual(formatEta(59), `59s`);
});

run("formatEta shows zero-padded minutes and seconds", () => {
  assert.strictEqual(formatEta(65), `1m 05s`);
  assert.strictEqual(formatEta(120), `2m 00s`);
  assert.strictEqual(formatEta(255), `4m 15s`);
});

run("selectArrivalsForBoard spreads across time brackets and routes", () => {
  filters.route = "";
  // Ingest arrivals across routes
  const arrivals = [
    { route_id: "1", direction: "N", stop_name: "Times Sq", eta_seconds: 5 },
    { route_id: "2", direction: "N", stop_name: "125 St", eta_seconds: 10 },
    { route_id: "3", direction: "N", stop_name: "Penn Sta", eta_seconds: 15 },
    { route_id: "4", direction: "S", stop_name: "Wall St", eta_seconds: 45 },
    { route_id: "5", direction: "N", stop_name: "Grand Central", eta_seconds: 140 },
    { route_id: "6", direction: "S", stop_name: "Canal St", eta_seconds: 310 },
    { route_id: "7", direction: "N", stop_name: "Hudson Yards", eta_seconds: 500 },
    { route_id: "A", direction: "S", stop_name: "Fulton St", eta_seconds: 700 },
    { route_id: "C", direction: "N", stop_name: "59 St", eta_seconds: 1100 },
  ];

  const selected = selectArrivalsForBoard(arrivals, 8);
  assert.strictEqual(selected.length, 8);

  // Does NOT fill all 8 with 'arriving' (<30s), caps at 2 arriving
  const arrivingCount = selected.filter(a => a.eta_seconds < 30).length;
  assert(arrivingCount <= 2, `Expected at most 2 arriving, got ${arrivingCount}`);

  // The rest have countdowns >= 30s
  const upcomingCount = selected.filter(a => a.eta_seconds >= 30).length;
  assert(upcomingCount >= 6, `Expected at least 6 upcoming, got ${upcomingCount}`);

  // Route diversity: all distinct routes
  const routes = selected.map(a => a.route_id);
  assert.strictEqual(new Set(routes).size, 8);
});

run("selectArrivalsForBoard respects route filter", () => {
  filters.route = "A";
  const arrivals = [
    { route_id: "1", direction: "N", stop_name: "Times Sq", eta_seconds: 5 },
    { route_id: "A", direction: "N", stop_name: "59 St", eta_seconds: 15 },
    { route_id: "A", direction: "N", stop_name: "125 St", eta_seconds: 150 },
    { route_id: "A", direction: "N", stop_name: "168 St", eta_seconds: 400 },
    { route_id: "7", direction: "N", stop_name: "Flushing", eta_seconds: 50 },
  ];

  const selected = selectArrivalsForBoard(arrivals, 8);
  assert.strictEqual(selected.length, 3);
  assert(selected.every(a => a.route_id === "A"));
  filters.route = "";
});

console.log("\nAll arrival frontend tests passed.");
