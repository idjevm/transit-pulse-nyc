"use strict";
// Node test for the dashboard's client-side markdown renderer (PR: dashboard-
// copilot-ux). Agent answers are written with innerHTML, so the renderer must
// escape untrusted model output before applying formatting. There is no browser
// here, so we load app.js in a vm sandbox with the handful of globals it touches
// at load time, then exercise the pure renderer functions.
//
// Run: node tests/frontend_markdown.test.js   (exits non-zero on failure)

const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const APP_JS = path.join(__dirname, "..", "dashboard", "static", "app.js");

// A proxy that swallows any property access / call / construction and returns
// itself, so Leaflet (L) and DOM chains in app.js execute without a browser.
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

const source = fs.readFileSync(APP_JS, "utf8") +
  "\n;globalThis.__t = { renderMarkdown, escapeHtml, inlineMd };";

vm.createContext(sandbox);
vm.runInContext(source, sandbox, { filename: "app.js" });

const { renderMarkdown, escapeHtml } = sandbox.__t;

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log("  ok -", name);
}

check("escapeHtml escapes all five sensitive chars", () => {
  assert.strictEqual(escapeHtml("&<>\"'"), "&amp;&lt;&gt;&quot;&#39;");
});

check("renderMarkdown escapes raw HTML (no live tag survives)", () => {
  const html = renderMarkdown("<script>alert(1)</script>");
  assert.ok(!html.includes("<script>"), "raw <script> must not appear");
  assert.ok(html.includes("&lt;script&gt;"), "script tag must be escaped");
});

check("bold becomes <strong>", () => {
  assert.ok(renderMarkdown("**hold train**").includes("<strong>hold train</strong>"));
});

check("italic becomes <em>", () => {
  assert.ok(renderMarkdown("this is *urgent* now").includes("<em>urgent</em>"));
});

check("inline code becomes <code>", () => {
  assert.ok(renderMarkdown("run `mta_producer`").includes("<code>mta_producer</code>"));
});

check("bullet list becomes <ul><li>", () => {
  const html = renderMarkdown("- alpha\n- beta");
  assert.ok(html.includes("<ul>") && html.includes("<li>alpha</li>") && html.includes("<li>beta</li>"));
});

check("numbered list becomes <ol><li>", () => {
  const html = renderMarkdown("1. first\n2. second");
  assert.ok(html.includes("<ol>") && html.includes("<li>first</li>"));
});

check("heading becomes <h4>", () => {
  assert.ok(renderMarkdown("## Risk summary").includes('<h4 class="md-h">Risk summary</h4>'));
});

check("plain text becomes a paragraph", () => {
  assert.ok(renderMarkdown("Trains are running normally.").includes("<p>Trains are running normally.</p>"));
});

check("injection inside bold is still escaped", () => {
  const html = renderMarkdown("**<img src=x onerror=alert(1)>**");
  assert.ok(!html.includes("<img"), "no live <img> tag");
  assert.ok(html.includes("&lt;img"), "img must be escaped");
});

console.log(`\n${passed} passed`);
