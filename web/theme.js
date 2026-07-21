// Manual light/dark override for the theme toggle. The default is the
// system preference (no `data-theme` attribute); the first click pins an
// explicit choice and persists it. A tiny inline script in each page's
// <head> applies the stored choice before first paint to avoid a flash.
(function () {
  var root = document.documentElement;
  var btn = document.getElementById("theme-toggle");
  if (!btn) return;

  function effective() {
    var attr = root.getAttribute("data-theme");
    if (attr === "light" || attr === "dark") return attr;
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  btn.addEventListener("click", function () {
    var next = effective() === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try {
      localStorage.setItem("corium-theme", next);
    } catch (e) {
      /* storage disabled — the choice still applies for this page load */
    }
  });
})();
