(function () {
  var path = window.location.pathname;

  function redirectTo(targetPath) {
    if (!targetPath || targetPath === path) {
      return;
    }
    window.location.replace(targetPath + window.location.search + window.location.hash);
  }

  if (path.endsWith(".md")) {
    var normalized = path.replace(/\/index\.md$/, "/").replace(/\.md$/, "/");
    redirectTo(normalized);
    return;
  }

  if (path.endsWith("/index")) {
    redirectTo(path + "/");
  }
})();
