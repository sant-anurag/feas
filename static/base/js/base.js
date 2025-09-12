// static/base/js/base.js
document.addEventListener("DOMContentLoaded", function() {
  // submenu toggles
  document.querySelectorAll(".side-link").forEach(function(link) {
    link.addEventListener("click", function(e) {
      // If link has data-toggle-target attribute, toggle submenu
      const target = link.getAttribute("data-toggle-target");
      if (target) {
        e.preventDefault();
        const submenu = document.querySelector(target);
        if (!submenu) return;
        submenu.classList.toggle("open");
      }
    });
  });

  // active state highlighting (basic)
  const current = window.location.pathname + window.location.hash;
  document.querySelectorAll(".side-link").forEach(function(a) {
    // exact match or contains
    if (a.getAttribute("href") && current.indexOf(a.getAttribute("href")) !== -1 && a.getAttribute("href") !== "#") {
      a.classList.add("active");
    }
  });

  // sidebar collapse toggle (desktop)
  const collapseBtn = document.querySelector(".sidebar-toggle-btn");
  if (collapseBtn) {
    collapseBtn.addEventListener("click", function() {
      document.querySelector(".sidebar").classList.toggle("narrow");
      document.querySelector(".content").classList.toggle("compact");
    });
  }

  // mobile open/close
  const mobileToggle = document.querySelector(".mobile-toggle-btn");
  if (mobileToggle) {
    mobileToggle.addEventListener("click", function() {
      document.querySelector(".sidebar").classList.toggle("open");
    });
  }
});
