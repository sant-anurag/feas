// static/base/js/base.js
// Behavior: inline submenu accordion & profile dropdown accessibility (preserves original IDs/functions)

document.addEventListener("DOMContentLoaded", function() {

  // active-state detection (non-invasive) -- add .active class to matching side-link + parent
  const current = window.location.pathname + window.location.hash;
  document.querySelectorAll(".side-link").forEach(function(a) {
    try {
      const href = a.getAttribute("href");
      if (href && href !== "#" && current.indexOf(href) !== -1) {
        a.classList.add("active");
        const li = a.closest('.side-item'); if (li) li.classList.add('active');
      }
    } catch (e) { /* ignore */ }
  });

  // Collapse (pin) button keeps same layout logic
  const collapseBtn = document.querySelector(".sidebar-pin");
  if (collapseBtn) {
    collapseBtn.addEventListener("click", function(e) {
      e.preventDefault();
      const sb = document.getElementById('feasSidebar');
      const content = document.querySelector('.content');
      sb.classList.toggle('collapsed');
      if (sb.classList.contains('collapsed')) content.style.marginLeft = '72px';
      else content.style.marginLeft = '260px';
    });
  }

  // Mobile toggle (keeps same IDs & behavior)
  const mobileToggle = document.getElementById('mobileMenuBtn');
  if (mobileToggle) {
    mobileToggle.addEventListener("click", function() {
      const sb = document.getElementById('feasSidebar');
      const overlay = document.getElementById('sidebarOverlay');
      sb.classList.toggle('open');
      overlay.classList.toggle('active');
      overlay.setAttribute('aria-hidden', sb.classList.contains('open') ? 'false' : 'true');
    });
  }

  // Submenu expand/collapse via click (accordion). We delegating to toggleSubmenu inline so keep minimal here.
  // But provide keyboard support: pressing Enter/Space on .side-link toggles submenu if present
  document.querySelectorAll('.side-link').forEach(function(link){
    const parent = link.closest('.side-item');
    if (parent && parent.getAttribute('data-has-sub') === '1') {
      link.addEventListener('keydown', function(e){
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          const submenu = link.parentElement.querySelector('.submenu');
          if (submenu) {
            // call existing global function to keep semantics
            toggleSubmenu(e, submenu.id);
          }
        }
      });
    }
  });

  // Accessibility / closing submenus with ESC: close any open submenus on Escape
  document.addEventListener('keyup', function(e) {
    if (e.key === 'Escape') {
      document.querySelectorAll('.submenu.open').forEach(s => {
        s.classList.remove('open');
        s.setAttribute('aria-hidden','true');
        const link = s.previousElementSibling;
        if (link) link.setAttribute('aria-expanded','false');
      });
      // also close sidebar overlay if mobile open
      const sb = document.getElementById('feasSidebar');
      const overlay = document.getElementById('sidebarOverlay');
      if (sb && overlay && sb.classList.contains('open')) {
        sb.classList.remove('open');
        overlay.classList.remove('active');
        overlay.setAttribute('aria-hidden','true');
      }
    }
  });

  // Profile dropdown is handled inline in base.html, but keep a small protection here in case it's toggled programmatically:
  document.addEventListener('click', function(e){
    // if click occurs on an element with .profile-dropdown, do nothing
    // otherwise closing is handled inline in base.html handlers
  });

});
