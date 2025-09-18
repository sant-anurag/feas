document.addEventListener('DOMContentLoaded', function () {
  const sidebar = document.getElementById('feasSidebar');
  const sidebarToggle = document.querySelector('.sidebar-toggle-btn');
  const mobileToggle = document.querySelector('.mobile-toggle-btn');
  const overlay = document.getElementById('sidebarOverlay');
  const mainMenu = document.getElementById('mainMenu');
  const userToggle = document.getElementById('userToggle');
  const userMenu = document.getElementById('userMenu');

  // collapse
  if (sidebarToggle) {
    sidebarToggle.addEventListener('click', function () {
      sidebar.classList.toggle('collapsed');
    });
  }

  // mobile toggle
  if (mobileToggle) {
    mobileToggle.addEventListener('click', function () {
      sidebar.classList.toggle('open');
      overlay.classList.toggle('active');
    });
  }
  if (overlay) {
    overlay.addEventListener('click', function () {
      sidebar.classList.remove('open');
      overlay.classList.remove('active');
    });
  }

  // submenu toggle (only one open at a time)
  if (mainMenu) {
    mainMenu.querySelectorAll('.has-sub-toggle').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        const submenu = link.nextElementSibling;
        const isOpen = submenu.classList.contains('open');
        // close all
        mainMenu.querySelectorAll('.submenu').forEach(s => { s.classList.remove('open'); });
        mainMenu.querySelectorAll('.has-sub-toggle').forEach(l => { l.classList.remove('active'); l.setAttribute('aria-expanded','false'); });
        // open this
        if (!isOpen) {
          submenu.classList.add('open');
          link.classList.add('active');
          link.setAttribute('aria-expanded','true');
        }
      });
    });
  }

  // user dropdown
  if (userToggle && userMenu) {
    userToggle.addEventListener('click', function (e) {
      const show = userMenu.classList.toggle('show');
      userToggle.setAttribute('aria-expanded', show ? 'true' : 'false');
    });
    document.addEventListener('click', function (e) {
      if (!userToggle.contains(e.target) && !userMenu.contains(e.target)) {
        userMenu.classList.remove('show');
        userToggle.setAttribute('aria-expanded','false');
      }
    });
  }
});
