// static/accounts/js/login.js
document.addEventListener('DOMContentLoaded', function(){
  const form = document.getElementById('loginForm');
  const pw = document.getElementById('password');
  const toggle = document.getElementById('togglePw');

  toggle.addEventListener('click', function(){
    if (pw.type === 'password') {
      pw.type = 'text';
      toggle.textContent = 'Hide';
    } else {
      pw.type = 'password';
      toggle.textContent = 'Show';
    }
  });

  form.addEventListener('submit', function(e){
    // basic UX validation
    const u = document.getElementById('username').value.trim();
    const p = pw.value;
    if (!u || !p) {
      e.preventDefault();
      alert('Please enter username and password.');
      return false;
    }
    // let the request go through
  });
});
