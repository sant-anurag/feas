document.addEventListener('DOMContentLoaded', function(){
  const form = document.getElementById('loginForm');
  const pw = document.getElementById('password');
  const toggle = document.getElementById('togglePw');
  const pwEye = document.getElementById('pwEye');

  toggle.addEventListener('click', function(){
    if (pw.type === 'password') {
      pw.type = 'text';
      pwEye.textContent = '🙈';
    } else {
      pw.type = 'password';
      pwEye.textContent = '👁️';
    }
  });

  form.addEventListener('submit', function(e){
    const u = document.getElementById('username').value.trim();
    const p = pw.value;
    if (!u || !p) {
      e.preventDefault();
      alert('Please enter username and password.');
      return false;
    }
  });
});