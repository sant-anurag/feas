// static/projects/js/projects-actions.js
// Modal wiring + LDAP autocomplete + robust (debounced & guarded) POST handlers
// Prevents duplicate submits by disabling buttons and using a guard.

document.addEventListener("DOMContentLoaded", function() {

  // ---------- modal open/close ----------
  document.querySelectorAll("[data-open]").forEach(btn => {
    btn.addEventListener("click", function() {
      const id = btn.getAttribute("data-open");
      const m = document.getElementById(id);
      if (m) openModal(m);
    });
  });
  document.querySelectorAll(".close-modal, [data-close]").forEach(el => {
    el.addEventListener("click", function(e) {
      const id = el.getAttribute("data-close") || el.closest(".modal")?.id;
      if (id) {
        const m = document.getElementById(id);
        if (m) closeModal(m);
      }
    });
  });

  function openModal(m) {
    m.setAttribute("aria-hidden", "false");
    m.style.display = "flex";
    document.body.style.overflow = "hidden";
  }
  function closeModal(m) {
    m.setAttribute("aria-hidden", "true");
    m.style.display = "none";
    document.body.style.overflow = "";
  }

  document.addEventListener("keyup", function(e) {
    if (e.key === "Escape") {
      document.querySelectorAll(".modal[aria-hidden='false']").forEach(closeModal);
    }
  });

  // ---------- LDAP autocomplete (unchanged) ----------
  function wireAutocomplete(inputId, hiddenId, suggestionBoxId) {
    const input = document.getElementById(inputId);
    const hidden = document.getElementById(hiddenId);
    const box = document.getElementById(suggestionBoxId);
    let timer = null;
    let activeIndex = -1;
    let items = [];

    if (!input || !hidden || !box) return;

    input.addEventListener("input", function() {
      const v = input.value.trim();
      hidden.value = "";
      if (v.length < 3) {
        box.innerHTML = "";
        box.style.display = "none";
        return;
      }
      clearTimeout(timer);
      timer = setTimeout(() => {
        fetch(`/projects/ldap-search/?q=${encodeURIComponent(v)}`)
          .then(r => r.json())
          .then(data => {
            items = data.results || [];
            box.innerHTML = "";
            if (items.length === 0) { box.style.display = "none"; return; }
            items.forEach((it, idx) => {
              const el = document.createElement("div");
              el.className = "autocomplete-item";
              el.setAttribute("role","option");
              el.dataset.idx = idx;
              el.innerHTML = `<div class="a-title">${escapeHtml(it.cn || it.sAMAccountName)}</div>
                              <div class="a-sub">${escapeHtml(it.mail || '')} <span class="a-muted">${escapeHtml(it.title||'')}</span></div>`;
              el.addEventListener("click", function(){ select(idx); });
              box.appendChild(el);
            });
            box.style.display = "block";
            activeIndex = -1;
          }).catch(err=>{
            console.error("LDAP search error", err);
            box.style.display = "none";
          });
      }, 240);
    });

    input.addEventListener("keydown", function(e){
      const itemsEls = box.querySelectorAll(".autocomplete-item");
      if (e.key === "ArrowDown") { e.preventDefault(); activeIndex = Math.min(activeIndex + 1, itemsEls.length - 1); highlight(itemsEls); }
      else if (e.key === "ArrowUp") { e.preventDefault(); activeIndex = Math.max(activeIndex - 1, 0); highlight(itemsEls); }
      else if (e.key === "Enter") { e.preventDefault(); if (activeIndex >= 0 && itemsEls[activeIndex]) select(itemsEls[activeIndex].dataset.idx); }
      else if (e.key === "Escape") { box.style.display = "none"; }
    });

    document.addEventListener("click", function(ev){
      if (!box.contains(ev.target) && ev.target !== input) { box.style.display = "none"; }
    });

    function highlight(itemsEls){
      itemsEls.forEach(el => el.classList.remove("active"));
      if (activeIndex >= 0 && itemsEls[activeIndex]) {
        itemsEls[activeIndex].classList.add("active");
        itemsEls[activeIndex].scrollIntoView({block:"nearest"});
      }
    }
    function select(idx){
      const it = items[idx];
      if (!it) return;

      // Show CN + mail in visible text field
      input.value = `${it.cn || it.displayName || it.sAMAccountName || ''} (${it.mail || it.userPrincipalName || ''})`;

      // Save only canonical login in hidden field
      hidden.value = it.userPrincipalName || it.mail || '';

      box.style.display = "none";
    }


  wireAutocomplete("pdl_picker","pdl_username","pdl_suggestions");
  wireAutocomplete("coe_leader_picker","coe_leader_username","coe_leader_suggestions");
  wireAutocomplete("domain_lead_picker","domain_lead_username","domain_lead_suggestions");
  wireAutocomplete("domain_edit_lead_picker","domain_edit_lead_username","domain_edit_lead_suggestions");

  // ---------- CSRF helper ----------
  function getCookie(name) {
    const v = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
    return v ? v.pop() : '';
  }
  const csrftoken = getCookie('csrftoken');

  // ---------- robust POST helper ----------
  function postFormRobust(url, formData, onSuccess, onError) {
    return fetch(url, {
      method: 'POST',
      headers: {
        'X-CSRFToken': csrftoken,
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: formData,
      credentials: 'same-origin'
    }).then(async resp => {
      const contentType = resp.headers.get('content-type') || '';
      const status = resp.status;
      let bodyText = null;
      try {
        if (contentType.includes('application/json')) {
          const json = await resp.json();
          bodyText = JSON.stringify(json);
          if (!resp.ok) throw {status, body: json};
          return {ok: true, data: json};
        } else {
          bodyText = await resp.text();
          if (!resp.ok) throw {status, body: bodyText};
          return {ok: true, data: bodyText};
        }
      } catch (err) {
        throw err;
      }
    }).then(result => {
      if (onSuccess) onSuccess(result.data);
      return result.data;
    }).catch(err => {
      console.error('[projects-actions] POST error', err);
      if (onError) onError(err);
      else {
        let msg = 'Request failed';
        if (err && err.status) msg += ` (status ${err.status})`;
        if (err && err.body) msg += ': ' + (typeof err.body === 'string' ? err.body : JSON.stringify(err.body));
        alert(msg);
      }
      throw err;
    });
  }

  // ---------- prevent double submissions helper ----------
  function withSingleSubmission(button, handler) {
    // Use a closure flag so multiple listeners / double clicks cannot submit twice
    let inProgress = false;
    if (!button) return function(){};
    const wrapped = function(e) {
      e && e.preventDefault && e.preventDefault();
      if (inProgress) {
        console.warn('Submission prevented: already in progress');
        return;
      }
      inProgress = true;
      button.setAttribute('aria-disabled', 'true');
      button.disabled = true;
      // call handler; ensure we unset inProgress on both success/error
      const done = function() {
        inProgress = false;
        button.disabled = false;
        button.removeAttribute('aria-disabled');
      };
      // handler must return a Promise (or we wrap result)
      try {
        const res = handler();
        if (res && typeof res.then === 'function') {
          res.then(done).catch(err => { done(); throw err; });
        } else {
          done();
        }
      } catch (err) {
        done();
        throw err;
      }
    };
    return wrapped;
  }

  // ---------- Modal action handlers (create/assign) ----------
  const coeSaveBtn = document.getElementById('coeCreateSave');
  if (coeSaveBtn) {
    const handler = function() {
      const name = (document.getElementById('coe_name_input') || {}).value || '';
      const desc = (document.getElementById('coe_desc_input') || {}).value || '';
      const leader = (document.getElementById('coe_leader_username') || {}).value || '';
      if (!name.trim()) { alert('COE name is required'); return Promise.resolve(); }

      const fd = new FormData();
      fd.append('name', name.trim());
      fd.append('description', desc.trim());
      if (leader) fd.append('leader_username', leader);

      return postFormRobust('/projects/coes/create/', fd, function(data){
        const m = document.getElementById('coeCreateModal');
        if (m) closeModal(m);
        window.location.reload();
      }, function(err) {
        // if error returned as JSON with 'error', present it
        if (err && err.body) {
          const b = typeof err.body === 'string' ? err.body : JSON.stringify(err.body);
          try {
            const parsed = (typeof err.body === 'string') ? JSON.parse(err.body) : err.body;
            if (parsed && parsed.error) alert(parsed.error);
          } catch(e) {
            // ignore parse
          }
        }
      });
    };
    coeSaveBtn.addEventListener('click', withSingleSubmission(coeSaveBtn, handler));
  }

  const domainSaveBtn = document.getElementById('domainCreateSave');
  if (domainSaveBtn) {
    const handler = function() {
      const name = (document.getElementById('domain_name_input') || {}).value || '';
      const coe = (document.getElementById('domain_coe_select') || {}).value || '';
      const lead = (document.getElementById('domain_lead_username') || {}).value || '';
      if (!name.trim()) { alert('Domain name required'); return Promise.resolve(); }
      const fd = new FormData();
      fd.append('name', name.trim());
      if (coe) fd.append('coe_id', coe);
      if (lead) fd.append('lead_username', lead);
      return postFormRobust('/projects/domains/create/', fd, function(data){
        const m = document.getElementById('domainCreateModal');
        if (m) closeModal(m);
        window.location.reload();
      }, function(err) {
        if (err && err.body) {
          try {
            const parsed = (typeof err.body === 'string') ? JSON.parse(err.body) : err.body;
            if (parsed && parsed.error) alert(parsed.error);
          } catch(e){}
        }
      });
    };
    domainSaveBtn.addEventListener('click', withSingleSubmission(domainSaveBtn, handler));
  }

  // COE edit/assign/ domain edit handlers remain similar but also use single submission guard
  const coeUpdateBtn = document.getElementById('coeUpdateBtn');
  if (coeUpdateBtn) {
    const handler = function() {
      const sel = document.getElementById('coe_select');
      if (!sel || !sel.value) { alert('Select COE to update'); return Promise.resolve(); }
      const coeId = sel.value;
      const desc = (document.getElementById('coe_edit_desc') || {}).value || '';
      const fd = new FormData();
      fd.append('name', sel.options[sel.selectedIndex].text || '');
      fd.append('description', desc);
      const leader = (document.getElementById('coe_leader_username') || {}).value || '';
      if (leader) fd.append('leader_username', leader);
      return postFormRobust(`/projects/coes/edit/${encodeURIComponent(coeId)}/`, fd, function(){ window.location.reload(); });
    };
    coeUpdateBtn.addEventListener('click', withSingleSubmission(coeUpdateBtn, handler));
  }

  const assignBtn = document.getElementById('assignCoeLeaderBtn');
  if (assignBtn) {
    const handler = function() {
      const coeSel = document.getElementById('coe_for_leader');
      const leaderUsername = (document.getElementById('coe_leader_username') || {}).value || '';
      if (!coeSel || !coeSel.value) { alert('Select COE'); return Promise.resolve(); }
      if (!leaderUsername) { alert('Search and select a user first'); return Promise.resolve(); }
      const fd = new FormData();
      fd.append('leader_username', leaderUsername);
      return postFormRobust(`/projects/coes/edit/${encodeURIComponent(coeSel.value)}/`, fd, function(){ window.location.reload(); });
    };
    assignBtn.addEventListener('click', withSingleSubmission(assignBtn, handler));
  }

  const domainUpdateBtn = document.getElementById('domainUpdateBtn');
  if (domainUpdateBtn) {
    const handler = function() {
      const sel = document.getElementById('domain_select');
      if (!sel || !sel.value) { alert('Select domain to update'); return Promise.resolve(); }
      const domainId = sel.value;
      const lead = (document.getElementById('domain_edit_lead_username') || {}).value || '';
      const name = sel.options[sel.selectedIndex].text || '';
      const fd = new FormData();
      fd.append('name', name);
      if (lead) fd.append('lead_username', lead);
      return postFormRobust(`/projects/domains/edit/${encodeURIComponent(domainId)}/`, fd, function(){ window.location.reload(); });
    };
    domainUpdateBtn.addEventListener('click', withSingleSubmission(domainUpdateBtn, handler));
  }

  // Project delete left as-is (no destructive auto-delete via modal)
  const projectDeleteConfirm = document.getElementById('projectDeleteConfirm');
  if (projectDeleteConfirm) {
    projectDeleteConfirm.addEventListener('click', function(e){
      e.preventDefault();
      alert('Use Project list page for deletion (modal delete not configured).');
      const m = document.getElementById('projectDeleteModal'); if (m) closeModal(m);
    });
  }

});
