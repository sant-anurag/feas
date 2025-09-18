// static/projects/js/projects-actions.js

document.addEventListener("DOMContentLoaded", function() {

  // simple modal open/close
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

  // close modal on ESC
  document.addEventListener("keyup", function(e) {
    if (e.key === "Escape") {
      document.querySelectorAll(".modal[aria-hidden='false']").forEach(closeModal);
    }
  });

  // ---------------- LDAP autocomplete helpers ----------------
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
            if (items.length === 0) {
              box.style.display = "none";
              return;
            }
            items.forEach((it, idx) => {
              const el = document.createElement("div");
              el.className = "autocomplete-item";
              el.setAttribute("role","option");
              el.dataset.idx = idx;
              el.innerHTML = `<div class="a-title">${escapeHtml(it.cn || it.sAMAccountName)}</div>
                              <div class="a-sub">${escapeHtml(it.mail || '')} <span class="a-muted">${escapeHtml(it.title||'')}</span></div>`;
              el.addEventListener("click", function(){
                select(idx);
              });
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
      if (e.key === "ArrowDown") {
        e.preventDefault();
        activeIndex = Math.min(activeIndex + 1, itemsEls.length - 1);
        highlight(itemsEls);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        activeIndex = Math.max(activeIndex - 1, 0);
        highlight(itemsEls);
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (activeIndex >= 0 && itemsEls[activeIndex]) {
          const idx = itemsEls[activeIndex].dataset.idx;
          select(idx);
        }
      } else if (e.key === "Escape") {
        box.style.display = "none";
      }
    });

    document.addEventListener("click", function(ev){
      if (!box.contains(ev.target) && ev.target !== input) {
        box.style.display = "none";
      }
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
      input.value = `${it.cn || it.sAMAccountName} (${it.mail||''})`;
      hidden.value = it.sAMAccountName || it.mail || it.cn;
      box.style.display = "none";
    }

    function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];}); }
  }

  // wire pickers present in template
  wireAutocomplete("pdl_picker","pdl_username","pdl_suggestions");
  wireAutocomplete("coe_leader_picker","coe_leader_username","coe_leader_suggestions");
  wireAutocomplete("domain_lead_picker","domain_lead_username","domain_lead_suggestions");
});
