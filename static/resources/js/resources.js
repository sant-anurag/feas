// static/resources/js/resources.js
document.addEventListener("DOMContentLoaded", function() {
  const cfg = window._resources_config || {};

  // --------- Sync start & poll --------------
  const startBtn = document.getElementById("startSyncBtn");
  const syncStatus = document.getElementById("syncStatus");
  const progressFill = document.getElementById("progressFill");
  const processedText = document.getElementById("processedText");
  const totalText = document.getElementById("totalText");
  const errorsText = document.getElementById("errorsText");

  let pollTimer = null;
  let currentJobId = null;

  function pollProgress() {
    if (!currentJobId) return;
    fetch(`${cfg.sync_progress_url}?job_id=${currentJobId}`)
      .then(r => r.json())
      .then(data => {
        if (!data.ok) {
          syncStatus.textContent = "Error getting progress";
          return;
        }
        const job = data.job;
        syncStatus.textContent = job.status;
        const total = job.total_count || 0;
        const processed = job.processed_count || 0;
        const errors = job.errors_count || 0;
        processedText.textContent = `Processed: ${processed}`;
        totalText.textContent = `Total: ${total}`;
        errorsText.textContent = errors ? `Errors: ${errors}` : "";
        let pct = 0;
        if (total > 0) pct = Math.min(100, Math.round((processed / total) * 100));
        progressFill.style.width = pct + "%";

        if (job.status === "COMPLETED" || job.status === "FAILED") {
          if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        }
      }).catch(err => {
        console.error("progress error", err);
      });
  }

  if (startBtn) {
    startBtn.addEventListener("click", function() {
      if (!confirm("Start a full LDAP sync? This may take some time.")) return;
      startBtn.disabled = true;
      syncStatus.textContent = "Starting...";
      fetch(cfg.sync_start_url, {method: "POST", headers: {'X-CSRFToken': getCookie('csrftoken')}})
        .then(r => r.json())
        .then(data => {
          if (!data.ok) {
            syncStatus.textContent = data.error || "Failed to start";
            startBtn.disabled = false;
            return;
          }
          currentJobId = data.job_id;
          syncStatus.textContent = "RUNNING";
          pollProgress();
          pollTimer = setInterval(pollProgress, 2500);
        }).catch(err => {
          console.error("start failed", err);
          syncStatus.textContent = "Start failed";
          startBtn.disabled = false;
        });
    });
  }

  // --------- local search (min 3 chars) ----------
  const searchInput = document.getElementById("localSearch");
  const searchBtn = document.getElementById("searchBtn");
  const employeesBody = document.getElementById("employeesBody");

  function runLocalSearch(q) {
    if (!q || q.length < 3) return;
    fetch(`${cfg.local_search_url}?q=${encodeURIComponent(q)}`)
      .then(r => r.json())
      .then(data => {
        const rows = data.results || [];
        if (employeesBody) {
          employeesBody.innerHTML = "";
          if (rows.length === 0) {
            employeesBody.innerHTML = '<tr><td colspan="6">No results</td></tr>';
            return;
          }
          for (const r of rows) {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td>${escapeHtml(r.username||'')}</td>
                            <td>${escapeHtml(r.cn||'')}</td>
                            <td>${escapeHtml(r.email||'')}</td>
                            <td>${escapeHtml(r.title||'')}</td>
                            <td>${escapeHtml(r.department||'')}</td>
                            <td><button class="action-btn edit view-emp-btn" data-id="${r.id}">View</button></td>`;
            employeesBody.appendChild(tr);
          }
        }
      }).catch(err => console.error("search err", err));
  }

  if (searchBtn) {
    searchBtn.addEventListener("click", function() {
      const v = (searchInput.value || "").trim();
      if (v.length < 3) { alert("Type at least 3 characters"); return; }
      runLocalSearch(v);
    });
  }

  if (searchInput) {
    searchInput.addEventListener("keyup", function(e) {
      if (e.key === "Enter") {
        const v = (searchInput.value || "").trim();
        if (v.length >= 3) runLocalSearch(v);
      }
    });
  }

  // --------- View modal (fetch profile) -------------
  document.addEventListener("click", function(ev) {
    const b = ev.target.closest(".view-emp-btn");
    if (!b) return;
    const id = b.dataset.id;
    if (!id) return;
    fetch(cfg.profile_url_template.replace("{id}", id))
      .then(r => r.json())
      .then(data => {
        if (!data.ok) { alert("Cannot load profile"); return; }
        openProfileModal(data.record);
      }).catch(err => console.error("profile err", err));
  });

  // --- Modal utility functions ---
  function openProfileModal(rec) {
    const modal = document.getElementById("empModal");
    const content = document.getElementById("empModalContent");
    if (!modal || !content) return;

    const attrs = rec.attributes || {};
    const avatarText = (rec.username || "").slice(0,1).toUpperCase();
    const displayName = (attrs.cn && String(attrs.cn)) || rec.username || "";
    const email = rec.email || attrs.mail || "";
    const title = attrs.title || "";
    const dept = attrs.department || "";

    const kv_html = `
      <div class="profile-header">
        <div class="profile-avatar">${escapeHtml(avatarText)}</div>
        <div>
          <h3 id="empModalTitle" class="profile-title">${escapeHtml(displayName)}</h3>
          <div class="profile-sub">${escapeHtml(title)} ${title && dept ? '&middot; ' : ''}${escapeHtml(dept)}</div>
          <div class="kv-grid" style="margin-top:12px;">
            <div class="kv-label">Username</div><div class="kv-value">${escapeHtml(rec.username || '')}</div>
            <div class="kv-label">CN</div><div class="kv-value">${escapeHtml(attrs.cn || '')}</div>
            <div class="kv-label">Email</div><div class="kv-value">${escapeHtml(email)}</div>
            <div class="kv-label">Telephone</div><div class="kv-value">${escapeHtml(attrs.telephoneNumber || attrs.mobile || '')}</div>
            <div class="kv-label">Manager DN</div><div class="kv-value">${escapeHtml(attrs.manager || '')}</div>
          </div>
        </div>
      </div>
      <div class="attributes-box">
        <div style="font-weight:700;margin-bottom:8px;">Full LDAP attributes</div>
        <pre id="attrPre">${escapeHtml(JSON.stringify(attrs, null, 2))}</pre>
      </div>
    `;

    content.innerHTML = kv_html;
    _showModal(modal);
    const closeBtn = modal.querySelector(".modal-close");
    if (closeBtn) closeBtn.focus();
  }

  function _showModal(modal) {
    document.body.style.overflow = "hidden";
    modal.style.display = "flex";
    modal.setAttribute("aria-hidden", "false");
    document.addEventListener("keydown", _modalKeyHandler);
  }

  function _hideModal(modal) {
    document.body.style.overflow = "";
    modal.style.display = "none";
    modal.setAttribute("aria-hidden", "true");
    document.removeEventListener("keydown", _modalKeyHandler);
  }

  function _modalKeyHandler(e) {
    if (e.key === "Escape") {
      const modal = document.getElementById("empModal");
      if (modal && modal.style.display !== "none") {
        _hideModal(modal);
      }
    }
  }

  // click outside overlay & close button
  document.addEventListener("click", function(ev) {
    const closeBtn = ev.target.closest(".modal-close");
    if (closeBtn) {
      const modalId = closeBtn.getAttribute("data-close") || "empModal";
      const modal = document.getElementById(modalId);
      if (modal) _hideModal(modal);
      return;
    }
    const overlay = ev.target.closest(".modal-overlay");
    if (overlay && ev.target === overlay) {
      _hideModal(overlay);
    }
  });

  // basic helpers
  function getCookie(name) {
    const v = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
    return v ? v.pop() : '';
  }
  function escapeHtml(s) {
    if (!s) return '';
    return (''+s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }
});
