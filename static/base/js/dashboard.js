// dashboard.js (load after Chart.js included)
async function loadPdlCostChart(year){
  const r = await fetch(`/dashboard/api/pdl_cost_series/${year}/`);
  if(!r.ok) return;
  const d = await r.json();
  const ctx = document.getElementById('pdlCostChart').getContext('2d');
  if(window._pdlChart) window._pdlChart.destroy();
  window._pdlChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: d.labels,
      datasets: [
        { label: 'Consumed', data: d.consumed, borderColor:'#1552db', backgroundColor:'rgba(21,82,219,0.12)', tension:0.25, fill:true },
        { label: 'Estimated', data: d.estimated, borderColor:'#2ea7ff', backgroundColor:'rgba(46,167,255,0.06)', borderDash:[6,4], tension:0.3 },
      ]
    },
    options:{responsive:true,plugins:{legend:{position:'top'}}}
  });
}

document.addEventListener('DOMContentLoaded', function(){
  const yearSelect = document.getElementById('yearSelect');
  if(yearSelect){
    yearSelect.addEventListener('change', (e)=> loadPdlCostChart(e.target.value));
    loadPdlCostChart(yearSelect.value);
  }
  // user and manager charts similar...
});
