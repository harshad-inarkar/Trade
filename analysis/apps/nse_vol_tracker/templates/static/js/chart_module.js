/**
 * VolumeChartManager — NSE Portal
 * Clean, dual-purpose MA chart (Volume / Price)
 * Features: Infinite X-Zoom/Pan, Dynamic Y-Scaling, Gridless Dark Mode
 */

const C = {
  TEAL:    '#00d4aa',
  RED:     '#e05555',
  BG:      '#0b0f14',
  SURFACE: '#131920',
  BORDER:  '#1e2a35',
  TEXT:    '#c8d8e8',
  MUTED:   '#4a6070',
  MONO:    "'IBM Plex Mono', monospace",
};

function hexToRgb(hex) {
  const h = hex.replace('#', '');
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}

function rgba(hex, a) {
  const [r, g, b] = hexToRgb(hex);
  return `rgba(${r},${g},${b},${a})`;
}

function fmtVol(v) {
  if (v == null || isNaN(v)) return '—';
  const n = Number(v);
  if (n >= 1e6)  return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3)  return (n / 1e3).toFixed(1)  + 'K';
  return Math.round(n).toLocaleString();
}

function fmtLabel(raw, { compact = false } = {}) {
  if (!raw) return '';
  const s = String(raw).trim();
  const sep = s.includes('T') ? 'T' : ' ';
  const parts = s.split(sep);
  if (parts.length >= 2) return parts[1].slice(0, 5);
  const d = new Date(s + 'T00:00:00');
  if (isNaN(d.getTime())) return s;
  return compact
    ? d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
    : d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: '2-digit' });
}

function calcMaxTicks(n) {
  if (n <= 20)  return n;
  if (n <= 80)  return 10;
  if (n <= 200) return 12;
  return 14;
}

const crosshairPlugin = {
  id: 'nseCrosshair',
  afterDraw(chart) {
    const active = chart.tooltip?._active;
    if (!active?.length) return;
    const { ctx, chartArea } = chart;
    const x = active[0].element.x;
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.lineWidth = 1;
    ctx.strokeStyle = rgba(C.TEXT, 0.15);
    ctx.setLineDash([3, 5]);
    ctx.stroke();
    ctx.restore();
  },
};

function makeEndLabelPlugin() {
  return {
    id: 'nseEndLabel',
    afterDatasetsDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      if (!scales.y) return;

      chart.data.datasets.forEach((ds, i) => {
        const meta   = chart.getDatasetMeta(i);
        const points = meta.data;
        if (!points.length) return;

        const lastPt  = points[points.length - 1];
        const lastVal = ds.data[ds.data.length - 1];
        if (lastVal == null) return;

        const isPrice = chart.options.plugins.customContext.isPrice;
        const text    = isPrice ? '₹' + Number(lastVal).toFixed(2) : fmtVol(lastVal);
        
        const color  = ds.borderColor;
        const padX   = 6, padY = 3, fSize = 9;

        ctx.save();
        ctx.font = `600 ${fSize}px ${C.MONO}`;
        const tw = ctx.measureText(text).width;
        const bw = tw + padX * 2, bh = fSize + padY * 2;
        const bx = chartArea.right + 6, by = lastPt.y - bh / 2;

        ctx.fillStyle = rgba(color, 0.18);
        ctx.beginPath();
        ctx.roundRect(bx, by, bw, bh, 3);
        ctx.fill();

        ctx.strokeStyle = rgba(color, 0.55);
        ctx.lineWidth = 0.8;
        ctx.stroke();

        ctx.fillStyle = color;
        ctx.textBaseline = 'middle';
        ctx.textAlign = 'left';
        ctx.fillText(text, bx + padX, by + bh / 2);
        ctx.restore();
      });
    },
  };
}

/** * Calculates the Min and Max Y-axis bounds based solely on the data 
 * currently visible within the X-axis Zoom frame. 
 */
function updateDynamicY({ chart }) {
  const xScale = chart.scales.x;
  const minIndex = Math.max(0, Math.floor(xScale.min));
  const maxIndex = Math.min(chart.data.labels.length - 1, Math.ceil(xScale.max));

  let minVal = Infinity;
  let maxVal = -Infinity;

  chart.data.datasets.forEach((ds, i) => {
    if (chart.getDatasetMeta(i).hidden) return;
    for (let j = minIndex; j <= maxIndex; j++) {
      const v = ds.data[j];
      if (v != null) {
        minVal = Math.min(minVal, v);
        maxVal = Math.max(maxVal, v);
      }
    }
  });

  if (minVal !== Infinity && maxVal !== -Infinity) {
    const pad = (maxVal - minVal) * 0.1 || (minVal * 0.05);
    chart.options.scales.y.min = minVal - pad;
    chart.options.scales.y.max = maxVal + pad;
  }
}


export class VolumeChartManager {
  constructor(canvasId, config) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) throw new Error(`Canvas #${canvasId} not found`);
    this.ctx    = this.canvas.getContext('2d');
    this.config = { isPrice: false, ...config };
    this.chart  = null;
    this._build();
  }

  _build() {
    const { fast, slow, fastPeriod, slowPeriod, labels, timeframe } = this.config;
    const fLabel = fastPeriod ? `Fast MA (${fastPeriod})` : 'Fast MA';
    const sLabel = slowPeriod ? `Slow MA (${slowPeriod})` : 'Slow MA';

    const datasets = [
      {
        label: sLabel,
        data: slow,
        borderColor: C.RED,
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        tension: 0.4,
        fill: false, // Absolutely no gradient fill under curve
        pointRadius: 0,
        pointHitRadius: 10,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: C.RED,
        pointHoverBorderColor: C.BG,
        pointHoverBorderWidth: 2,
        yAxisID: 'y'
      },
      {
        label: fLabel,
        data: fast,
        borderColor: C.TEAL,
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        tension: 0.4,
        fill: false, // Absolutely no gradient fill under curve
        pointRadius: 0,
        pointHitRadius: 10,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: C.TEAL,
        pointHoverBorderColor: C.BG,
        pointHoverBorderWidth: 2,
        yAxisID: 'y'
      }
    ];

    const isDaily = String(timeframe).toUpperCase() === 'D';
    const tickBase = { color: C.MUTED, font: { family: C.MONO, size: 10 } };
    
    // Disable all background grids
    const gridBase = { display: false, drawTicks: false };

    this.chart = new Chart(this.ctx, {
      type: 'line',
      plugins: [crosshairPlugin, makeEndLabelPlugin()],
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 500, easing: 'easeInOutCubic' },
        interaction: { mode: 'index', intersect: false, axis: 'x' },
        layout: { padding: { top: 16, right: 80, bottom: 4, left: 0 } },
        scales: {
          x: {
            grid: gridBase, border: { display: false },
            ticks: { ...tickBase, maxRotation: 0, minRotation: 0, maxTicksLimit: calcMaxTicks(labels.length), autoSkip: true, autoSkipPadding: 28, callback: (_, idx) => fmtLabel(labels[idx], { compact: isDaily }) },
          },
          y: {
            beginAtZero: false, // Forces dynamic scaling range
            position: 'right', grid: gridBase, border: { display: false },
            ticks: { 
              ...tickBase, 
              maxTicksLimit: 7, 
              callback: (v) => this.config.isPrice ? '₹' + Number(v).toFixed(2) : fmtVol(v),
              padding: 8 
            },
          },
        },
        plugins: {
          legend: { display: false },
          customContext: { isPrice: this.config.isPrice },
          
          // Infinite Pan & Zoom Hooks
          zoom: {
            pan: {
              enabled: true,
              mode: 'x',
              onPan: updateDynamicY
            },
            zoom: {
              wheel: { enabled: true },
              pinch: { enabled: true },
              mode: 'x',
              onZoom: updateDynamicY
            }
          },

          tooltip: {
            backgroundColor: C.SURFACE, borderColor: C.BORDER, borderWidth: 1,
            titleColor: C.TEXT, bodyColor: C.MUTED,
            titleFont: { family: C.MONO, size: 10, weight: '600' }, bodyFont: { family: C.MONO, size: 10 },
            padding: 12, cornerRadius: 4, displayColors: true, boxWidth: 8, boxHeight: 8, boxPadding: 4,
            callbacks: {
              title: (items) => String(items[0]?.label ?? '').trim(),
              label: (ctx) => {
                const val = Number(ctx.raw);
                const fmt = this.config.isPrice ? '₹' + val.toFixed(2) : fmtVol(val);
                return `  ${ctx.dataset.label}: ${fmt}`;
              },
            },
          },
        },
      },
    });
  }

  update({ fast, slow, isPrice } = {}) {
    if (!this.chart) return;
    if (fast) this.chart.data.datasets[1].data = fast;
    if (slow) this.chart.data.datasets[0].data = slow;
    if (isPrice !== undefined) {
        this.config.isPrice = isPrice;
        this.chart.options.plugins.customContext.isPrice = isPrice;
    }
    
    // Clear custom Y zoom limits when changing tabs to prevent the new series from flat-lining
    this.chart.resetZoom('none');
    delete this.chart.options.scales.y.min;
    delete this.chart.options.scales.y.max;
    
    this.chart.update('none');
  }
}

/* ─────────────────────────────────────────────
   Page Initialization & Event Binding
───────────────────────────────────────────── */
const rawDataEl = document.getElementById('rawData');
const configEl = document.getElementById('chartConfig');

if (rawDataEl && configEl) {
    const rawData = JSON.parse(rawDataEl.textContent);
    const config = JSON.parse(configEl.textContent);
    const rows = rawData.slice(1);

    const labels    = rows.map(r => r[0]);
    const volSlow   = rows.map(r => r[2]);
    const volFast   = rows.map(r => r[3]);
    const priceFast = rows.map(r => r[5]);
    const priceSlow = rows.map(r => r[6]);

    const TAB_KEY = 'nse_active_tab';
    let isPriceTab = sessionStorage.getItem(TAB_KEY) === 'price';

    const tabVol = document.getElementById('tabVol');
    const tabPrice = document.getElementById('tabPrice');

    if (isPriceTab) {
        tabPrice.classList.add('active');
        tabVol.classList.remove('active');
    }

    function updateFooterStats() {
      const fastArr = isPriceTab ? priceFast : volFast;
      const slowArr = isPriceTab ? priceSlow : volSlow;
      
      const lastFast = fastArr[fastArr.length - 1];
      const lastSlow = slowArr[slowArr.length - 1];
      const ratio    = lastSlow ? (lastFast / lastSlow) : null;
      const isBull   = lastFast > lastSlow;

      function fmt(v) {
        if (v == null || isNaN(v)) return '—';
        if (isPriceTab) return '₹' + Number(v).toFixed(2);
        const n = Number(v);
        if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1)  + 'K';
        return Math.round(n).toLocaleString();
      }

      document.getElementById('statFastNow').textContent = fmt(lastFast);
      document.getElementById('statSlowNow').textContent = fmt(lastSlow);
      document.getElementById('statRatio').textContent   = ratio ? ratio.toFixed(3) : '—';
      document.getElementById('statBars').textContent    = rows.length;

      const sigEl = document.getElementById('statSignal');
      if (lastFast != null && lastSlow != null) {
        sigEl.textContent = isBull ? '↑ BULL' : '↓ BEAR';
        sigEl.className = 'stat-val ' + (isBull ? 'c-green' : 'c-red');
      }
    }

    const chart = new VolumeChartManager('mainChart', {
      labels,
      fast: isPriceTab ? priceFast : volFast,
      slow: isPriceTab ? priceSlow : volSlow,
      fastPeriod: config.fastPeriod,
      slowPeriod: config.slowPeriod,
      timeframe: config.timeframe,
      isPrice: isPriceTab
    });

    updateFooterStats();

    tabVol.addEventListener('click', () => {
      if (!isPriceTab) return;
      isPriceTab = false;
      sessionStorage.setItem(TAB_KEY, 'volume');
      tabVol.classList.add('active');
      tabPrice.classList.remove('active');
      chart.update({ fast: volFast, slow: volSlow, isPrice: false });
      updateFooterStats();
    });

    tabPrice.addEventListener('click', () => {
      if (isPriceTab) return;
      isPriceTab = true;
      sessionStorage.setItem(TAB_KEY, 'price');
      tabPrice.classList.add('active');
      tabVol.classList.remove('active');
      chart.update({ fast: priceFast, slow: priceSlow, isPrice: true });
      updateFooterStats();
    });
}