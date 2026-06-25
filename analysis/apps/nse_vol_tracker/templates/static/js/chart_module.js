/**
 * VolumeChartManager — NSE Portal
 * Native Javascript Multi-Indicator Overlay Engine
 * (Mathematical computations delegated strictly to FastAPI backend)
 */

const C = {
  BG: '#000000',
  SURFACE: '#000000',
  BORDER: '#1e2a35',
  TEXT: '#c8d8e8',
  MUTED: '#4a6070',
  MONO: "'IBM Plex Mono', monospace",
};

// Indicator Color Ladder: Fastest -> Slowest
const MA_COLORS = ['#c0c0c0', '#00ffff', '#ffff00', '#ff0000'];
const DEFAULT_MA_COLOR = '#ffffff';

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
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return Math.round(n).toLocaleString();
}

function fmtLabel(raw, isDaily = false) {
  if (!raw) return '';
  const s = String(raw).trim();
  const parts = s.split(' '); // Splits "123: 15/06_0915"

  if (parts.length >= 2) {
    const dtStr = parts[1]; // "15/06_0915"
    if (dtStr.includes('_')) {
      const [d, t] = dtStr.split('_');
      if (isDaily) return d; // If Daily TF, only show "15/06"
      return `${d} ${t.slice(0, 2)}:${t.slice(2)}`; // For intraday, show "15/06 09:15"
    }
    return dtStr;
  }
  return s;
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
        if (chart.getDatasetMeta(i).hidden) return;
        const points = chart.getDatasetMeta(i).data;
        if (!points.length) return;

        const lastPt = points[points.length - 1];
        const lastVal = ds.data[ds.data.length - 1];
        if (lastVal == null) return;

        const source = ds.customSource;
        const text = source === 'price' ? '₹' + Number(lastVal).toFixed(2) : fmtVol(lastVal);

        const color = ds.borderColor;
        const padX = 6, padY = 3, fSize = 9;

        ctx.save();
        ctx.font = `600 ${fSize}px ${C.MONO}`;
        const tw = ctx.measureText(text).width;
        const bw = tw + padX * 2, bh = fSize + padY * 2;
        const bx = chartArea.right + 6, by = lastPt.y - bh / 2;

        ctx.fillStyle = rgba(color, 0.25);
        ctx.beginPath();
        ctx.roundRect(bx, by, bw, bh, 3);
        ctx.fill();

        ctx.strokeStyle = color;
        ctx.lineWidth = 1;
        ctx.stroke();

        ctx.fillStyle = '#fff';
        ctx.textBaseline = 'middle';
        ctx.textAlign = 'left';
        ctx.fillText(text, bx + padX, by + bh / 2);
        ctx.restore();
      });
    },
  };
}

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

class ChartManager {
  constructor(canvasId) {
    this.canvas = document.getElementById(canvasId);
    this.ctx = this.canvas.getContext('2d');
    this.chart = null;
  }

  build(labels, datasets, primarySource, tf) {
    if (this.chart) this.chart.destroy();

    const tickBase = { color: C.MUTED, font: { family: C.MONO, size: 10 } };
    const gridBase = { display: false, drawTicks: false };

    this.chart = new Chart(this.ctx, {
      type: 'line',
      plugins: [crosshairPlugin, makeEndLabelPlugin()],
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        interaction: { mode: 'index', intersect: false, axis: 'x' },
        layout: { padding: { top: 16, right: 80, bottom: 4, left: 0 } },
        scales: {
          x: {
            grid: gridBase, border: { display: false },
            ticks: {
              ...tickBase,
              maxRotation: 0,
              minRotation: 0,
              maxTicksLimit: 14,
              autoSkip: true,
              autoSkipPadding: 28,
              // FIXED: Uses 'val' (the exact zoomed data index) instead of 'idx' (the screen tick index)
              callback: (val) => {
                const isDaily = this.chart?.options?.plugins?.customContext?.tf === 'D';
                return fmtLabel(labels[val], isDaily);
              }
            },
          },
          y: {
            beginAtZero: false,
            position: 'right', grid: gridBase, border: { display: false },
            ticks: {
              ...tickBase,
              maxTicksLimit: 7,
              callback: function (v) {
                const src = this.chart?.options?.plugins?.customContext?.primarySource || 'volume';
                return src === 'price' ? '₹' + Number(v).toFixed(2) : fmtVol(v);
              },
              padding: 8
            },
          },
        },
        plugins: {
          legend: { display: false },
          customContext: { primarySource, tf },
          zoom: {
            pan: { enabled: true, mode: 'x', onPan: updateDynamicY },
            zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x', onZoom: updateDynamicY }
          },
          tooltip: {
            backgroundColor: '#131920', borderColor: C.BORDER, borderWidth: 1,
            titleColor: C.TEXT, bodyColor: C.MUTED,
            titleFont: { family: C.MONO, size: 10, weight: '600' }, bodyFont: { family: C.MONO, size: 10 },
            padding: 12, cornerRadius: 4, displayColors: true, boxWidth: 8, boxHeight: 8, boxPadding: 4,
            callbacks: {
              title: (items) => {
                const raw = items[0]?.label ?? '';
                const isDaily = this.chart?.options?.plugins?.customContext?.tf === 'D';
                return fmtLabel(raw, isDaily);
              },
              label: (ctx) => {
                const val = Number(ctx.raw);
                const src = ctx.dataset.customSource;
                const fmt = src === 'price' ? '₹' + val.toFixed(2) : fmtVol(val);
                return `  ${ctx.dataset.label}: ${fmt}`;
              },
            },
          },
        },
      },
    });
  }

  update(labels, datasets, primarySource, tf) {
    if (!this.chart) {
      this.build(labels, datasets, primarySource, tf);
    } else {
      this.chart.data.labels = labels;
      this.chart.data.datasets = datasets;
      this.chart.options.plugins.customContext = { primarySource, tf };

      this.chart.resetZoom('none');
      delete this.chart.options.scales.y.min;
      delete this.chart.options.scales.y.max;

      this.chart.update('none');
    }
  }
}

/* ─────────────────────────────────────────────
   State Management & Backend Fetching
───────────────────────────────────────────── */
const rawDataEl = document.getElementById('rawData');
const configEl = document.getElementById('chartConfig');

if (rawDataEl && configEl) {
  const rawData = JSON.parse(rawDataEl.textContent);
  const globalCfg = JSON.parse(configEl.textContent);

  const SYMBOL = globalCfg.symbol;
  const TF = globalCfg.timeframe;

  const rows = rawData.slice(1);
  const labels = rows.map(r => r[0]);

  const STATE_KEY = 'nse_sym_api_prefs_v2';
  let state = JSON.parse(localStorage.getItem(STATE_KEY)) || {
    indicators: [
      { id: 1, source: 'price', type: 'rma', p1: 8, data: null },
      { id: 2, source: 'price', type: 'rma', p1: 21, data: null }
    ],
    nextId: 3
  };

  const chart = new ChartManager('mainChart');
  const chipContainer = document.getElementById('indicatorChips');
  const tfSel = document.getElementById('symTf');
  const addBtn = document.getElementById('addIndBtn');
  const addBtnBaseText = addBtn.innerHTML;

  async function fetchIndicator(source, type, p1) {
    try {
      const res = await fetch(`/api/indicator?symbol=${SYMBOL}&tf=${TF}&source=${source}&ind_type=${type}&p1=${p1}`);
      if (!res.ok) throw new Error("API Error");
      const json = await res.json();
      return json.data;
    } catch (e) {
      console.error("Failed to fetch indicator:", e);
      return null;
    }
  }

  async function renderState() {
    state.indicators.sort((a, b) => parseInt(a.p1) - parseInt(b.p1));
    const cacheState = {
      nextId: state.nextId,
      indicators: state.indicators.map(ind => ({ id: ind.id, source: ind.source, type: ind.type, p1: ind.p1 }))
    };
    localStorage.setItem(STATE_KEY, JSON.stringify(cacheState));

    let fetches = state.indicators.map(async (ind) => {
      if (!ind.data) {
        ind.data = await fetchIndicator(ind.source, ind.type, ind.p1);
      }
    });

    if (fetches.length > 0) {
      addBtn.innerHTML = "↻...";
      addBtn.disabled = true;
      await Promise.all(fetches);
      addBtn.innerHTML = addBtnBaseText;
      addBtn.disabled = false;
    }

    chipContainer.innerHTML = '';
    state.indicators.forEach((ind, i) => {
      let color = MA_COLORS[i] || DEFAULT_MA_COLOR;
      let chip = document.createElement('div');
      chip.className = 'ind-chip';
      chip.innerHTML = `
                <div class="ind-color" style="background: ${color};"></div>
                ${ind.source.toUpperCase().substring(0, 3)} ${ind.type.toUpperCase()} ${ind.type === 'raw' ? '' : ind.p1}
                <span class="ind-close" data-id="${ind.id}">×</span>
            `;
      chipContainer.appendChild(chip);
    });

    let datasets = [];

    state.indicators.forEach((ind, i) => {
      if (!ind.data) return;
      let color = MA_COLORS[i] || DEFAULT_MA_COLOR;
      datasets.push({
        label: `${ind.source.toUpperCase().substring(0, 3)} ${ind.type.toUpperCase()} ${ind.type === 'raw' ? '' : ind.p1}`.trim(),
        data: ind.data,
        borderColor: color,
        borderWidth: 1.5,
        tension: ind.type === 'raw' ? 0.1 : 0.4,
        pointRadius: 0,
        pointHitRadius: 10,
        yAxisID: 'y',
        customSource: ind.source
      });
    });

    const primarySource = state.indicators.length > 0 ? state.indicators[0].source : 'price';
    chart.update(labels, datasets, primarySource, TF);
  }

  addBtn.addEventListener('click', () => {
    const src = document.getElementById('indSource').value;
    const type = document.getElementById('indType').value;
    const period = document.getElementById('indPeriod').value;

    if (type !== 'raw' && (!period || period < 1)) return;

    state.indicators.push({
      id: state.nextId++,
      source: src,
      type: type,
      p1: type === 'raw' ? 1 : parseInt(period),
      data: null
    });
    renderState();
  });

  chipContainer.addEventListener('click', (e) => {
    if (e.target.classList.contains('ind-close')) {
      const idToRemove = parseInt(e.target.getAttribute('data-id'));
      state.indicators = state.indicators.filter(ind => ind.id !== idToRemove);
      renderState();
    }
  });

  tfSel.addEventListener('change', (e) => {
    const params = new URLSearchParams(window.location.search);
    params.set('tf', e.target.value);
    window.location.search = params.toString();
  });

  renderState();
}