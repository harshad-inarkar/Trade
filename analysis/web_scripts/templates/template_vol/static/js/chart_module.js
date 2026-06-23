/**
 * VolumeChartManager — NSE Portal
 * Industry-standard volume MA chart with Chart.js v4
 * Features: gradient fills · crosshair · golden/death cross markers ·
 *            smart x-axis calibration · formatted tooltips · end-value labels
 */

/* ─────────────────────────────────────────────
   Constants
───────────────────────────────────────────── */
const C = {
  TEAL:    '#00d4aa',
  RED:     '#e05555',
  GOLD:    '#f0c040',
  GREEN:   '#1bbf49',
  BG:      '#0b0f14',
  SURFACE: '#131920',
  BORDER:  '#1e2a35',
  TEXT:    '#c8d8e8',
  MUTED:   '#4a6070',
  MONO:    "'IBM Plex Mono', monospace",
};

/* ─────────────────────────────────────────────
   Helpers
───────────────────────────────────────────── */
function hexToRgb(hex) {
  const h = hex.replace('#', '');
  return [
    parseInt(h.slice(0, 2), 16),
    parseInt(h.slice(2, 4), 16),
    parseInt(h.slice(4, 6), 16),
  ];
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

/**
 * Format a timestamp label for the x-axis.
 * Accepts: "YYYY-MM-DD HH:MM[:SS]" (intraday) or "YYYY-MM-DD" (daily).
 */
function fmtLabel(raw, { compact = false } = {}) {
  if (!raw) return '';
  const s = String(raw).trim();
  const sep = s.includes('T') ? 'T' : ' ';
  const parts = s.split(sep);

  // Intraday — show HH:MM only
  if (parts.length >= 2) {
    return parts[1].slice(0, 5);
  }

  // Daily — show "DD MMM"
  const d = new Date(s + 'T00:00:00');
  if (isNaN(d.getTime())) return s;
  return compact
    ? d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
    : d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: '2-digit' });
}

/**
 * Smart maxTicksLimit based on dataset length and expected density.
 */
function calcMaxTicks(n) {
  if (n <= 20)  return n;
  if (n <= 80)  return 10;
  if (n <= 200) return 12;
  return 14;
}

/**
 * Detect golden (↑) and death (↓) crosses between fast and slow arrays.
 * Returns an array of { idx, type: 'golden'|'death' }.
 */
function findCrosses(fast, slow) {
  const crosses = [];
  for (let i = 1; i < fast.length; i++) {
    const f0 = fast[i - 1], f1 = fast[i];
    const s0 = slow[i - 1], s1 = slow[i];
    if (f0 == null || s0 == null || f1 == null || s1 == null) continue;
    if (f0 <= s0 && f1 > s1) crosses.push({ idx: i, type: 'golden' });
    else if (f0 >= s0 && f1 < s1) crosses.push({ idx: i, type: 'death'  });
  }
  return crosses;
}

/* ─────────────────────────────────────────────
   Custom Plugins
───────────────────────────────────────────── */

/** Vertical crosshair that follows the active tooltip */
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

/**
 * End-of-series value labels pinned to the right edge of each MA line.
 * Uses a coloured pill so they don't clash with y-axis ticks.
 */
function makeEndLabelPlugin(fastColor, slowColor) {
  return {
    id: 'nseEndLabel',
    afterDatasetsDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      if (!scales.y) return;

      chart.data.datasets.forEach((ds, i) => {
        const isMA = ds._isMA;
        if (!isMA) return;

        const meta   = chart.getDatasetMeta(i);
        const points = meta.data;
        if (!points.length) return;

        const lastPt  = points[points.length - 1];
        const lastVal = ds.data[ds.data.length - 1];
        if (lastVal == null) return;

        const text   = fmtVol(lastVal);
        const color  = ds.borderColor;
        const padX   = 6;
        const padY   = 3;
        const fSize  = 9;

        ctx.save();
        ctx.font = `600 ${fSize}px ${C.MONO}`;
        const tw = ctx.measureText(text).width;
        const bw = tw + padX * 2;
        const bh = fSize + padY * 2;
        const bx = chartArea.right + 6;
        const by = lastPt.y - bh / 2;

        // Pill background
        ctx.fillStyle = rgba(color, 0.18);
        ctx.beginPath();
        ctx.roundRect(bx, by, bw, bh, 3);
        ctx.fill();

        // Pill border
        ctx.strokeStyle = rgba(color, 0.55);
        ctx.lineWidth = 0.8;
        ctx.stroke();

        // Text
        ctx.fillStyle = color;
        ctx.textBaseline = 'middle';
        ctx.textAlign = 'left';
        ctx.fillText(text, bx + padX, by + bh / 2);
        ctx.restore();
      });
    },
  };
}

/* ─────────────────────────────────────────────
   Main class
───────────────────────────────────────────── */
export class VolumeChartManager {
  /**
   * @param {string} canvasId
   * @param {object} config
   * @param {string[]}  config.labels      — x-axis timestamps
   * @param {number[]}  config.fast        — fast MA values
   * @param {number[]}  config.slow        — slow MA values
   * @param {number[]?} config.prices      — LTP series (optional, right axis)
   * @param {number?}   config.fastPeriod  — label display only
   * @param {number?}   config.slowPeriod  — label display only
   * @param {string?}   config.timeframe   — '3' | '15' | 'D'
   */
  constructor(canvasId, config) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) throw new Error(`Canvas #${canvasId} not found`);
    this.ctx    = this.canvas.getContext('2d');
    this.config = config;
    this.chart  = null;
    this._build();
  }

  /* ── gradient factory (must be called after chart area is known) ── */
  _makeGradient(hex, a0 = 0.22, a1 = 0.0) {
    return (context) => {
      const { chart }     = context;
      const { chartArea } = chart;
      if (!chartArea) return rgba(hex, a0 * 0.5);
      const g = chart.ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
      g.addColorStop(0, rgba(hex, a0));
      g.addColorStop(1, rgba(hex, a1));
      return g;
    };
  }

  /* ── cross datasets (scatter points at MA crosses) ── */
  _buildCrossDatasets(fast, slow) {
    const crosses     = findCrosses(fast, slow);
    const goldenArr   = new Array(fast.length).fill(null);
    const deathArr    = new Array(fast.length).fill(null);

    crosses.forEach(({ idx, type }) => {
      if (type === 'golden') goldenArr[idx] = fast[idx];
      else                   deathArr[idx]  = fast[idx];
    });

    const base = {
      type: 'scatter',
      showLine: false,
      pointRadius: 7,
      pointHoverRadius: 9,
      borderWidth: 0,
      yAxisID: 'y',
      order: 0,
      _isMA: false,
    };

    return [
      {
        ...base,
        label: '▲ Golden Cross',
        data: goldenArr,
        pointStyle: 'triangle',
        rotation: 0,
        pointBackgroundColor: C.GREEN,
        pointHoverBackgroundColor: C.GREEN,
      },
      {
        ...base,
        label: '▼ Death Cross',
        data: deathArr,
        pointStyle: 'triangle',
        rotation: 180,
        pointBackgroundColor: C.RED,
        pointHoverBackgroundColor: C.RED,
      },
    ];
  }

  _build() {
    const { fast, slow, prices, fastPeriod, slowPeriod, labels, timeframe } = this.config;
    const fLabel = fastPeriod ? `Fast MA (${fastPeriod})` : 'Fast MA';
    const sLabel = slowPeriod ? `Slow MA (${slowPeriod})` : 'Slow MA';

    /* ── datasets ── */
    const datasets = [
      /* Slow MA — solid coloured line, no fill */
      {
        label: sLabel,
        data: slow,
        borderColor: C.RED,
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        tension: 0.4,
        fill: false,
        pointRadius: 0,
        pointHitRadius: 10,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: C.RED,
        pointHoverBorderColor: C.BG,
        pointHoverBorderWidth: 2,
        order: 2,
        yAxisID: 'y',
        _isMA: true,
      },
      /* Fast MA — teal line with gradient fill underneath */
      {
        label: fLabel,
        data: fast,
        borderColor: C.TEAL,
        backgroundColor: this._makeGradient(C.TEAL, 0.18, 0.0),
        borderWidth: 1.5,
        tension: 0.4,
        fill: true,
        pointRadius: 0,
        pointHitRadius: 10,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: C.TEAL,
        pointHoverBorderColor: C.BG,
        pointHoverBorderWidth: 2,
        order: 1,
        yAxisID: 'y',
        _isMA: true,
      },
      /* Cross markers */
      ...this._buildCrossDatasets(fast, slow),
    ];

    /* Optional price overlay on secondary axis */
    const hasPrice = prices?.length && prices.some(p => p != null);
    if (hasPrice) {
      datasets.push({
        label: 'LTP',
        data: prices,
        borderColor: C.GOLD,
        backgroundColor: this._makeGradient(C.GOLD, 0.08, 0.0),
        borderWidth: 1,
        tension: 0.3,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        order: 3,
        yAxisID: 'yPrice',
        _isMA: false,
      });
    }

    /* ── scales ── */
    const isDaily     = String(timeframe).toUpperCase() === 'D';
    const n           = labels.length;
    const maxTicks    = calcMaxTicks(n);
    const tickBase    = {
      color: C.MUTED,
      font: { family: C.MONO, size: 10 },
    };
    const gridBase = { color: rgba(C.BORDER, 1), drawTicks: false };

    const scales = {
      x: {
        grid: gridBase,
        border: { display: false },
        ticks: {
          ...tickBase,
          maxRotation: 0,
          minRotation: 0,
          maxTicksLimit: maxTicks,
          autoSkip: true,
          autoSkipPadding: 28,
          callback: (_, idx) => fmtLabel(labels[idx], { compact: isDaily }),
        },
      },
      y: {
        position: 'right',
        grid: gridBase,
        border: { display: false },
        ticks: {
          ...tickBase,
          maxTicksLimit: 7,
          callback: (v) => fmtVol(v),
          padding: 8,
        },
      },
    };

    if (hasPrice) {
      scales.yPrice = {
        position: 'left',
        grid: { display: false },
        border: { display: false },
        ticks: {
          ...tickBase,
          maxTicksLimit: 7,
          callback: (v) => '₹' + Number(v).toFixed(0),
          padding: 8,
        },
      };
    }

    /* ── plugins ── */
    const endLabelPlugin = makeEndLabelPlugin(C.TEAL, C.RED);

    /* ── chart init ── */
    this.chart = new Chart(this.ctx, {
      type: 'line',
      plugins: [crosshairPlugin, endLabelPlugin],
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 500, easing: 'easeInOutCubic' },
        interaction: { mode: 'index', intersect: false, axis: 'x' },
        layout: { padding: { top: 16, right: hasPrice ? 0 : 80, bottom: 4, left: hasPrice ? 64 : 0 } },
        scales,
        plugins: {
          legend: {
            display: true,
            position: 'top',
            align: 'end',
            labels: {
              color: C.TEXT,
              font: { family: C.MONO, size: 10 },
              usePointStyle: true,
              pointStyle: 'line',
              padding: 16,
              boxHeight: 1,
              filter: (item) => !item.text.includes('Cross') || true,
            },
          },
          tooltip: {
            backgroundColor: C.SURFACE,
            borderColor: C.BORDER,
            borderWidth: 1,
            titleColor: C.TEXT,
            bodyColor: C.MUTED,
            titleFont: { family: C.MONO, size: 10, weight: '600' },
            bodyFont:  { family: C.MONO, size: 10 },
            padding: 12,
            cornerRadius: 4,
            displayColors: true,
            boxWidth: 8,
            boxHeight: 8,
            boxPadding: 4,
            callbacks: {
              title: (items) => {
                const raw = items[0]?.label ?? '';
                // Show full label in tooltip (not truncated)
                const s = String(raw).trim();
                return s;
              },
              label: (ctx) => {
                if (ctx.raw == null) return null;
                const ds  = ctx.dataset;
                const val = Number(ctx.raw);
                const fmt = ds.yAxisID === 'yPrice'
                  ? '₹' + val.toFixed(2)
                  : fmtVol(val);
                return `  ${ds.label}: ${fmt}`;
              },
            },
          },
        },
      },
    });
  }

  /** Expose chart instance for external access */
  get instance() {
    return this.chart;
  }

  /** Update data without full rebuild */
  update({ labels, fast, slow, prices } = {}) {
    if (!this.chart) return;
    if (labels) this.chart.data.labels = labels;
    if (fast)   this.chart.data.datasets[1].data = fast; // index matches dataset order above
    if (slow)   this.chart.data.datasets[0].data = slow;
    this.chart.update('active');
  }

  destroy() {
    this.chart?.destroy();
    this.chart = null;
  }
}
