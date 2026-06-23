export class VolumeChartManager {
    constructor(canvasId, config) {
        this.ctx = document.getElementById(canvasId).getContext('2d');
        this.config = config;
        this.initChart();
    }

    initChart() {
        this.chart = new Chart(this.ctx, {
            type: 'line',
            data: {
                labels: this.config.labels,
                datasets: [
                    { label: 'Slow MA', data: this.config.slow, borderColor: '#e05555', tension: 0.3 },
                    { label: 'Fast MA', data: this.config.fast, borderColor: '#00d4aa', tension: 0.3 }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
}