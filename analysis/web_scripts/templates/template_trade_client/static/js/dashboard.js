/**
 * UI Manager - Handles DOM interactions like Sorting and Tabbing
 */
class UITableManager {
    constructor() {
        this.currentSort = { 
            active: { col: null, isNum: false, isAsc: true }, 
            closed: { col: null, isNum: false, isAsc: true } 
        };
    }

    togglePositions(view) {
        const viewActive = document.getElementById('view-active');
        const viewClosed = document.getElementById('view-closed');
        const tabActive = document.getElementById('tab-active');
        const tabClosed = document.getElementById('tab-closed');

        if (viewActive && viewClosed && tabActive && tabClosed) {
            viewActive.style.display = view === 'active' ? 'block' : 'none';
            viewClosed.style.display = view === 'closed' ? 'block' : 'none';
            
            tabActive.classList.toggle('active', view === 'active');
            tabClosed.classList.toggle('active', view === 'closed');
        }
    }

    sortTable(tableId, colIndex, isNumeric, forceAsc = null) {
        const table = document.getElementById(tableId);
        if (!table) return;
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        if (rows.length === 0 || (rows.length === 1 && rows[0].cells.length === 1)) return;

        const header = table.querySelectorAll('th')[colIndex];
        let isAsc = forceAsc !== null ? forceAsc : !header.classList.contains('sort-asc');

        table.querySelectorAll('th').forEach(th => th.classList.remove('sort-asc', 'sort-desc'));
        header.classList.add(isAsc ? 'sort-asc' : 'sort-desc');

        const stateKey = tableId === 'table-active' ? 'active' : 'closed';
        this.currentSort[stateKey] = { col: colIndex, isNum: isNumeric, isAsc: isAsc };

        const dir = isAsc ? 1 : -1;

        rows.sort((a, b) => {
            let aCol = a.querySelectorAll('td')[colIndex];
            let bCol = b.querySelectorAll('td')[colIndex];
            let aText = aCol ? aCol.textContent.trim() : '';
            let bText = bCol ? bCol.textContent.trim() : '';

            if (isNumeric) {
                let aNum = parseFloat(aText.replace(/[^0-9.-]+/g,"")) || 0;
                let bNum = parseFloat(bText.replace(/[^0-9.-]+/g,"")) || 0;
                return (aNum - bNum) * dir;
            } else {
                return aText.localeCompare(bText) * dir;
            }
        });

        rows.forEach((row, index) => {
            let firstCell = row.querySelector('td');
            if (firstCell && !isNaN(parseInt(firstCell.textContent))) {
                firstCell.textContent = index + 1;
            }
            tbody.appendChild(row);
        });
    }

    restoreSortState() {
        if (this.currentSort.active.col !== null) {
            this.sortTable('table-active', this.currentSort.active.col, this.currentSort.active.isNum, this.currentSort.active.isAsc);
        }
        if (this.currentSort.closed.col !== null) {
            this.sortTable('table-closed', this.currentSort.closed.col, this.currentSort.closed.isNum, this.currentSort.closed.isAsc);
        }
    }
}

/**
 * Symbol Search - Handles Autocomplete API Fetching & Rendering
 */
class SymbolSearch {
    constructor() {
        this.debounceTimer = null;
        this.reqId = 0;
        this.setupListeners();
    }

    hideDropdown() {
        const dropdown = document.getElementById('symbol-search-dropdown');
        if (dropdown) dropdown.style.display = 'none';
    }

    renderDropdown(items, input) {
        const dropdown = document.getElementById('symbol-search-dropdown');
        if (!dropdown) return;

        dropdown.innerHTML = '';
        if (!Array.isArray(items) || items.length === 0) {
            dropdown.style.display = 'none';
            return;
        }

        items.forEach(item => {
            const row = document.createElement('div');
            row.className = 'symbol-search-item';
            row.textContent = item.display || '';

            row.addEventListener('mousedown', (e) => {
                e.preventDefault();
                input.value = item.symbol || '';
                const form = input.closest('form');

                if (form) {
                    form.querySelector('[name="exchange"]').value = item.exch || '';
                    form.querySelector('[name="inst_type"]').value = item.inst_type || '';

                    if (item.inst_type === 'OPT' || item.inst_type === 'FUT') {
                        form.querySelector('[name="expiry"]').value = item.expiry || '';
                    } else {
                        form.querySelector('[name="expiry"]').value = '';
                    }

                    if (item.inst_type === 'OPT') {
                        form.querySelector('[name="strike"]').value = item.strike || 0;
                        form.querySelector('[name="opt_type"]').value = item.opt_type || '';
                    } else {
                        form.querySelector('[name="strike"]').value = '0.0';
                        form.querySelector('[name="opt_type"]').value = '';
                    }
                }
                this.hideDropdown();
            });
            dropdown.appendChild(row);
        });
        dropdown.style.display = 'block';
    }

    setupListeners() {
        document.addEventListener('input', (e) => {
            if (e.target?.name !== 'symbol' || e.target.tagName !== 'INPUT') return;
            const input = e.target;
            const val = input.value.trim();

            if (val.length < 2) {
                this.hideDropdown();
                return;
            }

            clearTimeout(this.debounceTimer);
            this.debounceTimer = setTimeout(async () => {
                const reqId = ++this.reqId;
                try {
                    const res = await fetch(`/api/search_symbols?q=${encodeURIComponent(val)}`);
                    if (!res.ok) return;

                    const matches = await res.json();
                    if (reqId !== this.reqId) return;
                    if (input.value.trim() !== val) return;

                    this.renderDropdown(Array.isArray(matches) ? matches : [], input);
                } catch (err) {
                    console.warn('Symbol search failed', err);
                }
            }, 120);
        });

        document.addEventListener('click', (e) => {
            const dropdown = document.getElementById('symbol-search-dropdown');
            if (dropdown && !dropdown.contains(e.target)) {
                this.hideDropdown();
            }
        });
    }
}

/**
 * Live Updater - Handles background polling and DOM Patching
 */
class LiveDashboard {
    constructor(uiManager) {
        this.uiManager = uiManager;
        this.liveIntervalId = null;
        this.fetchInFlight = false;
        this.sectionRefreshInFlight = false;
        this.autoRefreshTimer = null;
        this.setupListeners();
    }

    formatCurrency(value) {
        return Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    haveCoreCountsChanged(data) {
        const posCountEl = document.getElementById('live-position-count');
        const closedCountEl = document.getElementById('live-closed-count');
        const orderCountEl = document.getElementById('live-order-count');

        const currentPosCount = posCountEl ? parseInt(posCountEl.textContent, 10) : 0;
        const currentClosedCount = closedCountEl ? parseInt(closedCountEl.textContent, 10) : 0;
        const currentOrderCount = orderCountEl ? parseInt(orderCountEl.textContent, 10) : 0;

        return (currentPosCount !== data.position_count) || 
               (currentClosedCount !== data.closed_count) ||
               (currentOrderCount !== data.order_count);
    }

    async refreshDataStack() {
        if (this.sectionRefreshInFlight) return;
        this.sectionRefreshInFlight = true;

        try {
            const response = await fetch(window.location.href);
            const html = await response.text();
            const doc = new DOMParser().parseFromString(html, 'text/html');
            
            const newData = doc.querySelector('.data-stack');
            const oldData = document.querySelector('.data-stack');

            const activeTab = document.querySelector('.btn-tab.active');
            const viewToRestore = activeTab ? (activeTab.id === 'tab-active' ? 'active' : 'closed') : 'active';

            if (newData && oldData) {
                oldData.innerHTML = newData.innerHTML;
            }

            const newHdrCount = doc.getElementById('hdr-live-position-count');
            const oldHdrCount = document.getElementById('hdr-live-position-count');
            if (newHdrCount && oldHdrCount) oldHdrCount.textContent = newHdrCount.textContent;

            const newOrderCount = doc.getElementById('live-order-count');
            const oldOrderCount = document.getElementById('live-order-count');
            if (newOrderCount && oldOrderCount) oldOrderCount.textContent = newOrderCount.textContent;
            
            this.uiManager.togglePositions(viewToRestore);
            this.uiManager.restoreSortState();

        } catch (e) {
            console.warn('[data_stack] refresh failed:', e);
        } finally {
            this.sectionRefreshInFlight = false;
        }
    }

    async fetchLiveData() {
        if (this.fetchInFlight) return;
        this.fetchInFlight = true;

        try {
            const res = await fetch('/api/live_data');
            const data = await res.json();
            
            const fundsEl = document.getElementById('live-funds');
            if (fundsEl && data.funds !== undefined) {
                fundsEl.textContent = '₹ ' + this.formatCurrency(data.funds);
            }

            if (this.haveCoreCountsChanged(data)) {
                await this.refreshDataStack();
                return;
            }
            
            for (const [sec_id, pos] of Object.entries(data.positions)) {
                const pnlEl = document.getElementById('pnl-' + sec_id);
                if (pnlEl) {
                    const pnlVal = parseFloat(pos.pnl);
                    pnlEl.textContent = this.formatCurrency(pnlVal);
                    pnlEl.className = pnlVal > 0 ? 'pnl-positive' : (pnlVal < 0 ? 'pnl-negative' : 'pnl-neutral');
                }

                const qtyEl = document.getElementById('qty-' + sec_id);
                if (qtyEl) {
                    qtyEl.textContent = Math.abs(parseInt(pos.qty, 10) || 0);
                }

                const sideEl = document.getElementById('side-' + sec_id);
                if (sideEl) {
                    const q = parseInt(pos.qty, 10) || 0;
                    const side = q > 0 ? 'B' : (q < 0 ? 'S' : '-');
                    sideEl.textContent = side;
                    sideEl.className = 'box-side box-' + side;
                }
            }
        } catch (e) {
            console.warn('[live_data] fetch failed:', e);
        } finally {
            this.fetchInFlight = false;
        }
    }

    startLiveDataUpdater() {
        if (this.liveIntervalId) clearInterval(this.liveIntervalId);
        const selectEl = document.getElementById('live-interval-select');
        if (!selectEl) return;
        
        const ms = parseInt(selectEl.value, 10);
        if (ms > 0) {
            this.liveIntervalId = setInterval(() => this.fetchLiveData(), ms);
        }
    }

    startAutoRefresh() {
        if (!window.DhanConfig.refreshInterval) return;
        
        const refreshSecs = window.DhanConfig.refreshInterval;
        const autoRefreshTask = async () => {
            const activeTag = document.activeElement?.tagName ?? '';
            const hasOpenReentry = !!document.querySelector('details.reentry-panel[open]');
            
            if (activeTag !== 'INPUT' && activeTag !== 'SELECT' && !hasOpenReentry) {
                await this.refreshDataStack();
            }
            this.autoRefreshTimer = setTimeout(autoRefreshTask, refreshSecs * 1000);
        };
        
        this.autoRefreshTimer = setTimeout(autoRefreshTask, refreshSecs * 1000);
    }

    setupListeners() {
        const selectEl = document.getElementById('live-interval-select');
        if (selectEl) {
            selectEl.addEventListener('change', () => this.startLiveDataUpdater());
            this.startLiveDataUpdater(); 
        }
        this.startAutoRefresh();
    }
}

// Bootstrap the App
document.addEventListener('DOMContentLoaded', () => {
    window.UI = new UITableManager();
    window.Search = new SymbolSearch();
    
    // Only boot live updater if we are not in order-only view
    if (window.DhanConfig && window.DhanConfig.isDashboard) {
        window.LivePoller = new LiveDashboard(window.UI);
    }
});