/**
* UI Manager - Handles DOM interactions like Sorting and Tabbing
*/
class UITableManager {
    constructor() {
        this.currentView = 'active';
        this.totals = { active: 0, closed: 0 };
        this.currentSort = {
            active: { col: null, isNum: false, isAsc: true },
            closed: { col: null, isNum: false, isAsc: true }
        };
    }


    togglePositions(view) {
        this.currentView = view;
        const viewActive = document.getElementById("view-active");
        const viewClosed = document.getElementById("view-closed");
        const tabActive = document.getElementById("tab-active");
        const tabClosed = document.getElementById("tab-closed");

        if (viewActive && viewClosed && tabActive && tabClosed) {
            // Strip any lingering inline styles from the old logic
            viewActive.style.display = "";
            viewClosed.style.display = "";

            // Toggle the .hidden utility class instead of forcing inline styles
            if (view === "active") {
                viewActive.classList.remove("hidden");
                viewClosed.classList.add("hidden");
            } else {
                viewActive.classList.add("hidden");
                viewClosed.classList.remove("hidden");
            }

            tabActive.classList.toggle("active", view === "active");
            tabClosed.classList.toggle("active", view === "closed");
        }

        if (typeof this.updateTotalDisplay === "function") {
            this.updateTotalDisplay();
        }
    }


    updateTotalDisplay() {
        const activeSum = this.totals.active || 0;
        const closedSum = this.totals.closed || 0;

        // Update the Active Element
        const activeEl = document.getElementById('total-pnl-active');
        if (activeEl) {
            activeEl.textContent = '₹ ' + Number(activeSum).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            activeEl.className = 'pnl-display ' + (activeSum > 0 ? 'pnl-positive' : (activeSum < 0 ? 'pnl-negative' : 'pnl-neutral'));
            if (this.currentView === 'closed') activeEl.classList.add('hidden');
            else activeEl.classList.remove('hidden');
        }

        // Update the Closed Element
        const closedEl = document.getElementById('total-pnl-closed');
        if (closedEl) {
            closedEl.textContent = '₹ ' + Number(closedSum).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            closedEl.className = 'pnl-display ' + (closedSum > 0 ? 'pnl-positive' : (closedSum < 0 ? 'pnl-negative' : 'pnl-neutral'));
            if (this.currentView === 'active') closedEl.classList.add('hidden');
            else closedEl.classList.remove('hidden');
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
                let aNum = parseFloat(aText.replace(/[^0-9.-]+/g, "")) || 0;
                let bNum = parseFloat(bText.replace(/[^0-9.-]+/g, "")) || 0;
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

    closeAllDropdowns() {
        document.querySelectorAll('details.action-dropdown-panel').forEach(details => {
            details.removeAttribute('open');
        });
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

                    // --- NEW FIX: Force the form logic to recalculate instantly ---
                    form.querySelector('[name="inst_type"]').dispatchEvent(new Event('change'));
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

        // Removed autoRefreshTimer: fetchLiveData naturally handles layout refreshes now.
        this.setupListeners();
    }

    formatCurrency(value) {
        return Number(value || 0).toLocaleString("en-IN", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
    }

    haveCoreCountsChanged(data) {
        const posCountEl = document.getElementById("live-position-count");
        const closedCountEl = document.getElementById("live-closed-count");
        const orderCountEl = document.getElementById("live-order-count");

        const currentPosCount = posCountEl ? parseInt(posCountEl.textContent, 10) : 0;
        const currentClosedCount = closedCountEl ? parseInt(closedCountEl.textContent, 10) : 0;
        const currentOrderCount = orderCountEl ? parseInt(orderCountEl.textContent, 10) : 0;

        return (
            currentPosCount !== data.position_count ||
            currentClosedCount !== data.closed_count ||
            currentOrderCount !== data.order_count
        );
    }

    async refreshDataStack() {
        if (this.sectionRefreshInFlight) return;
        this.sectionRefreshInFlight = true;

        try {
            const response = await fetch(window.location.href);
            const html = await response.text();
            const doc = new DOMParser().parseFromString(html, "text/html");

            const newData = doc.querySelector(".data-stack");
            const oldData = document.querySelector(".data-stack");
            const activeTab = document.querySelector(".btn-tab.active");
            const viewToRestore = activeTab ? (activeTab.id === "tab-active" ? "active" : "closed") : "active";

            if (newData && oldData) {
                // Seamlessly swap the raw HTML
                oldData.innerHTML = newData.innerHTML;
                // Re-bind logic specifically to the newly swapped-in dropdown forms
                document.querySelectorAll('.data-stack .order-form').forEach(f => new OrderFormLogic(f));
            }

            const newHdrCount = doc.getElementById("hdr-live-position-count");
            const oldHdrCount = document.getElementById("hdr-live-position-count");
            if (newHdrCount && oldHdrCount) oldHdrCount.textContent = newHdrCount.textContent;

            const newOrderCount = doc.getElementById("live-order-count");
            const oldOrderCount = document.getElementById("live-order-count");
            if (newOrderCount && oldOrderCount) oldOrderCount.textContent = newOrderCount.textContent;

            // Instantly restore layout and calculate sums
            this.uiManager.togglePositions(viewToRestore);
            this.uiManager.restoreSortState();
            this.uiManager.updateTotalDisplay();

        } catch (e) {
            console.warn("[data_stack] refresh failed:", e);
        } finally {
            this.sectionRefreshInFlight = false;
        }
    }

    async fetchLiveData() {
        if (this.fetchInFlight) return;
        this.fetchInFlight = true;

        try {
            const res = await fetch("/api/live_data");
            const data = await res.json();

            const fundsEl = document.getElementById("live-funds");
            if (fundsEl && data.funds !== undefined) {
                fundsEl.textContent = "₹ " + this.formatCurrency(data.funds);
            }


            // Apply exact backend totals
            if (data.active_pnl_total !== undefined) {
                this.uiManager.totals.active = data.active_pnl_total;
                this.uiManager.totals.closed = data.closed_pnl_total;
            }

            if (this.haveCoreCountsChanged(data)) {
                await this.refreshDataStack();
                this.uiManager.updateTotalDisplay();
                return;
            }

            // Loop and patch the individual rows
            for (const [key, pos] of Object.entries(data.positions)) {
                const sec_id = pos.sec_id;

                if (pos.is_active) {
                    const pnlEl = document.getElementById("pnl-active-" + sec_id);
                    if (pnlEl) {
                        const pnlVal = parseFloat(pos.pnl);
                        pnlEl.textContent = this.formatCurrency(pnlVal);
                        pnlEl.className = pnlVal > 0 ? "pnl-positive" : (pnlVal < 0 ? "pnl-negative" : "pnl-neutral");
                    }

                    const qtyEl = document.getElementById("qty-" + sec_id);
                    if (qtyEl) {
                        qtyEl.textContent = Math.abs(parseInt(pos.qty, 10) || 0);
                    }

                    const sideEl = document.getElementById("side-" + sec_id);
                    if (sideEl) {
                        const q = parseInt(pos.qty, 10) || 0;
                        const side = q > 0 ? "B" : q < 0 ? "S" : "-";
                        sideEl.textContent = side;
                        sideEl.className = "box-side box-" + side;
                    }

                    const entryEl = document.getElementById("entry-" + sec_id);
                    if (entryEl && pos.entry_price !== undefined) {
                        entryEl.textContent = this.formatCurrency(pos.entry_price);
                    }

                    const ltpEl = document.getElementById("ltp-" + sec_id);
                    if (ltpEl && pos.ltp !== undefined) {
                        ltpEl.textContent = this.formatCurrency(pos.ltp);
                    }
                } else {
                    const pnlEl = document.getElementById("pnl-closed-" + sec_id);
                    if (pnlEl) {
                        const pnlVal = parseFloat(pos.pnl);
                        pnlEl.textContent = this.formatCurrency(pnlVal);
                        pnlEl.className = pnlVal > 0 ? "pnl-positive" : (pnlVal < 0 ? "pnl-negative" : "pnl-neutral");
                    }

                    const buyQtyEl = document.getElementById("buyqty-" + sec_id);
                    if (buyQtyEl && pos.buyqty !== undefined) {
                        buyQtyEl.textContent = parseInt(pos.buyqty, 10) || 0;
                    }

                    const buyAvgEl = document.getElementById("buyavg-" + sec_id);
                    if (buyAvgEl && pos.buy_avg !== undefined) {
                        buyAvgEl.textContent = this.formatCurrency(pos.buy_avg);
                    }

                    const sellAvgEl = document.getElementById("sellavg-" + sec_id);
                    if (sellAvgEl && pos.sell_avg !== undefined) {
                        sellAvgEl.textContent = this.formatCurrency(pos.sell_avg);
                    }
                }
            }

            // Fire the sum calculation using the backed-in totals
            this.uiManager.updateTotalDisplay();

        } catch (e) {
            console.warn("[live_data] fetch failed:", e);
        } finally {
            this.fetchInFlight = false;
        }
    }

    startLiveDataUpdater() {
        if (this.liveIntervalId) clearInterval(this.liveIntervalId);
        const selectEl = document.getElementById("live-interval-select");
        if (!selectEl) return;

        const ms = parseInt(selectEl.value, 10);
        if (ms > 0) {
            this.liveIntervalId = setInterval(() => this.fetchLiveData(), ms);
        }
    }

    setupListeners() {
        const selectEl = document.getElementById("live-interval-select");
        if (selectEl) {
            selectEl.addEventListener("change", () => this.startLiveDataUpdater());
            this.startLiveDataUpdater();
        }
        // Completely disabled the rigid auto-refresh interval
    }
}

// Bootstrap the App
document.addEventListener('DOMContentLoaded', () => {
    window.UI = new UITableManager();
    
    // Initialize logic for the main Left Panel AND all inline dropdown forms
    document.querySelectorAll('.order-form').forEach(f => new OrderFormLogic(f));
    
    if (window.DhanConfig) {
        window.UI.totals.active = window.DhanConfig.initActivePnl || 0;
        window.UI.totals.closed = window.DhanConfig.initClosedPnl || 0;
        window.UI.updateTotalDisplay();
    }
    
    window.Search = new SymbolSearch();
    if (window.DhanConfig && window.DhanConfig.isDashboard) {
        window.LivePoller = new LiveDashboard(window.UI);
    }
});
// Global Function for Auth Modal Tab Switching
window.switchAuthTab = function (tab) {
    const tabs = ['generate', 'renew', 'update'];
    tabs.forEach(t => {
        const form = document.getElementById('form-' + t);
        const btn = document.getElementById('btn-tab-' + t);
        if (t === tab) {
            form.classList.remove('hidden');
            btn.classList.add('tab-btn-active');
            btn.classList.remove('tab-btn-inactive');
        } else {
            form.classList.add('hidden');
            btn.classList.remove('tab-btn-active');
            btn.classList.add('tab-btn-inactive');
        }
    });
};





/**
 * Dynamic Order Form - Enables/Disables fields based on conditions
 */
class OrderFormLogic {
    constructor(formEl) {
        this.form = formEl;
        if (!this.form) return;

        this.instType = this.form.querySelector('[name="inst_type"]');

        // Support both main form names and re-entry form names
        this.orderMode = this.form.querySelector('[name="order_mode"]') || this.form.querySelector('[name="reentry_type"]');
        this.alertTarget = this.form.querySelector('[name="alert_trigger_base"]') || this.form.querySelector('[name="reentry_alert_base"]');
        this.strike = this.form.querySelector('[name="strike"]');
        this.optType = this.form.querySelector('[name="opt_type"]');
        this.expiry = this.form.querySelector('[name="expiry"]');
        this.triggerPrice = this.form.querySelector('[name="price"]') || this.form.querySelector('[name="reentry_price"]');
        this.limitPrice = this.form.querySelector('[name="limit_price"]') || this.form.querySelector('[name="reentry_limit_price"]');
        this.stopLoss = this.form.querySelector('[name="stop_loss"]') || this.form.querySelector('[name="reentry_stop_loss"]');
        this.targetPrice = this.form.querySelector('[name="target_price"]') || this.form.querySelector('[name="reentry_target_price"]');

        this.slPerc = 0.7;
        this.targetPerc = this.slPerc * 2;
        this.optBump = 10;

        // Ensure critical elements exist before attaching listeners
        if (this.instType && this.orderMode) {
            this.setupListeners();
            this.updateState(); // Boot initial state
        }
    }

    setupListeners() {
        this.instType.addEventListener('change', () => this.updateState());
        this.orderMode.addEventListener('change', () => this.updateState());
        if (this.limitPrice) {
            this.limitPrice.addEventListener('input', () => this.autoPopulateSuper());
        }
    }

    updateState() {
        const inst = this.instType.value;
        const mode = this.orderMode.value;

        // Reset all to enabled
        const fields = [this.strike, this.optType, this.expiry, this.alertTarget, this.stopLoss, this.targetPrice, this.triggerPrice, this.limitPrice];
        fields.forEach(f => { if (f) f.disabled = false; });

        // Constraint 1, 4, 5: Instrument rules
        if (inst === 'EQ') {
            if (this.strike) this.strike.disabled = true;
            if (this.optType) this.optType.disabled = true;
            if (this.expiry) this.expiry.disabled = true;
        } else if (inst === 'FUT') {
            if (this.strike) this.strike.disabled = true;
            if (this.optType) this.optType.disabled = true;
        } else if (inst === '') {
            if (this.strike) this.strike.disabled = true;
            if (this.optType) this.optType.disabled = true;
            if (this.expiry) this.expiry.disabled = true;
            if (this.limitPrice) this.limitPrice.disabled = true;
            if (this.stopLoss) this.stopLoss.disabled = true;
            if (this.targetPrice) this.targetPrice.disabled = true;
            if (this.alertTarget) this.alertTarget.disabled = true;
        }

        // Constraint 2: Super Order Mode
        if (mode === 'SUPER') {
            if (this.triggerPrice) this.triggerPrice.disabled = true;
        } else {
            if (this.stopLoss) this.stopLoss.disabled = true;
            if (this.targetPrice) this.targetPrice.disabled = true;
        }

        if (inst === '') {
            if (this.triggerPrice) this.triggerPrice.disabled = false;
            if (this.stopLoss) this.stopLoss.disabled = true;
            if (this.targetPrice) this.targetPrice.disabled = true;
        }

        // Constraint 3: Alert Target
        if (mode !== 'ALERT') {
            if (this.alertTarget) this.alertTarget.disabled = true;
        }

        this.autoPopulateSuper();
    }

    autoPopulateSuper() {
        const mode = this.orderMode.value;
        const inst = this.instType.value;

        if (mode !== 'SUPER' || inst === '') return;
        if (!this.limitPrice || !this.targetPrice || !this.stopLoss) return;

        const limitVal = parseFloat(this.limitPrice.value) || 0;
        if (limitVal <= 0) {
            this.targetPrice.value = "0.0";
            this.stopLoss.value = "0.0";
            return;
        }

        const isOpt = (inst === 'OPT');
        const bump = isOpt ? this.optBump : 1;

        const finalTargetPerc = this.targetPerc * bump;
        const finalSlPerc = this.slPerc * bump;

        const target = Math.ceil(limitVal * (1 + finalTargetPerc / 100));
        const stopLoss = Math.floor(limitVal * (1 - finalSlPerc / 100));

        this.targetPrice.value = target.toFixed(2);
        this.stopLoss.value = stopLoss.toFixed(2);
    }
}


/**
 * Global Dropdown Management (Mutual Exclusion & Hotkeys)
 */
document.addEventListener('click', (e) => {
    const summary = e.target.closest('summary');
    if (summary) {
        const parentDetails = summary.parentElement;
        // If a user clicks a closed dropdown to open it, close all others first
        if (parentDetails && parentDetails.tagName === 'DETAILS' && !parentDetails.hasAttribute('open')) {
            if (window.UI && typeof window.UI.closeAllDropdowns === 'function') {
                window.UI.closeAllDropdowns();
            }
        }
    }
});

// Allow the Escape key to instantly clear any open form menus
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        if (window.UI && typeof window.UI.closeAllDropdowns === 'function') {
            window.UI.closeAllDropdowns();
        } else {
            // Fallback in case UI hasn't initialized
            document.querySelectorAll('details.action-dropdown-panel').forEach(d => d.removeAttribute('open'));
        }
    }
});