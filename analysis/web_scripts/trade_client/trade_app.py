"""
trade_app.py — Dhan Trading Portal (FastAPI)
"""

import asyncio
import tomllib
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates

from tradeapi.dhan_trade import DhanTrader, UIOverride
from utils.data.paths import TEMPLATES_ROOT_DIR

_APP_CONFIG_PATH = Path(__file__).parent / 'trade_app.toml'


def _format_order_detail(
    qty: int | None,
    price: float | None,
    trig: float | None,
) -> str:
    """Helper formatting string details for pending orders based on type."""
    parts: list[str] = []
    if qty not in (None, "", 0):
        parts.append(f"Qty: {qty}")
    try:
        if trig and float(trig) > 0:
            parts.append(f"Trig: {trig}")
    except (TypeError, ValueError):
        pass
    try:
        if price and float(price) > 0:
            parts.append(f"Lmt: {price}")
    except (TypeError, ValueError):
        pass
    return " | ".join(parts)


class AppConfig:
    def __init__(self, path: Path):
        self.path = path
        self.raw_cfg = self._load()

        srv = self.raw_cfg.get('server', {})
        self.host: str      = srv.get('host', '127.0.0.1')
        self.port: int      = srv.get('port', 8000)
        self.reload: bool   = srv.get('reload', False)
        self.log_level: str = srv.get('log_level', 'info')

        app_cfg = self.raw_cfg.get('app', {})
        self.title: str                  = app_cfg.get('title', 'Dhan Trading Portal')
        self.template_subdir: str        = app_cfg.get('template_subdir', 'template_trade_client')
        self.refresh_interval: int       = app_cfg.get('refresh_interval', 15)
        self.refresh_master_script: bool = app_cfg.get('refresh_master_script', False)
        self.reset_proxy_at_start: bool  = app_cfg.get('reset_proxy_at_start', False)

        cls_cfg = self.raw_cfg.get('close', {})
        self.reentry_order_mode: str           = cls_cfg.get('reentry_order_mode', 'FOREVER')
        self.reentry_product_type: str         = cls_cfg.get('reentry_product_type', 'CNC')
        self.clean_orphaned_super_orders: bool = cls_cfg.get('clean_orphaned_super_orders', False)

        ord_cfg = self.raw_cfg.get('orders', {})
        self.pending_statuses: tuple[str, ...] = tuple(
            ord_cfg.get('pending_statuses', ['TRANSIT', 'PENDING', 'PART_TRADED'])
        )
        self.forever_active_statuses: tuple[str, ...] = tuple(
            ord_cfg.get('forever_active_statuses', ['PENDING', 'CONFIRM'])
        )
        self.alert_active_statuses: tuple[str, ...] = tuple(
            ord_cfg.get('alert_active_statuses', ['ACTIVE'])
        )

    def _load(self) -> dict:
        try:
            with open(self.path, 'rb') as f:
                return tomllib.load(f)
        except Exception as exc:
            print(f'[!] Could not load config from {self.path}: {exc}. Using defaults.')
            return {}


class TradePortalApp:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.app = FastAPI(title=self.cfg.title)

        template_dir = Path(TEMPLATES_ROOT_DIR) / self.cfg.template_subdir
        self.templates = Jinja2Templates(directory=template_dir)

        print("[*] Initializing DhanTrader...")
        self.trader = DhanTrader(
            refresh_master_scrip=self.cfg.refresh_master_script,
            restart_proxy=self.cfg.reset_proxy_at_start
        )
        self.trader.begin_session()

        self._setup_routes()

    def _setup_routes(self):

        @self.app.get("/", response_class=HTMLResponse)
        async def dashboard(request: Request, view: Optional[str] = None):
            positions = self.trader.get_active_positions()
            funds = self.trader.get_funds()
            active_orders = []

            for o in self.trader.get_pending_orders(self.cfg.pending_statuses):
                active_orders.append({
                    "symbol":   o["symbol"],
                    "order_id": o["order_id"],
                    "leg":      "",
                    "type":     o["type"],
                    "side":     o.get("transaction_type", ""),
                    "detail":   _format_order_detail(
                        o.get("qty"), o.get("price"), o.get("trigger_price")
                    )
                })

            for symbol, oid, leg, txn, qty, price, trig in self.trader.get_active_super_orders():
                dtl = _format_order_detail(qty, price, trig)
                if leg and leg != "ENTRY_LEG":
                    dtl += f" ({leg})"
                active_orders.append({
                    "symbol":   symbol,
                    "order_id": oid,
                    "leg":      leg,
                    "type":     "SUPER",
                    "side":     txn,
                    "detail":   dtl
                })

            for o in self.trader.get_forever_orders(self.cfg.forever_active_statuses):
                active_orders.append({
                    "symbol":   o["symbol"],
                    "order_id": o["order_id"],
                    "leg":      o["leg"],
                    "type":     "FOREVER",
                    "side":     o.get("transaction_type", ""),
                    "detail":   _format_order_detail(
                        o.get("qty"), o.get("price"), o.get("trigger_price")
                    )
                })

            for o in self.trader.get_all_alerts(self.cfg.alert_active_statuses):
                active_orders.append({
                    "symbol":   o["symbol"],
                    "order_id": o["order_id"],
                    "leg":      "",
                    "type":     "ALERT",
                    "side":     o.get("transaction_type", ""),
                    "detail":   _format_order_detail(
                        o.get("qty"), o.get("price"), o.get("comparing_value")
                    )
                })

            return self.templates.TemplateResponse("dashboard.html", {
                "request":          request,
                "positions":        positions,
                "total_positions":  len(positions),
                "active_orders":    active_orders,
                "total_orders":     len(active_orders),
                "funds":            funds,
                "refresh_interval": self.cfg.refresh_interval,
                "view":             view
            })


        @self.app.get("/api/search_symbols")
        async def search_symbols(q: str = Query("")):

            q = (q or "").strip()

            if len(q) < 2:
                return JSONResponse([])

            try:

                matches = self.trader.scrip.search_symbols(q, limit=30)

                # remove internal fields from UI payload
                clean = []

                for m in matches:
                    strike_val = m.get("strike", 0)

                    try:
                        strike_val = float(strike_val)
                    except Exception:
                        strike_val = 0.0

                    clean.append({
                        "display":   str(m.get("display", "")),
                        "symbol":    str(m.get("symbol", "")),
                        "inst_type": str(m.get("inst_type", "")),
                        "strike":    strike_val,
                        "opt_type":  str(m.get("opt_type", "")),
                        "expiry":    str(m.get("expiry", "")),
                        "exch":      str(m.get("exch", ""))
                    })

                return JSONResponse(
                    content=jsonable_encoder(clean)
                )

            except Exception as exc:

                print(f"[search_symbols API ERROR] q={q} err={exc}")

                return JSONResponse([])

        @self.app.get("/api/live_data")
        async def live_data():
            positions = self.trader.get_active_positions()
            funds = self.trader.get_funds()

            pnl_data = {}
            for p in positions:
                pnl_data[p['security_id']] = {
                    "pnl": p['pnl'],
                    "qty": p['qty']
                }
            return JSONResponse({"funds": funds, "positions": pnl_data})

        @self.app.post("/place_order")
        async def place_order(
            symbol: str = Form(...),
            exchange: str = Form("NSE"),
            signal: str = Form(...),
            qty: int = Form(1),
            price: float = Form(0.0),
            limit_price: float = Form(0.0),
            order_mode: str = Form("MARKET"),
            inst_type: str = Form(""),
            alert_trigger_base: str = Form("PARENT"),
            strike: float = Form(0.0),
            expiry: str = Form(""),
            opt_type: str = Form(""),
            view: Optional[str] = Query(None)
        ):
            overrides = UIOverride(
                inst_type=inst_type,
                strike=strike,
                expiry=expiry,
                trigger_price=price,
                force_qty=True,
                opt_type=opt_type,
                limit_price=limit_price,
            )

            inst = self.trader.resolve_instrument(
                symbol, exchange, signal, qty, price, overrides=overrides
            )

            if inst:
                sec_id, lot_size = self.trader.scrip.lookup_with_fallback(inst)
                if sec_id:
                    match order_mode:
                        case "MARKET":
                            self.trader.place_simple_order(sec_id, lot_size, inst)
                        case "SUPER":
                            self.trader.place_super_order(sec_id, lot_size, inst)
                        case "FOREVER":
                            self.trader.place_trigger_forever_order(sec_id, lot_size, inst)
                        case "ALERT":
                            fno_sig = signal if alert_trigger_base == "PARENT" else None
                            self.trader.place_trigger_alert_order(
                                sec_id, lot_size, inst, fno_signal=fno_sig
                            )
            else:
                print(f'Could not resolve {exchange} {symbol}')

            redirect_url = "/?view=order" if view == "order" else "/"
            return RedirectResponse(url=redirect_url, status_code=303)

        @self.app.post("/close_reentry")
        async def close_reentry(
            symbol: str = Form(...),
            exchange: str = Form("NSE"),
            sec_id: str = Form(...),
            exchange_seg: str = Form(...),
            net_qty: int = Form(...),
            qty: int = Form(1),
            reentry_price: float = Form(0.0),
            reentry_limit_price: float = Form(0.0),
            reentry_side: str = Form("BUY"),
            inst_type: str = Form(""),
            reentry_type: str = Form(""),
            reentry_alert_base: str = Form("PARENT"),
            strike: float = Form(0.0),
            expiry: str = Form(""),
            opt_type: str = Form("")
        ):
            self.trader.close_position_by_secid(sec_id, exchange_seg, net_qty)

            if self.cfg.clean_orphaned_super_orders:
                await asyncio.sleep(1)
                self.trader.clean_orphaned_orders()

            if reentry_price > 0 or reentry_limit_price > 0:
                overrides = UIOverride(
                    inst_type=inst_type,
                    strike=strike,
                    expiry=expiry,
                    trigger_price=reentry_price,
                    force_qty=True,
                    opt_type=opt_type,
                    limit_price=reentry_limit_price
                )

                inst_reentry = self.trader.resolve_instrument(
                    symbol, exchange, reentry_side, qty, reentry_price, overrides=overrides
                )

                if inst_reentry:
                    new_sec_id, lot_size = self.trader.scrip.lookup_with_fallback(inst_reentry)
                    if new_sec_id:
                        mode_to_use = reentry_type if reentry_type else self.cfg.reentry_order_mode
                        match mode_to_use:
                            case "FOREVER":
                                self.trader.place_trigger_forever_order(
                                    new_sec_id, lot_size, inst_reentry
                                )
                            case "SUPER":
                                self.trader.place_super_order(
                                    new_sec_id, lot_size, inst_reentry
                                )
                            case "ALERT":
                                fno_sig = reentry_side if reentry_alert_base == "PARENT" else None
                                self.trader.place_trigger_alert_order(
                                    new_sec_id, lot_size, inst_reentry, fno_signal=fno_sig
                                )
                            case _:
                                self.trader.place_simple_order(
                                    new_sec_id, lot_size, inst_reentry
                                )

            return RedirectResponse(url="/", status_code=303)

        @self.app.post("/close_position")
        async def close_position(
            sec_id: str = Form(...),
            exchange_seg: str = Form(...),
            net_qty: int = Form(...)
        ):
            self.trader.close_position_by_secid(sec_id, exchange_seg, net_qty)
            if self.cfg.clean_orphaned_super_orders:
                await asyncio.sleep(1)
                self.trader.clean_orphaned_orders()

            return RedirectResponse(url="/", status_code=303)

        @self.app.post("/cancel_order")
        async def cancel_order(
            order_id: str = Form(...),
            order_type: str = Form(...),
            leg: str = Form("ENTRY_LEG")
        ):
            match order_type:
                case "SUPER":
                    self.trader.cancel_super_order(order_id, leg)
                case "FOREVER":
                    self.trader.cancel_forever_order(order_id)
                case "ALERT":
                    self.trader.cancel_alert_order(order_id)
                case _:
                    self.trader.cancel_normal_order(order_id)

            return RedirectResponse(url="/", status_code=303)

        @self.app.post("/clean_orphaned")
        async def clean_orphaned():
            self.trader.clean_orphaned_orders()
            return RedirectResponse(url="/", status_code=303)

        @self.app.post("/cancel_all")
        async def cancel_all():
            for o in self.trader.get_pending_orders(self.cfg.pending_statuses):
                self.trader.cancel_normal_order(o["order_id"])

            for _, oid, leg, *_ in self.trader.get_active_super_orders():
                self.trader.cancel_super_order(oid, leg)

            for o in self.trader.get_forever_orders(self.cfg.forever_active_statuses):
                self.trader.cancel_forever_order(o["order_id"])

            for o in self.trader.get_all_alerts(self.cfg.alert_active_statuses):
                self.trader.cancel_alert_order(o["order_id"])

            return RedirectResponse(url="/", status_code=303)

    def run(self):
        uvicorn.run(
            self.app,
            host=self.cfg.host,
            port=self.cfg.port,
            reload=self.cfg.reload,
            log_level=self.cfg.log_level
        )


if __name__ == "__main__":
    config = AppConfig(_APP_CONFIG_PATH)
    portal = TradePortalApp(config)
    portal.run()