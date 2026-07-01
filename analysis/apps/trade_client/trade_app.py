"""FastAPI dashboard for the Dhan trading portal."""

import hmac
import threading
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from tradeapi.dhan_trade import DhanTrader, UIOverride
from utils.fastapi.fastapi_base import AppPaths, BaseAppConfig, BaseFastAPIApp
from utils.logging.log_utils import LOGGER, bool_env_or_cfg
from utils.time.time_utils import wait_next_wall_clock

paths = AppPaths.resolve(__file__)

_MIN_QUERY_LEN = 2

_app_root_path = "/trade_portal"
_app_cur_redirect_url = "./"


# ==========================================
# Pydantic Response Schemas
# ==========================================
class SymbolSearchItem(BaseModel):
    display: str
    symbol: str
    inst_type: str
    strike: float
    opt_type: str
    expiry: str
    exch: str


class TradeAlert(BaseModel):
    secret: str
    symbol: str
    signal: str
    price: float
    exch: str


class PositionData(BaseModel):
    pnl: float
    qty: int
    buyqty: int
    sellqty: int
    entry_price: float = 0.0
    ltp: float = 0.0
    buy_avg: float = 0.0
    sell_avg: float = 0.0
    display_name: str
    exchange_seg: str
    product_type: str = "INTRADAY"
    ui_inst_type: str = ""
    is_active: bool = True
    sec_id: str = ""


class LiveDataResponse(BaseModel):
    funds: float
    position_count: int
    closed_count: int
    order_count: int
    active_pnl_total: float
    closed_pnl_total: float
    positions: dict[str, PositionData] | dict[str, dict[str, Any]]


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


class AppConfig(BaseAppConfig):
    """Runtime settings loaded from ``trade_app.toml``."""

    def __init__(self, path: Path):
        super().__init__(path)

        self.webhook_secret: str = self.raw_cfg.get("server", {}).get(
            "webhook_secret", ""
        )

        app_cfg = self.raw_cfg.get("app", {})
        self.title: str = app_cfg.get("title", "Dhan Trading Portal")
        self.template_subdir: str = app_cfg.get(
            "template_subdir", "template_trade_client"
        )

        self.refresh_master_script: bool = bool_env_or_cfg(
            "refresh_master_script", app_cfg, default_val=False
        )
        self.reset_proxy_at_start: bool = bool_env_or_cfg(
            "reset_proxy_at_start", app_cfg, default_val=False
        )
        self.apply_proxy_flag: bool = bool_env_or_cfg(
            "apply_proxy_flag", app_cfg, default_val=False
        )
        self.reload_interval: int = app_cfg.get("reload_interval", 3)
        self.buffer_seconds: int = app_cfg.get("buffer_seconds", 0)

        cls_cfg = self.raw_cfg.get("close", {})
        self.reentry_order_mode: str = cls_cfg.get("reentry_order_mode", "FOREVER")
        self.reentry_product_type: str = cls_cfg.get("reentry_product_type", "CNC")
        self.clean_orphaned_super_orders: bool = cls_cfg.get(
            "clean_orphaned_super_orders", False
        )


@dataclass(frozen=True)
class DashboardSnapshot:
    """Data needed by the dashboard and live-data endpoint."""

    positions: list[dict]
    closed_positions: list[dict]
    funds: float
    active_orders: list[dict]

    @property
    def total_positions(self) -> int:
        return len(self.positions)

    @property
    def total_closed(self) -> int:
        return len(self.closed_positions)

    @property
    def total_orders(self) -> int:
        return len(self.active_orders)

    @property
    def active_pnl_total(self) -> float:
        return sum(p.get("pnl", 0.0) for p in self.positions)

    @property
    def closed_pnl_total(self) -> float:
        return sum(p.get("pnl", 0.0) for p in self.closed_positions)

    def live_payload(self) -> LiveDataResponse:
        positions = {}
        for position in self.positions:
            security_id = str(position.get("security_id", ""))
            if not security_id:
                continue
            positions[f"active_{security_id}"] = {
                "pnl": position.get("pnl", 0.0),
                "qty": position.get("qty", 0),
                "display_name": position.get("display_name", ""),
                "exchange_seg": position.get("exchange_seg", ""),
                "buyqty": position.get("buyqty", 0),
                "sellqty": position.get("sellqty", 0),
                "entry_price": position.get("entry_price", 0.0),
                "ltp": position.get("ltp", 0.0),
                "buy_avg": position.get("buy_avg", 0.0),
                "sell_avg": position.get("sell_avg", 0.0),
                "is_active": True,  # <-- ADDED
                "sec_id": security_id,  # <-- ADDED
            }

        for position in self.closed_positions:
            security_id = str(position.get("security_id", ""))
            if not security_id:
                continue
            positions[f"closed_{security_id}"] = {
                "pnl": position.get("pnl", 0.0),
                "qty": position.get("qty", 0),
                "display_name": position.get("display_name", ""),
                "exchange_seg": position.get("exchange_seg", ""),
                "buyqty": position.get("buyqty", 0),
                "sellqty": position.get("sellqty", 0),
                "buy_avg": position.get("buy_avg", 0.0),
                "sell_avg": position.get("sell_avg", 0.0),
                "is_active": False,  # <-- ADDED
                "sec_id": security_id,  # <-- ADDED
            }

        return LiveDataResponse(
            funds=self.funds,
            position_count=self.total_positions,
            closed_count=self.total_closed,
            order_count=self.total_orders,
            active_pnl_total=self.active_pnl_total,
            closed_pnl_total=self.closed_pnl_total,
            positions=positions,
        )


class DashboardService:
    """Builds normalized dashboard data from the trading API client."""

    def __init__(self, trader: DhanTrader, config: AppConfig):
        self.trader = trader
        self.config = config

    def get_snapshot(self) -> DashboardSnapshot:
        active_pos, closed_pos = self.trader.get_positions()
        funds = self.trader.get_funds()
        active_orders = self._get_active_orders()
        return DashboardSnapshot(active_pos, closed_pos, funds, active_orders)

    def _get_active_orders(self) -> list[dict]:

        active_orders: list[dict] = []
        active_orders.extend(self._get_pending_orders())
        active_orders.extend(self._get_super_orders())
        active_orders.extend(self._get_forever_orders())
        active_orders.extend(self._get_alerts())
        return active_orders

    def _get_pending_orders(self) -> list[dict]:
        return [
            {
                "symbol": order["symbol"],
                "order_id": order["order_id"],
                "leg": "",
                "type": order["type"],
                "side": order.get("transaction_type", ""),
                "detail": _format_order_detail(
                    order.get("qty"),
                    order.get("price"),
                    order.get("trigger_price"),
                ),
            }
            for order in self.trader.get_pending_orders()
        ]

    def _get_super_orders(self) -> list[dict]:
        orders = []
        for (
            symbol,
            order_id,
            leg,
            txn,
            qty,
            price,
            trig,
        ) in self.trader.get_active_super_orders():
            detail = _format_order_detail(qty, price, trig)
            if leg and leg != "ENTRY_LEG":
                detail += f" ({leg})"
            orders.append(
                {
                    "symbol": symbol,
                    "order_id": order_id,
                    "leg": leg,
                    "type": "SUPER",
                    "side": txn,
                    "detail": detail,
                },
            )
        return orders

    def _get_forever_orders(self) -> list[dict]:
        return [
            {
                "symbol": order["symbol"],
                "order_id": order["order_id"],
                "leg": order["leg"],
                "type": "FOREVER",
                "side": order.get("transaction_type", ""),
                "detail": _format_order_detail(
                    order.get("qty"),
                    order.get("price"),
                    order.get("trigger_price"),
                ),
            }
            for order in self.trader.get_forever_orders()
        ]

    def _get_alerts(self) -> list[dict]:
        return [
            {
                "symbol": order["symbol"],
                "order_id": order["order_id"],
                "leg": "",
                "type": "ALERT",
                "side": order.get("transaction_type", ""),
                "detail": _format_order_detail(
                    order.get("qty"),
                    order.get("price"),
                    order.get("comparing_value"),
                ),
            }
            for order in self.trader.get_all_alerts()
        ]


class BackgroundCleaner:
    """Periodically cleans orphaned super orders in the background."""

    def __init__(self, trader: DhanTrader, config: AppConfig) -> None:
        self.trader = trader
        self.config = config

    def start(self) -> None:
        if self.config.clean_orphaned_super_orders and self.config.reload_interval:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        while True:
            wait_next_wall_clock(
                self.config.reload_interval, self.config.buffer_seconds or 0
            )
            try:
                self.trader.clean_orphaned_orders()
                LOGGER.info(
                    "Background job: clean_orphaned_orders executed successfully."
                )
            except RuntimeError:
                LOGGER.error(
                    "Background cleaner failed with a RuntimeError:\n%s",
                    traceback.format_exc(),
                )


class TradePortalApp(BaseFastAPIApp):
    """FastAPI application wrapper for the trading portal."""

    def __init__(self, config: AppConfig):
        super().__init__(
            title=config.title,
            config=config,
            template_dir=paths.templates,
            root_path=_app_root_path,
        )

        self.cfg: AppConfig = config

        LOGGER.info("Initializing DhanTrader...")
        self.trader = DhanTrader(
            refresh_master_scrip=self.cfg.refresh_master_script,
            restart_proxy=self.cfg.reset_proxy_at_start,
            apply_proxy_flag=self.cfg.apply_proxy_flag,
        )
        self.trader.begin_session()
        self.dashboard = DashboardService(self.trader, self.cfg)

        self.cleaner = BackgroundCleaner(self.trader, self.cfg)
        self.cleaner.start()

        self._setup_routes()

    def _setup_routes(self) -> None:
        router = APIRouter()
        router.add_api_route(
            "/",
            self._dashboard,
            methods=["GET"],
            response_class=HTMLResponse,
        )
        router.add_api_route(
            "/api/search_symbols",
            self._search_symbols,
            methods=["GET"],
            response_model=list[SymbolSearchItem],
        )
        router.add_api_route(
            "/api/live_data",
            self._live_data,
            methods=["GET"],
            response_model=LiveDataResponse,
        )
        router.add_api_route(
            "/place_order",
            self._place_order,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/close_reentry",
            self._close_reentry,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/close_position",
            self._close_position,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/cancel_order",
            self._cancel_order,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/clean_orphaned",
            self._clean_orphaned,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/cancel_all",
            self._cancel_all,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/generate_token",
            self._generate_token,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/renew_token",
            self._renew_token,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/update_token",
            self._update_token,
            methods=["POST"],
            response_class=RedirectResponse,
        )
        router.add_api_route(
            "/webhook/",
            self._receive_webhook,
            methods=["POST"],
        )

        self.app.include_router(router)

    async def _dashboard(
        self,
        request: Request,
        view: str | None = None,
    ) -> HTMLResponse:
        snapshot = self.dashboard.get_snapshot()
        return self.templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "positions": snapshot.positions,
                "closed_positions": snapshot.closed_positions,
                "total_positions": snapshot.total_positions,
                "total_closed": snapshot.total_closed,
                "active_orders": snapshot.active_orders,
                "total_orders": snapshot.total_orders,
                "active_pnl_total": snapshot.active_pnl_total,
                "closed_pnl_total": snapshot.closed_pnl_total,
                "funds": snapshot.funds,
                "view": view,
                "client_id": self.trader.client_id,
                "client_name": self.trader.client_name,
                "expiry_time": self.trader.expiry_time,
            },
        )

    async def _update_token(
        self,
        client_id: str = Form(...),
        access_token: str = Form(...),
    ) -> RedirectResponse:
        # We reuse the existing client_name, but mark expiry as "Manual Update"
        self.trader.update_credentials(
            client_id, access_token, self.trader.client_name, "Manual Update"
        )
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _generate_token(
        self,
        client_id: str = Form(...),
        pin: str = Form(...),
        totp: str = Form(...),
    ) -> RedirectResponse:
        self.trader.generate_token(client_id, pin, totp)
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _renew_token(self) -> RedirectResponse:
        self.trader.renew_token()
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _search_symbols(self, q: str = Query("")) -> list[SymbolSearchItem]:
        q = (q or "").strip()
        if len(q) < _MIN_QUERY_LEN:
            return []
        try:
            matches = self.trader.search_symbols(q, limit=30)
            clean: list[SymbolSearchItem] = []

            for match in matches:
                strike_val = match.get("strike", 0)
                try:
                    strike_val = float(strike_val)
                except (ValueError, TypeError):
                    strike_val = 0.0

                item = SymbolSearchItem(
                    display=str(match.get("display", "")),
                    symbol=str(match.get("symbol", "")),
                    inst_type=str(match.get("inst_type", "")),
                    strike=strike_val,
                    opt_type=str(match.get("opt_type", "")),
                    expiry=str(match.get("expiry", "")),
                    exch=str(match.get("exch", "")),
                )
                clean.append(item)

        except (ValueError, TypeError, KeyError):
            LOGGER.exception("[search_symbols API ERROR] q=%s", q)
            return []
        else:
            return clean

    async def _live_data(self) -> LiveDataResponse:
        return self.dashboard.get_snapshot().live_payload()

    async def _place_order(
        self,
        symbol: str = Form(...),
        exchange: str = Form("NSE"),
        signal: str = Form(...),
        qty: int = Form(1),
        price: float = Form(0.0),
        limit_price: float = Form(0.0),
        stop_loss: float = Form(0.0),
        target_price: float = Form(0.0),
        order_mode: str = Form("MARKET"),
        product_type: str = Form("INTRADAY"),
        inst_type: str = Form(""),
        alert_trigger_base: str = Form("PARENT"),
        strike: float = Form(0.0),
        expiry: str = Form(""),
        opt_type: str = Form(""),
        view: str | None = Query(None),
    ) -> RedirectResponse:
        overrides = UIOverride(
            inst_type=inst_type,
            strike=strike,
            expiry=expiry,
            trigger_price=price,
            force_qty=True,
            opt_type=opt_type,
            limit_price=limit_price,
            stop_loss=stop_loss,
            target_price=target_price,
            product_type=product_type,
        )

        inst = self.trader.resolve_instrument(
            symbol,
            exchange,
            signal,
            qty,
            price,
            overrides=overrides,
        )
        if inst:
            sec_id, lot_size = self.trader.lookup_with_fallback(inst)
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
                            sec_id,
                            lot_size,
                            inst,
                            fno_signal=fno_sig,
                        )
        else:
            LOGGER.warning("Could not resolve %s %s", exchange, symbol)

        redirect_url = "?view=order" if view == "order" else _app_cur_redirect_url
        return RedirectResponse(url=redirect_url, status_code=303)

    async def _close_reentry(
        self,
        symbol: str = Form(...),
        exchange: str = Form("NSE"),
        sec_id: str = Form(...),
        exchange_seg: str = Form(...),
        net_qty: int = Form(...),
        product_type: str = Form("INTRADAY"),
        qty: int = Form(1),
        reentry_price: float = Form(0.0),
        reentry_limit_price: float = Form(0.0),
        reentry_stop_loss: float = Form(0.0),
        reentry_target_price: float = Form(0.0),
        reentry_side: str = Form("BUY"),
        reentry_product_type: str = Form("INTRADAY"),
        inst_type: str = Form(""),
        reentry_type: str = Form(""),
        reentry_alert_base: str = Form("PARENT"),
        strike: float = Form(0.0),
        expiry: str = Form(""),
        opt_type: str = Form(""),
    ) -> RedirectResponse:
        self.trader.close_position_by_secid(sec_id, exchange_seg, net_qty, product_type)

        if reentry_price > 0 or reentry_limit_price > 0:
            overrides = UIOverride(
                inst_type=inst_type,
                strike=strike,
                expiry=expiry,
                trigger_price=reentry_price,
                force_qty=True,
                opt_type=opt_type,
                limit_price=reentry_limit_price,
                stop_loss=reentry_stop_loss,
                target_price=reentry_target_price,
                product_type=reentry_product_type,
            )

            inst_reentry = self.trader.resolve_instrument(
                symbol,
                exchange,
                reentry_side,
                qty,
                reentry_price,
                overrides=overrides,
            )

            if inst_reentry:
                new_sec_id, lot_size = self.trader.lookup_with_fallback(inst_reentry)
                if new_sec_id:
                    mode_to_use = reentry_type or self.cfg.reentry_order_mode
                    match mode_to_use:
                        case "FOREVER":
                            self.trader.place_trigger_forever_order(
                                new_sec_id,
                                lot_size,
                                inst_reentry,
                            )
                        case "SUPER":
                            self.trader.place_super_order(
                                new_sec_id,
                                lot_size,
                                inst_reentry,
                            )
                        case "ALERT":
                            fno_sig = (
                                reentry_side if reentry_alert_base == "PARENT" else None
                            )
                            self.trader.place_trigger_alert_order(
                                new_sec_id,
                                lot_size,
                                inst_reentry,
                                fno_signal=fno_sig,
                            )
                        case _:
                            self.trader.place_simple_order(
                                new_sec_id,
                                lot_size,
                                inst_reentry,
                            )

        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _close_position(
        self,
        sec_id: str = Form(...),
        exchange_seg: str = Form(...),
        net_qty: int = Form(...),
        product_type: str = Form("INTRADAY"),
        limit_price: float = Form(0.0),
    ) -> RedirectResponse:
        self.trader.close_position_by_secid(
            sec_id, exchange_seg, net_qty, product_type, limit_price=limit_price
        )

        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _cancel_order(
        self,
        order_id: str = Form(...),
        order_type: str = Form(...),
        leg: str = Form("ENTRY_LEG"),
    ) -> RedirectResponse:
        match order_type:
            case "SUPER":
                self.trader.cancel_super_order(order_id, leg)
            case "FOREVER":
                self.trader.cancel_forever_order(order_id)
            case "ALERT":
                self.trader.cancel_alert_order(order_id)
            case _:
                self.trader.cancel_normal_order(order_id)
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _clean_orphaned(self) -> RedirectResponse:
        self.trader.clean_orphaned_orders()
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _cancel_all(self) -> RedirectResponse:
        for o in self.trader.get_pending_orders():
            self.trader.cancel_normal_order(o["order_id"])
        for _, oid, leg, *_ in self.trader.get_active_super_orders():
            self.trader.cancel_super_order(oid, leg)
        for o in self.trader.get_forever_orders():
            self.trader.cancel_forever_order(o["order_id"])
        for o in self.trader.get_all_alerts():
            self.trader.cancel_alert_order(o["order_id"])
        return RedirectResponse(url=_app_cur_redirect_url, status_code=303)

    async def _receive_webhook(self, alert: TradeAlert) -> dict:
        LOGGER.info("Incoming Webhook Alert: %s", alert.model_dump())

        if not self.cfg.webhook_secret or not hmac.compare_digest(
            alert.secret, self.cfg.webhook_secret
        ):
            LOGGER.info("Unauthorized webhook attempt rejected!")
        try:
            self.trader.fire_trade(
                symb=alert.symbol,
                exch=alert.exch,
                signal=alert.signal.upper(),
                entry_val=alert.price,
            )
        except Exception as err:
            LOGGER.exception("Failed to execute webhook trade")
            raise HTTPException(
                status_code=500, detail="Internal Execution Error"
            ) from err
        else:
            return {
                "status": "success",
                "message": f"Executed {alert.signal} for {alert.symbol}",
            }


if __name__ == "__main__":
    config = AppConfig(paths.config)
    portal = TradePortalApp(config)
    portal.run()
