"""Dhan HQ automated order placement client."""

import logging
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from types import MappingProxyType
from typing import Any

import requests
import tomllib
from requests.exceptions import RequestException

from tradeapi.price_strike_calc import get_price_strike, get_strike_interval
from tradeapi.scrip_master import ScripMaster, _get_today_str, india_tz
from utils.network.start_proxy import SSHProxyManager

__all__ = ["DhanTrader", "Instrument", "PriceLevels", "UIOverride"]

LOGGER = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
SYMBOLS_CONFIG = BASE_DIR / "symbols_config.toml"
ACCESS_FILE_PATH = BASE_DIR / "access_token.toml"
API_CONFIG_PATH = BASE_DIR / "dhan_trade.toml"
REQUEST_TIMEOUT_SECONDS = 3
OPT_BUMP_MULT = 10


class PriceCondition(Enum):
    GREATER_THAN = "GREATER_THAN"
    LESS_THAN = "LESS_THAN"


@dataclass
class PriceLevels:
    entry: float
    limit: float
    stop_loss: float
    stop_limit: float
    target: float
    trail: float


@dataclass
class Instrument:
    symb: str
    exch: str
    seg: str
    expiry_date: str = ""
    signal: str = ""
    quant: int = 1
    entry_val: float = 0.0
    trade_amount: float = 0.0
    strike: float | None = None
    opt_type: str | None = None
    trigger_price: float = 0.0
    limit_price: float = 0.0


@dataclass
class UIOverride:
    inst_type: str = ""
    strike: float = 0.0
    expiry: str = ""
    limit_price: float = 0.0
    trigger_price: float = 0.0
    force_qty: bool = False
    opt_type: str | None = None


@dataclass
class ForeverOrderParams:
    ord_type: str
    signal: str
    exchange_seg: str
    quant: int
    trigger_price: float
    limit_price: float = 0.0
    trigger_price1: float = 0.0
    is_oco: bool = False
    product_type: str = "CNC"


def _signal_to_opt(signal: str) -> str:
    return "CE" if signal == "BUY" else "PE"


def _invert_signal(signal: str) -> str:
    return "SELL" if signal == "BUY" else "BUY"


def _format_expiry_time(expiry_time: str) -> str:
    try:
        # Expecting ISO format, parse and reformat to 'YYYY-MM-DD  HH:MM'
        dt = datetime.strptime(expiry_time[:16], "%Y-%m-%dT%H:%M").astimezone(
            tz=india_tz
        )
        expiry_time = dt.strftime("%Y-%m-%d  %H:%M")

    except (ValueError, TypeError) as e:
        LOGGER.info("Date conversion failed %s\n%s", expiry_time, e)

    return expiry_time


def _adjust_price(
    base: float,
    perc: float,
    signal: str,
    *,
    opt_bump: bool = False,
) -> float:
    perc = OPT_BUMP_MULT * perc if opt_bump else perc
    if signal == "BUY":
        return math.ceil(base * (1 + perc / 100))
    return math.floor(base * (1 - perc / 100))


class SymbolsConfig:
    def __init__(self, path: Path):
        self._path = path
        self._mtime = None
        self._config = {}

    def get(self, key: str, default: Any = None) -> Any:
        self.refresh()
        return self._config.get(key, default)

    def refresh(self, *, retry: bool = True) -> None:
        try:
            mtime = self._path.stat().st_mtime
        except OSError:
            retry_label = "[Retry] " if not retry else ""
            LOGGER.warning("%sFailed to stat %s", retry_label, self._path)
            if retry:
                self._path = SYMBOLS_CONFIG
                LOGGER.info("Set config path to %s", self._path)
                self.refresh(retry=False)
            return

        if self._mtime == mtime:
            return

        LOGGER.info("Symbols config file changed. Reloading.")
        self._mtime = mtime

        try:
            with self._path.open("rb") as config_file:
                self._config = tomllib.load(config_file) or {}
            LOGGER.info("Symbol config loaded.")
        except (OSError, tomllib.TOMLDecodeError):
            retry_label = "[Retry] " if not retry else ""
            LOGGER.exception(
                "%sFailed to parse TOML config at %s",
                retry_label,
                self._path,
            )


class DhanAPIConfig:
    def __init__(self, path: Path):
        self.settings: dict[str, str] = {}
        self.urls: dict[str, str] = {}
        self.segments: dict[str, list[str]] = {}
        self.opt_segments: frozenset[str] = frozenset()
        self.fut_segments: frozenset[str] = frozenset()
        self.fno_segments: frozenset[str] = frozenset()
        self.maps: dict[str, dict[str, str]] = {}
        self.fallback_steps: list[int] = []
        self._load(path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            LOGGER.warning("Dhan API config not found at %s. Using defaults.", path)
            return
        try:
            with path.open("rb") as config_file:
                data = tomllib.load(config_file)
        except (OSError, tomllib.TOMLDecodeError):
            LOGGER.exception("Failed parsing Dhan API config at %s", path)
            return

        self.settings = data.get("settings", {})
        self.urls = data.get("urls", {})
        self.segments = data.get("segments", {})
        self.opt_segments = frozenset(self.segments.get("opt_segments", []))
        self.fut_segments = frozenset(self.segments.get("fut_segments", []))
        self.fno_segments = frozenset(self.segments.get("fno_segments", []))
        self.maps = data.get("maps", {})
        self.fallback_steps = data.get("fallback", {}).get("steps", [])


class DhanTrader:
    def __init__(
        self,
        symb_config: Path = SYMBOLS_CONFIG,
        *,
        refresh_master_scrip: bool = False,
        restart_proxy: bool = False,
        log_level: str = "",
    ):
        self.api_cfg = DhanAPIConfig(API_CONFIG_PATH)
        self.cfg = SymbolsConfig(symb_config)
        self.cfg.refresh()

        self._set_logging(log_level)

        self._defaults_config: MappingProxyType = MappingProxyType(
            {
                "expiry": self.cfg.get("def_expiry_date", ""),
                "quant": self.cfg.get("def_quantity", 1),
                "trade_amount": self.cfg.get("def_trade_amount", 10000),
                "order_mode": self.cfg.get("def_order_mode", ""),
                "place_order_mode": self.cfg.get("place_order_mode", "MARKET"),
            },
        )

        self.traded_this_scan = set()

        self.proxy_manager = SSHProxyManager()
        if restart_proxy:
            self.proxy_manager.restart()

        self.session = requests.Session()
        self._apply_proxy()

        (
            self.client_id,
            self.access_token,
            self.client_name,
            self.expiry_time,
        ) = self._load_credentials(ACCESS_FILE_PATH)

        self.api_headers = {
            "dhanClientId": self.client_id,
            "access-token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.scrip = ScripMaster(
            session_obj=self.session,
            refresh_master_scrip=refresh_master_scrip,
        )

        self.entry_perc = self.cfg.get("entry_price_perc", 0.1)
        self.limit_perc = self.cfg.get("limit_price_perc", 0.2)
        self.target_perc = self.cfg.get("target_perc", 4.0)
        self.stop_loss_perc = self.cfg.get("stop_loss_perc", 0.7)
        self.stop_trail_perc = self.cfg.get("stop_trail_perc", 0.5)

    def _set_logging(self, log_level: str = "") -> None:

        cfg_log_level = self.api_cfg.settings.get("log_level", "")
        log_level = log_level or cfg_log_level
        if bool(log_level):
            numeric_level = logging.getLevelNamesMapping().get(
                log_level.upper(), logging.CRITICAL
            )
            logging.basicConfig(
                level=numeric_level,
                format="[%(levelname)s] %(message)s",
                handlers=[logging.StreamHandler(sys.stdout)],
            )

    def _apply_proxy(self) -> None:
        try:
            proxy_cfg = self.proxy_manager.config.get("proxy", {})
            proxy_host = proxy_cfg.get("proxy_host", "")
            proxy_port = proxy_cfg.get("port", 0)
            if proxy_host and proxy_port:
                proxy_url = f"socks5h://{proxy_host}:{proxy_port}"
                self.session.proxies = {"http": proxy_url, "https": proxy_url}
        except (AttributeError, TypeError) as exc:
            LOGGER.warning("Unable to apply proxy configuration: %s", exc)

    def _load_credentials(self, path: Path) -> tuple[str, str, str, str]:
        try:
            with path.open("rb") as config_file:
                data = tomllib.load(config_file)
        except (OSError, tomllib.TOMLDecodeError):
            LOGGER.exception("Unable to load Dhan credentials from %s", path)
            return "", "", "", ""

        client_id = str(data.get("CLIENT_ID", "")).strip()
        access_token = str(data.get("ACCESS_TOKEN", "")).strip()
        client_name = str(data.get("CLIENT_NAME", "")).strip()
        expiry_time = str(data.get("TIME_TO_EXPIRY", "")).strip()

        expiry_time = _format_expiry_time(expiry_time)

        if not client_id or not access_token:
            LOGGER.error("Dhan credentials are incomplete in %s.", path)
        return client_id, access_token, client_name, expiry_time

    def update_credentials(
        self,
        new_client_id: str,
        new_access_token: str,
        new_client_name: str,
        new_expiry_time: str,
    ) -> bool:
        try:
            with ACCESS_FILE_PATH.open("w", encoding="utf-8") as f:
                f.write(f'CLIENT_ID = "{new_client_id}"\n')
                f.write(f'ACCESS_TOKEN = "{new_access_token}"\n')
                f.write(f'CLIENT_NAME = "{new_client_name}"\n')
                f.write(f'TIME_TO_EXPIRY = "{new_expiry_time}"\n')

            self.client_id = new_client_id
            self.access_token = new_access_token
            self.client_name = new_client_name
            self.expiry_time = _format_expiry_time(new_expiry_time)

            self.api_headers["access-token"] = new_access_token
            self.api_headers["dhanClientId"] = self.client_id

            LOGGER.info(
                "API Credentials successfully updated in memory and saved to disk."
            )
        except OSError:
            LOGGER.exception("Failed to write to %s", ACCESS_FILE_PATH)
            return False
        else:
            return True

    def generate_token(self, client_id: str, pin: str, totp: str) -> bool:
        try:
            # Direct post to bypass our standard retry/auth logic
            url = (
                f"{self.api_cfg.urls.get('gen_token', '')}"
                f"?dhanClientId={client_id}&pin={pin}&totp={totp}"
            )

            resp = self._request_with_retry("POST", url, label="GENTOKEN", data={})

            if resp and resp.status_code == HTTPStatus.OK:
                data = resp.json()
                return self.update_credentials(
                    new_client_id=client_id,
                    new_access_token=data.get("accessToken", ""),
                    new_client_name=data.get("dhanClientName", ""),
                    new_expiry_time=data.get("expiryTime", ""),
                )
            LOGGER.error("Generate Token Failed: %s", resp.text)
        except Exception:
            LOGGER.exception("Generate Token Exception")
            return False
        else:
            return False

    def renew_token(self) -> bool:
        try:
            # Renew requires the CURRENT access token in the headers
            url = self.api_cfg.urls.get("renew_token", "")
            resp = self._request_with_retry("GET", url, label="RENEW", data={})

            if resp and resp.status_code == HTTPStatus.OK:
                data = resp.json()
                return self.update_credentials(
                    new_client_id=self.client_id,
                    new_access_token=data.get("token", ""),
                    new_client_name=self.client_name,
                    new_expiry_time=data.get("expiryTime", ""),
                )
            LOGGER.error("Renew Token Failed: %s", resp.text)

        except Exception:
            LOGGER.exception("Renew Token Exception")
            return False
        else:
            return False

    def begin_session(self) -> None:
        self.cfg.refresh()
        self.traded_this_scan.clear()

    def _compute_price_levels(
        self,
        raw_entry: float,
        signal: str,
        *,
        opt_bump: bool = False,
    ) -> PriceLevels:
        inv = _invert_signal(signal)
        entry = _adjust_price(raw_entry, self.entry_perc, signal, opt_bump=opt_bump)
        limit = _adjust_price(entry, self.limit_perc, signal, opt_bump=opt_bump)
        stop_loss = _adjust_price(entry, self.stop_loss_perc, inv, opt_bump=opt_bump)
        stop_limit = _adjust_price(stop_loss, self.limit_perc, inv, opt_bump=opt_bump)
        target = _adjust_price(entry, self.target_perc, signal, opt_bump=opt_bump)
        trail_factor = self.stop_trail_perc * (OPT_BUMP_MULT if opt_bump else 1)
        trail = math.ceil(entry * trail_factor / 100)
        return PriceLevels(entry, limit, stop_loss, stop_limit, target, trail)

    def _get_symbol_config(self, symb: str, exch: str) -> dict:
        dfl = self._defaults_config
        res = {
            "order_mode": dfl["order_mode"],
            "expiry_date": dfl["expiry"],
            "quantity": dfl["quant"],
            "trade_amount": dfl["trade_amount"],
            "call_strike": None,
            "put_strike": None,
            "strike": None,
            "is_index": False,
        }

        if exch == "NSE":
            nse_cfg = self.cfg.get("nse", {})
            indices = nse_cfg.get("indices", {})
            if symb in indices.get("symbols", {}):
                sym_cfg = indices["symbols"][symb]
                grp_mode = indices.get("config", {}).get(
                    "order_mode",
                    res["order_mode"],
                )
                grp_exp = indices.get("config", {}).get(
                    "expiry_date",
                    res["expiry_date"],
                )

                res["order_mode"] = sym_cfg.get("order_mode", grp_mode)
                res["expiry_date"] = sym_cfg.get("expiry_date", grp_exp)
                res["quantity"] = sym_cfg.get("quantity", res["quantity"])
                res["call_strike"] = sym_cfg.get("call_strike")
                res["put_strike"] = sym_cfg.get("put_strike")
                res["strike"] = sym_cfg.get("strike")
                res["is_index"] = True
                return res

            stocks = nse_cfg.get("stocks", {})
            if symb in stocks.get("symbols", {}):
                sym_cfg = stocks["symbols"][symb]
                grp_mode = stocks.get("config", {}).get("order_mode", res["order_mode"])
                grp_exp = stocks.get("config", {}).get(
                    "expiry_date",
                    res["expiry_date"],
                )
                grp_amt = stocks.get("config", {}).get(
                    "trade_amount",
                    res["trade_amount"],
                )

                res["order_mode"] = sym_cfg.get("order_mode", grp_mode)
                res["expiry_date"] = sym_cfg.get("expiry_date", grp_exp)
                res["trade_amount"] = sym_cfg.get("trade_amount", grp_amt)
                res["quantity"] = sym_cfg.get("quantity", res["quantity"])
                return res

            res["order_mode"] = stocks.get("config", {}).get(
                "order_mode",
                res["order_mode"],
            )
            res["expiry_date"] = stocks.get("config", {}).get(
                "expiry_date",
                res["expiry_date"],
            )
            res["trade_amount"] = stocks.get("config", {}).get(
                "trade_amount",
                res["trade_amount"],
            )

        elif exch == "MCX":
            mcx = self.cfg.get("mcx", {}).get("comm", {})
            if symb in mcx.get("symbols", {}):
                sym_cfg = mcx["symbols"][symb]
                grp_mode = mcx.get("config", {}).get("order_mode", res["order_mode"])
                grp_exp = mcx.get("config", {}).get("expiry_date", res["expiry_date"])

                res["order_mode"] = sym_cfg.get("order_mode", grp_mode)
                res["expiry_date"] = sym_cfg.get("expiry_date", grp_exp)
                res["quantity"] = sym_cfg.get("quantity", res["quantity"])

        return res

    def _merge_overrides(
        self,
        symb: str,
        exch: str,
        overrides: UIOverride,
    ) -> tuple[str, str]:
        data = self.scrip.get_data_by_display_name(symb)
        if data:
            symb = data["symbol"]
            exch = data["exch"]
            if not overrides.inst_type:
                overrides.inst_type = data["inst_type"]
            if overrides.strike <= 0 and data["strike"] > 0:
                overrides.strike = data["strike"]
            if not overrides.opt_type and data["opt_type"]:
                overrides.opt_type = data["opt_type"]
            if not overrides.expiry and data["expiry"]:
                overrides.expiry = data["expiry"]
        return symb, exch

    def _resolve_segment(
        self,
        exch: str,
        ord_mode: str,
        *,
        is_index: bool,
    ) -> str | None:
        if exch == "NSE":
            if is_index:
                return (
                    "OPTIDX"
                    if ord_mode == "OPT"
                    else ("FUTIDX" if ord_mode == "FUT" else "")
                )
            return (
                "EQUITY"
                if ord_mode == "EQ"
                else (
                    "OPTSTK"
                    if ord_mode == "OPT"
                    else ("FUTSTK" if ord_mode == "FUT" else "")
                )
            )
        if exch == "MCX":
            return (
                "OPTFUT"
                if ord_mode == "OPT"
                else ("FUTCOM" if ord_mode == "FUT" else "")
            )
        return None

    def _resolve_option_params(
        self,
        symb: str,
        signal: str,
        entry_val: float,
        ord_mode: str,
        overrides: UIOverride,
        sym_cfg: dict,
    ) -> tuple[str | None, str, float | None]:
        if ord_mode != "OPT":
            return None, signal, None
        opt_type = overrides.opt_type or _signal_to_opt(signal)
        fin_signal = signal if overrides.opt_type else "BUY"
        if overrides.strike > 0:
            strike = overrides.strike
        else:
            sig_key = "call_strike" if signal == "BUY" else "put_strike"
            strike = (
                sym_cfg.get(sig_key)
                or sym_cfg.get("strike")
                or get_price_strike(symb, entry_val, signal)
            )
        return opt_type, fin_signal, strike

    def resolve_instrument(
        self,
        symb: str,
        exch: str,
        signal: str,
        quant: int,
        entry_val: float,
        overrides: UIOverride | None = None,
    ) -> Instrument | None:
        overrides = overrides or UIOverride()
        symb, exch = self._merge_overrides(symb, exch, overrides)
        sym_cfg = self._get_symbol_config(symb, exch)

        ord_mode = overrides.inst_type or sym_cfg.get("order_mode", "EQ")
        trade_amt = 0.0 if overrides.force_qty else sym_cfg.get("trade_amount", 0.0)
        fin_quant = (
            quant if (overrides.force_qty or quant > 1) else sym_cfg.get("quantity", 1)
        )

        raw_exp = str(overrides.expiry or sym_cfg.get("expiry_date", ""))
        exp_parts = raw_exp.split(maxsplit=1)
        expiry = exp_parts[0] if exp_parts else ""

        seg = self._resolve_segment(
            exch,
            ord_mode,
            is_index=sym_cfg.get("is_index", False),
        )
        if not seg:
            return None

        opt_type, fin_signal, strike = self._resolve_option_params(
            symb,
            signal,
            entry_val,
            ord_mode,
            overrides,
            sym_cfg,
        )

        return Instrument(
            symb=symb,
            exch=exch,
            seg=seg,
            expiry_date=expiry,
            signal=fin_signal,
            quant=fin_quant,
            strike=strike,
            opt_type=opt_type,
            entry_val=entry_val,
            trade_amount=trade_amt,
            trigger_price=overrides.trigger_price,
            limit_price=overrides.limit_price,
        )

    def get_instr_data(self, inst: Instrument) -> tuple[str, str]:
        if inst.seg in self.api_cfg.opt_segments:
            display_symb = (
                f"{inst.symb} {inst.strike} {inst.opt_type} {inst.expiry_date}"
            )
        elif inst.seg in self.api_cfg.fut_segments:
            display_symb = f"{inst.symb} Fut {inst.expiry_date}"
        else:
            display_symb = f"{inst.symb} {inst.seg}"

        seg_suffix = self.api_cfg.maps.get("seg_exchange_suffix", {}).get(inst.seg, "")
        exch_seg = "IDX_I" if inst.seg == "INDEX" else f"{inst.exch}_{seg_suffix}"
        return display_symb, exch_seg

    def _get_fallback_strike(
        self,
        base: str,
        strike: float,
        opt_type: str,
    ) -> float | None:
        fb_step = None
        for step in self.api_cfg.fallback_steps:
            if step > get_strike_interval(base, strike):
                fb_step = step
                break

        if fb_step is None:
            return None

        if opt_type == "CE":
            new_strike = math.floor(strike / fb_step) * fb_step
        else:
            new_strike = math.ceil(strike / fb_step) * fb_step

        return float(new_strike) if new_strike != strike else None

    def lookup_with_fallback(self, inst: Instrument) -> tuple[str | None, int]:
        sec_id, lot_size = self.scrip.lookup(
            inst.exch,
            inst.seg,
            inst.symb,
            inst.expiry_date,
            inst.strike,
            inst.opt_type,
        )
        if sec_id is not None:
            return sec_id, lot_size

        if inst.seg not in self.api_cfg.opt_segments:
            return None, 0

        fb_strike = self._get_fallback_strike(inst.symb, inst.strike, inst.opt_type)
        if fb_strike:
            original_strike = inst.strike
            inst.strike = fb_strike
            sec_id, lot_size = self.scrip.lookup(
                inst.exch,
                inst.seg,
                inst.symb,
                inst.expiry_date,
                inst.strike,
                inst.opt_type,
            )
            if sec_id is not None:
                return sec_id, lot_size
            inst.strike = original_strike

        return None, 0

    def _request_with_retry(
        self,
        method: str,
        url: str,
        label: str = "",
        **kwargs: Any,
    ) -> requests.Response | None:
        for attempt in range(2):
            try:
                return self.session.request(
                    method,
                    url,
                    headers=self.api_headers,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    **kwargs,
                )
            except RequestException:
                if attempt == 0:
                    LOGGER.warning(
                        "Request failed for %s — restarting proxy and retrying.",
                        label,
                    )
                    self.proxy_manager.restart()
        return None

    def _post_order(self, url: str, payload: dict, label: str = "") -> None:
        resp = self._request_with_retry("POST", url, label=label, json=payload)
        if resp and resp.status_code == HTTPStatus.OK:
            LOGGER.info("[✓] %s Order placed successfully.", label)
        else:
            err_msg = resp.json() if resp is not None else "No Response Data"
            LOGGER.error(
                "[✗] %s Order failed. Payload: %s | Response: %s",
                label,
                payload,
                err_msg,
            )

    def _base_payload(self, signal: str, exchange_seg: str, sec_id: str) -> dict:
        return {
            "dhanClientId": self.client_id,
            "correlationId": f"auto_{self.client_id}",
            "transactionType": signal,
            "exchangeSegment": exchange_seg,
            "productType": "INTRADAY",
            "orderType": "MARKET",
            "validity": "DAY",
            "securityId": sec_id,
            "quantity": 0,
            "price": 0,
            "triggerPrice": 0,
            "afterMarketOrder": False,
            "amoTime": "OPEN",
            "targetPrice": 0,
            "stopLossPrice": 0,
        }

    def _build_alert_payload(
        self,
        alert_exch_seg: str,
        alert_sec_id: str,
        operator: str,
        comp_price: float,
        exp_date: str,
        note: str,
        orders: list[dict],
    ) -> dict:
        return {
            "dhanClientId": self.client_id,
            "condition": {
                "comparisonType": "PRICE_WITH_VALUE",
                "exchangeSegment": alert_exch_seg,
                "securityId": alert_sec_id,
                "operator": operator,
                "comparingValue": comp_price,
                "expDate": exp_date,
                "frequency": "ONCE",
                "userNote": note,
            },
            "orders": orders,
        }

    def _compute_quantity(
        self,
        trade_amount: float,
        price: float,
        lot_size: int,
        base_quant: int,
    ) -> int:
        if trade_amount > 0 and price > 0:
            lots = math.ceil(trade_amount / (price * lot_size))
            return lots * lot_size
        return base_quant * lot_size

    def place_super_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        raw_price = inst.entry_val if inst.entry_val > 0 else inst.limit_price
        levels = self._compute_price_levels(
            raw_price, inst.signal, opt_bump=inst.seg in self.api_cfg.opt_segments
        )
        total_quant = self._compute_quantity(
            inst.trade_amount,
            levels.entry,
            lot_size,
            inst.quant,
        )

        final_limit = inst.limit_price if inst.limit_price > 0 else levels.limit

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            "orderType": "LIMIT",
            "quantity": total_quant,
            "price": final_limit,
            "stopLossPrice": levels.stop_loss,
            "trailingJump": levels.trail,
        }
        self._post_order(
            self.api_cfg.urls.get("super_order", ""),
            payload,
            label="SUPER",
        )

    def place_market_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            "quantity": inst.quant * lot_size,
        }
        self._post_order(self.api_cfg.urls.get("order", ""), payload, label="MARKET")

    def _get_ord_type(self, inst: Instrument) -> str:
        if inst.trigger_price > 0 and inst.limit_price > 0:
            return "STOP_LOSS"
        if inst.trigger_price > 0:
            return "STOP_LOSS_MARKET"
        if inst.limit_price > 0:
            return "LIMIT"
        return "MARKET"

    def place_simple_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        ord_type = self._get_ord_type(inst)

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            "quantity": inst.quant * lot_size,
            "orderType": ord_type,
            "price": inst.limit_price,
            "triggerPrice": inst.trigger_price,
        }
        self._post_order(self.api_cfg.urls.get("order", ""), payload, label=ord_type)

    def place_forever_order(self, sec_id: str, params: ForeverOrderParams) -> None:
        payload = self._base_payload(params.signal, params.exchange_seg, sec_id) | {
            "correlationId": f"cond_{self.client_id}",
            "orderFlag": "OCO" if params.is_oco else "SINGLE",
            "orderType": params.ord_type,
            "productType": params.product_type,
            "validity": "DAY",
            "quantity": params.quant,
            "disclosedQuantity": 0,
            "triggerPrice": params.trigger_price,
            "price": params.limit_price,
            "price1": 0,
            "triggerPrice1": params.trigger_price1,
            "quantity1": params.quant if params.is_oco else 0,
        }
        self._post_order(
            self.api_cfg.urls.get("forever_order", ""),
            payload,
            label="FOREVER",
        )

    def place_trigger_forever_order(
        self,
        sec_id: str,
        lot_size: int,
        inst: Instrument,
    ) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        levels = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = self._compute_quantity(
            inst.trade_amount,
            levels.entry,
            lot_size,
            inst.quant,
        )

        product_type = "MARGIN" if inst.seg in self.api_cfg.fno_segments else "CNC"
        trig_price = inst.trigger_price if inst.trigger_price > 0 else levels.entry
        ord_type = self._get_ord_type(inst)

        if ord_type != "MARKET":
            ord_type = "LIMIT"

        params = ForeverOrderParams(
            ord_type=ord_type,
            signal=inst.signal,
            exchange_seg=exchange_seg,
            quant=total_quant,
            trigger_price=trig_price,
            limit_price=inst.limit_price,
            product_type=product_type,
        )
        self.place_forever_order(sec_id, params)

    def place_trigger_alert_order(
        self,
        sec_id: str,
        lot_size: int,
        inst: Instrument,
        fno_signal: str | None = None,
    ) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        alert_signal = fno_signal or inst.signal
        levels = self._compute_price_levels(inst.entry_val, alert_signal)
        total_quant = self._compute_quantity(
            inst.trade_amount,
            levels.entry,
            lot_size,
            inst.quant,
        )

        ord_payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            "quantity": total_quant,
        }
        condition = (
            PriceCondition.GREATER_THAN.value
            if alert_signal == "BUY"
            else PriceCondition.LESS_THAN.value
        )

        alert_sec_id, alert_exch_seg = sec_id, exchange_seg
        if fno_signal:
            parent_seg = self.api_cfg.maps.get("underlying_seg_map", {}).get(
                inst.seg,
                inst.seg,
            )
            alert_sec_id, _ = self.scrip.lookup(
                inst.exch,
                parent_seg,
                inst.symb,
                inst.expiry_date,
                inst.strike,
                inst.opt_type,
            )
            if not alert_sec_id:
                return
            parent_suffix = self.api_cfg.maps.get("seg_exchange_suffix", {}).get(
                parent_seg,
                "",
            )
            alert_exch_seg = (
                "IDX_I" if parent_seg == "INDEX" else f"{inst.exch}_{parent_suffix}"
            )

        payload = self._build_alert_payload(
            alert_exch_seg,
            alert_sec_id,
            condition,
            levels.entry,
            _get_today_str(),
            "Main Order",
            [ord_payload],
        )
        self._post_order(
            self.api_cfg.urls.get("alert_order", ""),
            payload,
            label="ALERT",
        )

    def get_funds(self) -> float:
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("fund_limit", ""),
            label="GET Funds",
        )
        if not resp or resp.status_code != HTTPStatus.OK:
            return 0.0
        try:
            data = resp.json()
        except ValueError:
            return 0.0
        return float(data.get("availabelBalance", data.get("availableBalance", 0.0)))

    def _aggregate_positions(self, resp_data: list[dict]) -> dict:
        aggregated = {}
        for pos in resp_data:
            if not pos.get("tradingSymbol"):
                continue

            sec_id = str(pos.get("securityId", ""))
            if sec_id not in aggregated:
                trade_sym = pos.get("tradingSymbol", "")
                exch = pos.get("exchangeSegment", "NSE_EQ").split("_")[0]

                aggregated[sec_id] = {
                    "display_name": self.scrip.get_symbol_name(sec_id, trade_sym),
                    "base_symbol": self.scrip.get_base_symbol(sec_id, trade_sym),
                    "security_id": sec_id,
                    "exchange_seg": pos.get("exchangeSegment", "NSE_EQ"),
                    "exchange": exch,
                    "multiplier": float(pos.get("multiplier", 1.0)),
                    "realizedProfit": 0.0,
                    "unrealizedProfit": 0.0,
                    "netQty": 0,
                    "buyQty": 0,
                    "sellQty": 0,
                    "totBuyVal": 0.0,
                    "totSellVal": 0.0,
                }

            agg = aggregated[sec_id]
            mult = float(pos.get("multiplier", 1.0))

            agg["realizedProfit"] += float(pos.get("realizedProfit", 0.0))
            agg["unrealizedProfit"] += float(pos.get("unrealizedProfit", 0.0)) * mult
            agg["netQty"] += int(pos.get("netQty", 0))
            agg["buyQty"] += int(pos.get("buyQty", 0))
            agg["sellQty"] += int(pos.get("sellQty", 0))
            agg["totBuyVal"] += float(pos.get("buyAvg", 0)) * int(pos.get("buyQty", 0))
            agg["totSellVal"] += float(pos.get("sellAvg", 0)) * int(
                pos.get("sellQty", 0)
            )

        return aggregated

    def get_positions(self) -> tuple[list[dict], list[dict]]:
        """Returns isolated lists of Active and Closed positions with true net PnL."""
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("positions", ""),
            label="GET Positions",
        )
        if resp is None or resp.status_code != HTTPStatus.OK:
            return [], []

        try:
            resp_data = resp.json()
        except ValueError:
            return [], []

        aggregated = self._aggregate_positions(resp_data)

        LOGGER.debug("All Positions:\n%s", resp_data)

        active = []
        closed = []

        for agg in aggregated.values():
            qty = agg["netQty"]
            entry = {
                "display_name": agg["display_name"],
                "base_symbol": agg["base_symbol"],
                "security_id": agg["security_id"],
                "exchange_seg": agg["exchange_seg"],
                "exchange": agg["exchange"],
                "qty": qty * agg["multiplier"],
                "buyqty": agg["buyQty"],
                "sellqty": agg["sellQty"],
                "pnl": 0.0,
                "entry_price": 0.0,
                "ltp": 0.0,
            }

            if qty != 0:
                mult = agg["multiplier"]
                entry["pnl"] = agg["unrealizedProfit"]

                # Optimized Net Cash Flow approach (avoids ZeroDivisionError entirely)
                if qty > 0:
                    active_cost = (
                        agg["totBuyVal"]
                        - agg["totSellVal"]
                        + (agg["realizedProfit"] / mult)
                    )
                    entry["entry_price"] = active_cost / qty
                    entry["ltp"] = entry["entry_price"] + (
                        agg["unrealizedProfit"] / (qty * mult)
                    )
                else:
                    abs_qty = abs(qty)
                    active_cost = (
                        agg["totSellVal"]
                        - agg["totBuyVal"]
                        - (agg["realizedProfit"] / mult)
                    )
                    entry["entry_price"] = active_cost / abs_qty
                    entry["ltp"] = entry["entry_price"] - (
                        agg["unrealizedProfit"] / (abs_qty * mult)
                    )

                active.append(entry)

            elif agg["buyQty"] > 0 or agg["sellQty"] > 0:
                entry["pnl"] = agg["realizedProfit"]
                entry["buy_avg"] = (
                    (agg["totBuyVal"] / agg["buyQty"]) if agg["buyQty"] > 0 else 0.0
                )
                entry["sell_avg"] = (
                    (agg["totSellVal"] / agg["sellQty"]) if agg["sellQty"] > 0 else 0.0
                )
                closed.append(entry)

        return active, closed

    def get_active_positions(self) -> list[dict]:
        """Legacy helper matching old return structures."""
        active, _ = self.get_positions()
        return active

    def get_pending_orders(
        self,
        pending_statuses: tuple[str, ...] = ("TRANSIT", "PENDING", "PART_TRADED"),
    ) -> list[dict]:
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("order", ""),
            label="GET Orders",
        )
        if not resp or resp.status_code != HTTPStatus.OK:
            return []

        try:
            resp_data = resp.json()
        except ValueError:
            return []

        results = []
        for order in resp_data:
            if order.get("orderStatus", "") not in pending_statuses:
                continue

            sec_id = str(order.get("securityId", ""))
            display_sym = self.scrip.get_symbol_name(
                sec_id,
                order.get("tradingSymbol", ""),
            )
            results.append(
                {
                    "symbol": display_sym,
                    "order_id": order.get("orderId", ""),
                    "type": order.get("orderType", "MARKET"),
                    "qty": order.get("quantity", 0),
                    "price": order.get("price", 0.0),
                    "trigger_price": order.get("triggerPrice", 0.0),
                    "transaction_type": order.get("transactionType", ""),
                },
            )
        return results

    def get_active_super_orders(self) -> set[tuple]:
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("super_order", ""),
            label="GET Super Orders",
        )
        if not resp or resp.status_code != HTTPStatus.OK:
            return set()

        try:
            resp_data = resp.json()
        except ValueError:
            return set()

        active_orders = set()
        for order in resp_data:
            status = order.get("orderStatus", "")
            sec_id = str(order.get("securityId", ""))
            sym = self.scrip.get_symbol_name(sec_id, order.get("tradingSymbol", ""))
            oid = order.get("orderId", "")
            txn = order.get("transactionType", "")
            qty = order.get("quantity", 0)
            prc = order.get("price", 0.0)
            trg = order.get("triggerPrice", 0.0)

            if status in {"PENDING", "PART_TRADED"}:
                active_orders.add((sym, oid, "ENTRY_LEG", txn, qty, prc, trg))

            if status in {"PENDING", "PART_TRADED", "TRADED"}:
                for leg in order.get("legDetails", []):
                    if leg.get("orderStatus") == "PENDING":
                        active_orders.add(
                            (
                                sym,
                                oid,
                                leg.get("legName", ""),
                                leg.get("transactionType", txn),
                                leg.get("quantity", qty),
                                leg.get("price", prc),
                                leg.get("triggerPrice", trg),
                            ),
                        )
        return active_orders

    def get_forever_orders(
        self,
        active_statuses: tuple[str, ...] = ("PENDING", "CONFIRM"),
    ) -> list[dict]:
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("forever_order", ""),
            label="GET Forever Orders",
        )
        if not resp or resp.status_code != HTTPStatus.OK:
            return []

        try:
            resp_data = resp.json()
        except ValueError:
            return []

        results = []
        for order in resp_data:
            if order.get("orderStatus", "") not in active_statuses:
                continue

            sec_id = str(order.get("securityId", ""))
            display_sym = self.scrip.get_symbol_name(
                sec_id,
                order.get("tradingSymbol", ""),
            )
            results.append(
                {
                    "symbol": display_sym,
                    "order_id": order.get("orderId", ""),
                    "type": "FOREVER",
                    "leg": order.get("legName", "TARGET_LEG"),
                    "qty": order.get("quantity", 0),
                    "price": order.get("price", 0.0),
                    "trigger_price": order.get("triggerPrice", 0.0),
                    "transaction_type": order.get("transactionType", ""),
                    "flag": order.get("orderType", "SINGLE"),
                },
            )
        return results

    def get_all_alerts(
        self,
        active_statuses: tuple[str, ...] = ("ACTIVE",),
    ) -> list[dict]:
        resp = self._request_with_retry(
            "GET",
            self.api_cfg.urls.get("alert_order", ""),
            label="GET Alert Orders",
        )
        if not resp or resp.status_code != HTTPStatus.OK:
            return []

        try:
            resp_data = resp.json()
        except ValueError:
            return []

        results = []
        for alert in resp_data:
            if alert.get("alertStatus", "") not in active_statuses:
                continue

            cond = alert.get("condition", {})
            orders = alert.get("orders", [{}])

            sec_id = str(orders[0].get("securityId", "")) if orders else ""
            qty = orders[0].get("quantity", 0) if orders else 0
            prc = orders[0].get("price", 0.0) if orders else 0.0
            txn = orders[0].get("transactionType", "") if orders else ""

            display_sym = self.scrip.get_symbol_name(sec_id, f"Trig: {sec_id}")
            results.append(
                {
                    "symbol": display_sym,
                    "order_id": alert.get("alertId", ""),
                    "type": "ALERT",
                    "leg": "",
                    "qty": qty,
                    "price": prc,
                    "transaction_type": txn,
                    "condition_note": cond.get("userNote", ""),
                    "comparing_value": cond.get("comparingValue", 0.0),
                    "exp_date": cond.get("expDate", ""),
                },
            )
        return results

    def cancel_normal_order(self, order_id: str) -> bool:
        return (
            self._request_with_retry(
                "DELETE",
                f"{self.api_cfg.urls.get('order', '')}/{order_id}",
                label=f"Cancel {order_id}",
            )
            is not None
        )

    def cancel_super_order(self, order_id: str, order_leg: str = "ENTRY_LEG") -> bool:
        return (
            self._request_with_retry(
                "DELETE",
                f"{self.api_cfg.urls.get('super_order', '')}/{order_id}/{order_leg}",
                label=f"Cancel {order_id}",
            )
            is not None
        )

    def cancel_forever_order(self, order_id: str) -> bool:
        return (
            self._request_with_retry(
                "DELETE",
                f"{self.api_cfg.urls.get('forever_order', '')}/{order_id}",
                label=f"Cancel {order_id}",
            )
            is not None
        )

    def cancel_alert_order(self, alert_id: str) -> bool:
        return (
            self._request_with_retry(
                "DELETE",
                f"{self.api_cfg.urls.get('alert_order', '')}/{alert_id}",
                label=f"Cancel {alert_id}",
            )
            is not None
        )

    def close_position_by_secid(
        self,
        sec_id: str,
        exchange_seg: str,
        net_qty: int,
    ) -> None:
        if net_qty == 0:
            return
        signal = "SELL" if net_qty > 0 else "BUY"
        payload = self._base_payload(signal, exchange_seg, sec_id) | {
            "quantity": abs(net_qty),
        }
        self._post_order(self.api_cfg.urls.get("order", ""), payload, label="CLOSE_POS")

    def dispatch_order(
        self,
        sec_id: str,
        lot_size: int,
        inst: Instrument,
        signal: str,
    ) -> None:
        mode = self._defaults_config.get("place_order_mode", "MARKET")

        if inst.seg in self.api_cfg.opt_segments and inst.exch == "MCX":
            self.place_simple_order(sec_id, lot_size, inst)
            return

        is_fno = inst.seg in self.api_cfg.fno_segments
        is_opt = inst.seg in self.api_cfg.opt_segments

        if mode == "ALERT":
            if is_fno and inst.exch != "MCX":
                self.place_trigger_alert_order(
                    sec_id,
                    lot_size,
                    inst,
                    fno_signal=signal,
                )
            else:
                self.place_trigger_alert_order(sec_id, lot_size, inst)
            return

        if mode in ("FOREVER", "SUPER"):
            if is_fno and is_opt:
                self.place_simple_order(sec_id, lot_size, inst)
                return
            if mode == "FOREVER":
                self.place_trigger_forever_order(sec_id, lot_size, inst)
            else:
                self.place_super_order(sec_id, lot_size, inst)
            return

        self.place_simple_order(sec_id, lot_size, inst)

    def fire_trade(
        self,
        symb: str,
        exch: str,
        signal: str,
        quant: int = 1,
        entry_val: float = 0.0,
    ) -> None:
        trade_key = f"{exch}:{symb}:{signal}"
        if trade_key in self.traded_this_scan:
            LOGGER.info("[skip] %s already traded this scan cycle.", trade_key)
            return

        self.traded_this_scan.add(trade_key)
        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None:
            return

        sec_id, lot_size = self.lookup_with_fallback(inst)
        if sec_id is None:
            return

        self.dispatch_order(sec_id, lot_size, inst, signal)

    def clean_orphaned_orders(self) -> None:
        LOGGER.info("Starting Cleanup Cycle")
        active_positions, _ = self.get_positions()
        active_symbols = {pos.get("display_name") for pos in active_positions}
        active_super_orders = self.get_active_super_orders()

        cancelled = [
            symb
            for symb, oid, leg, *_ in active_super_orders
            if symb not in active_symbols and self.cancel_super_order(oid, leg)
        ]

        if cancelled:
            LOGGER.info(
                "Cleanup Complete. Cancelled %d orphaned orders.",
                len(cancelled),
            )
            LOGGER.info("Symbols: %s", cancelled)
        else:
            LOGGER.info("Cleanup Complete. No orphaned orders found.")


# INSERT_YOUR_CODE
if __name__ == "__main__":
    trader = DhanTrader(log_level="debug")
    trader.get_positions()
