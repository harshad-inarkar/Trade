from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn
from typing import Optional

from pathlib import Path

from utils.data.paths import TEMPLATES_ROOT_DIR

# Import your existing DhanTrader class
from tradeapi.dhan_trade import DhanTrader

app = FastAPI(title="Dhan Trading Portal")

template_dir =  Path(TEMPLATES_ROOT_DIR) / 'template_trade_client'
templates = Jinja2Templates(directory="templates")

# Initialize the trader (Loads TOML configs, initializes ScripMaster)
trader = DhanTrader()
trader.begin_session()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Renders the main trading dashboard and active positions."""
    # Fetch active positions using your API method
    active_symbols = trader.get_active_positions()
    
    # Format for the template
    positions = [{"symbol": sym} for sym in active_symbols]
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "positions": positions,
        "total_positions": len(positions)
    })

@app.post("/place_order")
async def place_order(
    symbol: str = Form(...),
    exchange: str = Form("NSE"),
    signal: str = Form(...),
    qty: int = Form(1),
    price: float = Form(0.0),
    order_mode: str = Form("MARKET")
):
    """Handles manual order placement from the UI."""
    print(f"UI Order Received: {symbol} | {exchange} | {signal} | Qty: {qty} | Type: {order_mode}")
    
    # 1. Resolve Instrument
    inst = trader.resolve_instrument(symbol, exchange, signal, qty, price)
    if not inst:
        print(f"Failed to resolve instrument for {symbol}")
        return RedirectResponse(url="/?error=resolution_failed", status_code=303)
        
    # 2. Get Security ID and Lot Size
    sec_id, lot_size = trader.scrip.lookup_with_fallback(inst)
    if not sec_id:
        print(f"Failed to lookup Scrip for {symbol}")
        return RedirectResponse(url="/?error=lookup_failed", status_code=303)

    # 3. Route to the correct placement method based on UI selection
    if order_mode == "MARKET":
        trader.place_market_order(sec_id, lot_size, inst)
    elif order_mode == "SUPER":
        trader.place_super_order(sec_id, lot_size, inst)
    elif order_mode == "FOREVER":
        trader.place_trigger_forever_order(sec_id, lot_size, inst)
    elif order_mode == "ALERT":
        # Passing fno_signal as the same signal for simplicity; adjust as needed for your strategy
        trader.place_trigger_alert_order(sec_id, lot_size, inst, fno_signal=signal)

    return RedirectResponse(url="/", status_code=303)

@app.post("/close_position")
async def close_position(
    symbol: str = Form(...),
    exchange: str = Form("NSE"),
    # Ideally, you'd fetch the exact quantity and opposite signal from the live position data
    signal: str = Form("SELL"), 
    qty: int = Form(1)
):
    """Fires an opposing market order to close a position."""
    inst = trader.resolve_instrument(symbol, exchange, signal, qty, entry_val=0.0)
    if inst:
        sec_id, lot_size = trader.scrip.lookup_with_fallback(inst)
        if sec_id:
            trader.place_market_order(sec_id, lot_size, inst)
            
    return RedirectResponse(url="/", status_code=303)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)