from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import uvicorn
from typing import Optional
from pathlib import Path

from utils.data.paths import TEMPLATES_ROOT_DIR
from tradeapi.dhan_trade import DhanTrader

app = FastAPI(title="Dhan Trading Portal")

template_dir = Path(TEMPLATES_ROOT_DIR) / 'template_trade_client'
templates = Jinja2Templates(directory=template_dir)

# Initialize the trader
trader = DhanTrader()
trader.begin_session()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Renders the main trading dashboard, active positions, and open orders."""
    # 1. Fetch Active Positions
    active_symbols = trader.get_active_positions()
    positions = [{"symbol": sym} for sym in active_symbols]
    
    # 2. Fetch Active Orders (Currently fetching Super Orders based on your wrapper)
    super_orders = trader.get_active_super_orders()
    active_orders = []
    
    for symbol, oid, leg in super_orders:
        active_orders.append({
            "symbol": symbol,
            "order_id": oid,
            "leg": leg,
            "type": "SUPER"
        })
        
    # Note: If you add `get_pending_orders()` or `get_forever_orders()` to dhan_trade.py, 
    # you can append them to the `active_orders` list here in the same dictionary format.

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "positions": positions,
        "total_positions": len(positions),
        "active_orders": active_orders,
        "total_orders": len(active_orders)
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
    
    inst = trader.resolve_instrument(symbol, exchange, signal, qty, price)
    if not inst:
        return RedirectResponse(url="/?error=resolution_failed", status_code=303)
        
    sec_id, lot_size = trader.scrip.lookup_with_fallback(inst)
    if not sec_id:
        return RedirectResponse(url="/?error=lookup_failed", status_code=303)

    if order_mode == "MARKET":
        trader.place_market_order(sec_id, lot_size, inst)
    elif order_mode == "SUPER":
        trader.place_super_order(sec_id, lot_size, inst)
    elif order_mode == "FOREVER":
        trader.place_trigger_forever_order(sec_id, lot_size, inst)
    elif order_mode == "ALERT":
        trader.place_trigger_alert_order(sec_id, lot_size, inst, fno_signal=signal)

    return RedirectResponse(url="/", status_code=303)

@app.post("/close_position")
async def close_position(
    symbol: str = Form(...),
    exchange: str = Form("NSE"),
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

@app.post("/cancel_order")
async def cancel_order(
    order_id: str = Form(...),
    order_type: str = Form(...),
    leg: str = Form("ENTRY_LEG")
):
    """Cancels a pending active order."""
    print(f"Cancel Request Received: {order_id} | Type: {order_type} | Leg: {leg}")
    
    if order_type == "SUPER":
        trader.cancel_super_order(order_id, leg)
    else:
        # Extend dhan_trade.py to include a standard cancel_order(order_id) method
        print(f"[!] Standard order cancellation for {order_type} not yet mapped.")
        # trader.cancel_normal_order(order_id) 

    return RedirectResponse(url="/", status_code=303)

if __name__ == "__main__":
    uvicorn.run("trade_app:app", host="127.0.0.1", port=8000, reload=True)