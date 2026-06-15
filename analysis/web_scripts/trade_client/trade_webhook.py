import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from tradeapi.dhan_trade import DhanTrader

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Initialize FastAPI app
app = FastAPI(title="Dhan Execution Webhook")

# Initialize the trader session once at startup
trader = DhanTrader(log_level="info")
trader.begin_session()

# Define the expected TradingView JSON payload
class TradeAlert(BaseModel):
    symbol: str
    signal: str
    price: float
    exch: str

@app.post("/")
def receive_webhook(alert: TradeAlert):
    logging.info(f"Incoming Alert: {alert.model_dump()}")
    
    try:
        # Fire the trade using your existing script
        trader.fire_trade(
            symb=alert.symbol,
            exch=alert.exch,
            signal=alert.signal.upper(),
            entry_val=alert.price
        )
        return {"status": "success", "message": f"Executed {alert.signal} for {alert.symbol}"}
        
    except Exception as e:
        logging.error(f"Failed to execute trade: {e}")
        raise HTTPException(status_code=500, detail="Internal Execution Error")

if __name__ == "__main__":
    # Runs the server locally or on your cloud instance on port 8080
    uvicorn.run(app, host="0.0.0.0", port=8080)