import requests
import json, os

# ── Configuration ──────────────────────────────────────────────────────────────

# --- ADD THESE 3 LINES TO FORCE THE SOCKS PROXY ---
os.environ['HTTP_PROXY'] = "socks5h://localhost:9090"
os.environ['HTTPS_PROXY'] = "socks5h://localhost:9090"
os.environ['ALL_PROXY'] = "socks5h://localhost:9090"
# --------------------------------------------------

ACCESS_FILE_PATH = os.path.join(os.path.dirname(__file__), "access_token")
DHAN_CLIENT_ID = ""
ACCESS_TOKEN   = ""

with open(ACCESS_FILE_PATH, "r") as f:
    lines = f.readlines()
    if len(lines) >= 2:
        DHAN_CLIENT_ID = lines[0].strip()
        ACCESS_TOKEN = lines[1].strip()
    else:
        raise ValueError("access_token file must contain at least two lines: CLIENT_ID and ACCESS_TOKEN")


BASE_URL       = "https://api.dhan.co/v2"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

# ── 1. Get All Positions ────────────────────────────────────────────────────────
def get_positions():
    """
    Fetch all current positions from Dhan.
    Endpoint: GET /v2/positions
    Returns list of positions with tradingSymbol details.
    """
    url = f"{BASE_URL}/positions"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        positions = response.json()
        return positions
    else:
        print(f"[✗] Failed to fetch positions: {response.status_code} - {response.text}")
        return []


def extract_active_position_symbols(positions):
    """
    Extract trading symbols from positions where net quantity != 0.
    These are considered 'active' positions.
    """
    active_symbols = set()
    for pos in positions:
        net_qty = pos.get("netQty", 0)
        symbol  = pos.get("tradingSymbol", "")
        if net_qty != 0 and symbol:
            active_symbols.add(symbol)

    print(f"\n[✓] Total Active Position Symbols: {len(active_symbols)}")
    return active_symbols


# ── 2. Get All Super Orders ─────────────────────────────────────────────────────
def get_super_orders():
    """
    Fetch all super orders placed today.
    Endpoint: GET /v2/super/orders
    Returns list of super orders with legDetails.
    """
    url = f"{BASE_URL}/super/orders"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        super_orders = response.json()
        return super_orders
    else:
        print(f"[✗] Failed to fetch super orders: {response.status_code} - {response.text}")
        return []


def extract_active_super_orders(super_orders):
    """
    Filter super orders that are in PENDING or PART_TRADED state (active/cancellable).
    """
    active_statuses = {"PENDING", "PART_TRADED", 'TRADED'}
    
    active_orders   = set()

    st_set = set()

    for order in super_orders:
        status = order.get("orderStatus", "")
        symbol = order.get("tradingSymbol", "")
        oid= order.get('orderId','')

        if status in {"PENDING", "PART_TRADED"}:
            active_orders.add((symbol,oid,'ENTRY_LEG'))


        if status in active_statuses:
            legdetails = order.get('legDetails')
            for ord_leg in legdetails:
                ord_st = ord_leg.get('orderStatus','')
                if ord_st  in ('PENDING'):
                    active_orders.add((symbol,oid,ord_leg.get('legName','')))

    print(f"\n[✓] Total Active Super Orders: {len(active_orders)}")
    return active_orders


# ── 3. Cancel Super Order (Entry Leg cancels all legs) ─────────────────────────
def cancel_super_order(order_id, order_leg="ENTRY_LEG"):
    """
    Cancel a super order by order_id.
    Endpoint: DELETE /v2/super/orders/{order-id}/{order-leg}

    Cancelling ENTRY_LEG cancels ALL legs of the super order.
    Returns True on success, False on failure.
    """
    url = f"{BASE_URL}/super/orders/{order_id}/{order_leg}"

    response = requests.delete(url, headers=HEADERS)

    if response.status_code in (200, 202):
        result = response.json() if response.text else {}
        print(f"  [✓] Cancelled Super Order: {order_id} | Status: {result.get('orderStatus', 'CANCELLED')}")
        return True
    else:
        print(f"  [✗] Failed to cancel Order {order_id}: {response.status_code} - {response.text}")
        return False


# ── 4. Main Logic ───────────────────────────────────────────────────────────────
def cancel_pending_superorders():
    """
    Main workflow:
    1. Fetch all positions → extract active symbols
    2. Fetch all super orders → filter active (PENDING/PART_TRADED)
    3. Cancel super orders whose tradingSymbol is NOT in active positions
    """

    
    positions       = get_positions()
    active_symbols  = extract_active_position_symbols(positions)

    all_super_orders    = get_super_orders()
    active_super_orders = extract_active_super_orders(all_super_orders)

    cancelled_symb = []

    for (symb,oid, legname) in active_super_orders:
        if symb not in active_symbols:
            if(cancel_super_order(oid,legname)):
                cancelled_symb.append(symb)
    
    print(f'Cancelled Super order Count : {len(cancelled_symb)}')
    print(f'Cancelled Symbols {cancelled_symb}')
    

if __name__ == "__main__":
    cancel_pending_superorders()


            

