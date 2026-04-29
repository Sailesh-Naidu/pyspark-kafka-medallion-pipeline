import random
from datetime import datetime, timedelta

traders = [f"T{str(i).zfill(4)}" for i in range(1, 1001)]
heavy_traders = traders[:10]

symbols = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
    "META", "NFLX", "NVDA", "AMD", "INTC",
    "ORCL", "IBM", "ADBE", "CRM", "UBER",
    "LYFT", "SHOP", "SQ", "PYPL", "BABA",
    "TCS", "INFY", "WIPRO", "HCLTECH", "LTIM",
    "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK",
    "AXISBANK", "BAJFINANCE", "ITC", "HINDUNILVR", "ASIANPAINT",
    "MARUTI", "TATAMOTORS", "SUNPHARMA", "DRREDDY", "CIPLA",
    "ONGC", "COALINDIA", "NTPC", "POWERGRID", "BPCL",
    "IOC", "ADANIENT", "ADANIPORTS", "ZOMATO", "SWIGGY"
]


def inject_late_data(trade):
    event_time = datetime.fromisoformat(trade["trade_timestamp"])
    r = random.random()

    if r < 0.60:
        pass
    elif r < 0.85:
        event_time = event_time - timedelta(seconds=random.randint(10, 60))
    elif r < 0.95:
        event_time = event_time - timedelta(minutes=random.randint(2, 5))
    else:
        event_time = event_time - timedelta(minutes=random.randint(10, 15))
    trade["trade_timestamp"] = event_time.isoformat()
    return trade

trade_history = []

def inject_duplicates(trade):
    global trade_history

    r = random.random()
    if len(trade_history) > 100:
        trade_history.pop(0)

    # 5% exact duplicate
    if r < 0.05 and trade_history:
        dup = random.choice(trade_history)
        return dup

    # 5% modified duplicate
    elif r < 0.10 and trade_history:
        dup = random.choice(trade_history).copy()

        # modify some fields but keep same trade_id
        dup["price"] = round(random.uniform(100, 1000), 2)
        dup["quantity"] = random.randint(1, 1000)
        dup["trade_timestamp"] = datetime.utcnow().isoformat()

        return dup

    # otherwise normal flow
    trade_history.append(trade)
    return trade

def inject_bad_data(trade):

    r = random.random()
    t = trade.copy()
    if r < 0.03:
        field_to_remove = random.choice([
            "price", "quantity", "symbol", "trader_id"
        ])
        t.pop(field_to_remove, None)
        return t
    elif r < 0.06:
        field_to_corrupt = random.choice([
            "price", "quantity", "trade_timestamp"
        ])
        if field_to_corrupt == "price":
            t["price"] = str(t.get("price", ""))   # string instead of float
        elif field_to_corrupt == "quantity":
            t["quantity"] = str(t.get("quantity", ""))  # string instead of int
        else:
            t["trade_timestamp"] = "INVALID_TIMESTAMP"

        return t
    elif r < 0.08:
        field_to_null = random.choice([
            "price", "quantity", "symbol"
        ])
        t[field_to_null] = None
        return t

    elif r < 0.10:
        t["symbol"] = "INVALID_XYZ"
        return t
    return t

def apply_skew():
    if random.random() < 0.7:
        return random.choice(heavy_traders)
    else:
        non_heavy_traders = [t for t in traders if t not in heavy_traders]
        return random.choice(non_heavy_traders)

def generate_trade():
    trader = apply_skew()
    symbol = random.choice(symbols)
    trade = {
        "trade_id": f"TRD{random.randint(100000, 999999)}",
        "trader_id": trader,
        "symbol": symbol,
        "price": round(random.uniform(100, 1000), 2),
        "quantity": random.randint(1, 1000),
        "trade_timestamp": datetime.utcnow().isoformat(),
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "exchange": random.choice(["NYSE", "NASDAQ", "BINANCE"]),
        "side": random.choice(["BUY", "SELL"]),
        "metadata": {
            "source": random.choice(["mobile", "web", "api"]),
            "version": 1
        }
    }
    return trade