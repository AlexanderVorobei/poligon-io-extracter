import os
from datetime import datetime, timezone, timedelta
from tracemalloc import start

from dotenv import load_dotenv
from polygon import RESTClient
import pandas as pd


# ENVIRONMENT VARIABLES
load_dotenv()
API_KEY = os.getenv("API_KEY")
FILE_NAME = "data.xlsx"

LIST_LIMIT = 1000
DETAILS_LIMIT = 50000

CONDITIONS = {
    "exchanges": [
        "NASDAQ",
        "NSD",
        "AMEX",
        "AMX",
        "NYSE",
        "NYE",
        "ARCA",
    ],
    "OTC_stocks": [
        "%",
        "%",
        "/",
        "ETF",
        "Bond",
        "Class A",
        "Class B",
        "Class C",
        "Class D",
        "Class F",
        "Series A",
        "Series B",
        "Series C",
        "Series D",
        "Series E",
        "Series F",
        "ordinary shares",
        "mutual funds",
        "mutual fund",
    ],
    "endwith": ".WS",
}


def ts_to_time(ts, delta) -> str:
    to = datetime.fromtimestamp(ts / 1000.0)
    from_ = to - timedelta(minutes=delta)
    return (from_.strftime("%I:%M %p"), to.strftime("%I:%M %p"))


def ts_to_day_count(ts, start) -> str:
    start_date = datetime.strptime(start, "%Y-%m-%d")
    current_date = datetime.fromtimestamp(ts / 1000.0)
    return (current_date - start_date).days


def format_date(date: str):
    return (
        datetime.fromisoformat(date[:-1]).astimezone(timezone.utc).strftime("%Y-%m-%d")
        if date
        else None
    )


def convert_to_excel(file_name: str, sheet_name: str, content):
    xls_data: pd.DataFrame = pd.DataFrame(content)
    if os.path.exists(file_name):
        file_xls_data = pd.DataFrame(pd.read_excel(file_name))
        xls_data = pd.concat([file_xls_data, xls_data])
    xls_data.to_excel(
        file_name,
        sheet_name=sheet_name,
        engine="openpyxl",
        index=False,
    )


def symbol_to_dict(symbol: dict) -> dict:
    return {
        "name": symbol.get("name"),
        "symbol": symbol.get("ticker"),
        "exchange": symbol.get("primary_exchange"),
        "listdate": format_date(symbol.get("last_updated_utc")),
    }


def us_stock_exchange_to_dict(exchange: dict) -> dict:
    return {
        "primary_exchange": exchange.get("mic"),
        "exchange": exchange.get("code"),
    }


def detail_to_dict(ticker: dict, params: dict) -> dict:
    timestamp = ts_to_time(ticker.get("t"), params.get("multiplier"))
    return {
        "day N": f"D{ts_to_day_count(ticker.get('t'), params.get('from_'))}",
        f"Timestamp = {params.get('multiplier')} {params.get('timespan')}s": f"{timestamp[0]} to {timestamp[1]}",
        "Stock symbol": params.get("ticker"),
        "Name of the stock": params.get("name"),
        "Date of extraction": f"D={datetime.now().strftime('%Y-%m-%d')}",
        "Open": f"O={ticker.get('o')}",
        "High": f"H={ticker.get('h')}",
        "Low": f"L={ticker.get('l')}",
        "Close": f"C={ticker.get('c')}",
    }


def filter_ticker_by_exchange(ticker: dict) -> bool:
    exchange: str = ticker.get("exchange")
    if exchange:
        return exchange.lower() in [
            exchange.lower() for exchange in CONDITIONS.get("exchanges")
        ]
    return False


def filter_ticker_by_name(ticker: dict) -> bool:
    name: str = ticker.get("name")
    if name:
        return name.lower() not in [x.lower() for x in CONDITIONS.get("OTC_stocks")]
    return False


def filter_ticker_by_symbol(ticker: dict) -> bool:
    symbol: str = ticker.get("symbol")
    if symbol:
        return symbol.lower() not in [x.lower() for x in CONDITIONS.get("OTC_stocks")]
    return False


def filter_ticker_by_symbol_end(ticker: dict) -> bool:
    symbol: str = ticker.get("symbol")
    if symbol:
        return not symbol.upper().endswith(CONDITIONS.get("endwith"))
    return False


def search_duplicate(symbol: str, tickers: list[dict]):
    for ticker in tickers:
        if ticker.get("symbol") == symbol:
            return True
    return False


def filter_tickers_by_symbol_end(tickers: list[dict]) -> list[dict]:
    result: list[dict] = []
    for ticker in tickers:
        symbol: str = ticker.get("symbol")
        if symbol.lower().endswith("w"):
            if search_duplicate(symbol[:-1], tickers):
                continue
        if symbol.lower().endswith("u"):
            if search_duplicate(symbol[:-1], tickers):
                continue
        if symbol.lower().endswith(".w"):
            if search_duplicate(symbol[:-2], tickers):
                continue
        if symbol.lower().endswith(".u"):
            if search_duplicate(symbol[:-2], tickers):
                continue
        result += [ticker]
    return result


def filtered_tickers(tickers: list[dict]) -> list[dict]:
    return [
        ticker
        for ticker in tickers
        if filter_ticker_by_exchange(ticker)
        and filter_ticker_by_name(ticker)
        and filter_ticker_by_symbol(ticker)
        and filter_ticker_by_symbol_end(ticker)
    ]


def get_tickers():
    query_params = {"active": True, "market": "stocks", "limit": LIST_LIMIT}
    with RESTClient(API_KEY) as client:
        resp = client.reference_tickers_v3(**query_params)
        raw_data = resp.results
        while resp.count == LIST_LIMIT:
            resp = client.reference_tickers_v3(next_url=resp.next_url, **query_params)
            raw_data += resp.results
    return [symbol_to_dict(r) for r in raw_data]


def get_us_stocks_exchanges() -> list[dict]:
    query_params = {"asset_class": "stocks", "locale": "us"}
    with RESTClient(API_KEY) as client:
        resp = client.stocks_equities_exchanges(**query_params)
    return [
        us_stock_exchange_to_dict(exchange.__dict__)
        for exchange in resp.exchange
        if exchange.__dict__.get("mic") and exchange.__dict__.get("code")
    ]


def convert_exchange(symbol: dict, exchanges: list[dict]) -> dict:
    for exchange in exchanges:
        if symbol.get("exchange") == exchange.get("primary_exchange"):
            symbol["exchange"] = exchange.get("exchange")
            return symbol
    return symbol


def get_ticker_detailed(ticker: dict, start: str, end: str) -> list[dict]:
    query_params = {
        "ticker": ticker.get("symbol"),
        "name": ticker.get("name"),
        "multiplier": 30,
        "timespan": "minute",
        "from_": start,
        "to": end,
        "unadjusted": False,
        "limit": DETAILS_LIMIT,
    }
    with RESTClient(API_KEY) as client:
        resp = client.stocks_equities_aggregates(**query_params)
        raw_data: list[dict] = resp.__dict__.get("results")
        while resp.count == LIST_LIMIT:
            resp = client.reference_tickers_v3(next_url=resp.next_url, **query_params)
            raw_data += resp.__dict__.get("results")
    return [detail_to_dict(r, query_params) for r in raw_data]


if __name__ == "__main__":
    raw_tickers = get_tickers()

    exchanges = get_us_stocks_exchanges()
    tickers = [convert_exchange(ticker, exchanges) for ticker in raw_tickers]

    first_stage_filtered = filtered_tickers(tickers)

    second_stage_filtered = filter_tickers_by_symbol_end(first_stage_filtered)

    convert_to_excel("list_0.xlsx", "list 0", second_stage_filtered)

    for ticker in second_stage_filtered:
        ticker_details = get_ticker_detailed(ticker, "2007-05-01", "2022-04-26")
        convert_to_excel("list_1.xlsx", "list 1", ticker_details)
