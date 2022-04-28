import os
import itertools
from datetime import datetime
from urllib.parse import urljoin
from typing import Union

from dotenv import load_dotenv
import pandas as pd
from pydantic import BaseModel
import requests


load_dotenv()

API_KEY = os.environ.get("API_KEY", None)
API_HOST = os.environ.get("API_HOST", None)
LIST_ENDPOINT = "stocks/list"
MAX_SYMBOL_COUNT = 10000
PAGE_SIZE = 100


class Exchange(BaseModel):
    name: Union[str, None]


class Symbol(BaseModel):
    listDate: Union[datetime, None]
    ticker: Union[str, None]
    fullTicker: Union[str, None]
    name: Union[str, None]
    exchange: Exchange


class QueryParams(BaseModel):
    apiKey: str
    page: int
    pageSize: int


class Results(BaseModel):
    status: str
    results: list[Symbol]
    count: int


def get_symbols() -> list[Symbol]:
    assert API_KEY
    assert API_HOST

    url: str = urljoin(API_HOST, LIST_ENDPOINT)
    params: QueryParams = QueryParams(apiKey=API_KEY, page=0, pageSize=PAGE_SIZE)
    symbols: list[Symbol] = []
    for index in range(0, MAX_SYMBOL_COUNT, PAGE_SIZE):
        params.page = index // PAGE_SIZE + 1
        response = requests.get(
            url,
            params=params.dict(),
        )
        response.raise_for_status()
        result: Results = Results.parse_obj(response.json())
        symbols += result.results
        if result.count < PAGE_SIZE:
            break
    return symbols


def filter_symbol(symbols: list[Symbol]):
    result: list[Symbol] = []
    pre_result: list[Symbol] = []
    conditions = {
        "exchange": [
            "NASDAQ",
            "AMEX - American Exchange (AMX)",
            "NYSE (NYE)",
            "Arca",
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
        "endwith": [".ws"],
    }
    for symbol in symbols:
        if (
            symbol.exchange.name in conditions["exchange"]
            and symbol.name.lower() not in [x.lower() for x in conditions["OTC_stocks"]]
            and not symbol.name.endswith(".ws")
            and not symbol.exchange.name.endswith(".ws")
        ):
            pre_result += [symbol]
    uniq_result: list[Symbol] = [
        g.next() for k, g in itertools.groupby(pre_result, lambda x: x["fullTicker"])
    ]
    for symbol in uniq_result:
        if symbol.ticker.endswith("W") or symbol.ticker.endswith("U"):
            if symbol.ticker[:-1] in [k.ticker for k in uniq_result]:
                continue
        result += [symbol]
    return result


def to_xlsx(file_path: str, content: list[Symbol], sheet_name: str):
    xls_data: pd.DataFrame = pd.DataFrame(content.dict())
    xls_data.to_excel(
        file_path,
        sheet_name=sheet_name,
        engine="openpyxl",
        index=False,
    )
