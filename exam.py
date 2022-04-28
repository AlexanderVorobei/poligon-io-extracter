import os
from datetime import datetime
from urllib.parse import urljoin
from typing import Union

from dotenv import load_dotenv

from pydantic import BaseModel
import requests


load_dotenv()

API_KEY = os.environ.get("API_KEY", None)
API_HOST = os.environ.get("API_HOST", None)
LIST_ENDPOINT = "stocks/list"
MAX_SYMBOL_COUNT = 300
PAGE_SIZE = 100


class SymbolLogo(BaseModel):
    size: int
    extension: str
    filename: str
    contentType: str
    webPath: str
    webPathname: str
    width: int
    height: int


class SymbolIndustry(BaseModel):
    name: str


class Exchange(BaseModel):
    name: str
    # assetType: "CommonStock"
    operatingMic: str
    country: str
    currencyCode: str


class Price(BaseModel):
    close: float
    high: float
    low: float
    open: float
    volume: float
    time: datetime


class Symbol(BaseModel):
    logo: Union[SymbolLogo, None]
    # address: str
    # cik: str
    locale: str
    marketCap: float
    sharesOutstandingLast: float
    # phoneNumber: str
    # description: str
    # employees: int
    delisted: bool
    listDate: Union[datetime, None]
    # website: str
    industry: SymbolIndustry
    ticker: str
    fullTicker: str
    # assetType: "CommonStock"
    name: str
    exchange: Exchange
    lastPrice: Union[Price, None]


class ListQueryParam(BaseModel):
    apiKey: str
    page: int
    pageSize: int


class ListResult(BaseModel):
    status: str
    results: list[Symbol]
    count: int


def get_symbols() -> list[Symbol]:
    assert API_KEY
    assert API_HOST

    url = urljoin(API_HOST, LIST_ENDPOINT)
    params = ListQueryParam(apiKey=API_KEY, page=0, pageSize=PAGE_SIZE)
    symbols: list[Symbol] = []
    for index in range(0, MAX_SYMBOL_COUNT, PAGE_SIZE):
        params.page = index // PAGE_SIZE + 1
        response = requests.get(
            url,
            params=params.dict(),
        )
        response.raise_for_status()
        result: ListResult = ListResult.parse_obj(response.json())
        symbols += result.results
        if result.count < PAGE_SIZE:
            break
    return symbols
