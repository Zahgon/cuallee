import xml.etree.ElementTree as ET
import requests
from dataclasses import dataclass
import os
from operator import attrgetter as at
from functools import lru_cache
from typing import Dict


@dataclass
class Currency:
    country_name: str
    currency_name: str
    currency: str = None
    currency_number: str = None
    currency_units: str = None


@lru_cache
def _load_currencies():
    pass


@lru_cache
def _load_countries():
    pass


class ISO:

    CCY_CODE = "currency"
    CCY_NUMBER = "currency_number"
    COUNTRY_CODE = "country"
    COUNTRY_NAME = "name"

    def __init__(self, check):
        self._check = check
        self._ccy = []
        self._countries = []

    def iso_4217(
        self,
        column: str,
        pct: float = 1.0,
        options: Dict[str, str] = {"name": "iso_4217"},
    ):
        pass

    def iso_3166(
        self,
        column: str,
        pct: float = 1.0,
        options: Dict[str, str] = {"name": "iso_3166"},
    ):
        pass

    iso_currencies = iso_4217
    iso_countries = iso_3166
