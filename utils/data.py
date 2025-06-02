import random, string
from dataclasses import dataclass
from typing import List


@dataclass
class Order:
    price: float
    volume: float


@dataclass
class OrderBook:
    instrument_uid: str
    bids: List[Order]
    asks: List[Order]
    timestamp: int

class TradingData:
    def __init__(self):
        self.guid = {}
        self.order_book = {}

    def generate_guid(self, asset) -> str:
        while True:
            # Generate a new GUID
            guid = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8)) + '-' + \
                   ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' + \
                   ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' + \
                   ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' + \
                   ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))

            # Check if the GUID is unique
            if guid not in self.guid:
                self.guid[asset] = guid
                return guid

trading_data = TradingData()
