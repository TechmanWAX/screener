import asyncio
import time
import random
import string
import json
import os

import websockets

from utils.web_requests import async_post, async_get
import utils.data as td


class Alor:
    def __init__(self, assets):
        self.config_file = 'config.json'
        if not os.path.exists(self.config_file):
            raise FileNotFoundError('Config file not found')
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        self.refresh_token = self.config.get('AlorRefresh')
        self.url_trade = 'https://oauth.alor.ru'
        self.uri_wss_api = 'wss://api.alor.ru/ws'
        self.uri_securities = 'https://apidev.alor.ru/md/v2/Securities'
        self.alor_guids = {}
        self.token_timestamp = time.time()
        self.alor_access = None
        self.assets = assets
        self.alor_assets_data_out = {}
        self.alor_assets_data = {}
        self.connected = False

    # get securities from Alor
    async def get_securities(self):
        '''
        Get list of all securities on Alor exchange
        :return:
        '''
        self.securities = await async_get(self.uri_securities, limit=50)

    async def get_access_token(self):
        access_json = await async_post(f'{self.url_trade}/refresh?token={self.refresh_token}')
        self.token_timestamp = time.time()
        self.alor_access = access_json['AccessToken']

    async def add_query_asset(self, assets: list):
        for asset in assets:
            if self.token_timestamp + 60 * 20 >= time.time():
                await self.get_access_token()
            guid = td.trading_data.generate_guid(asset)
            query = {
                "opcode": "OrderBookGetAndSubscribe",
                "code": asset,
                "depth": 10,
                "exchange": "MOEX",
                "format": "Simple",
                "frequency": 1000,
                "guid": guid,
                "token": self.alor_access,
            }

            # print(f"Debug alor query: {query}")
            await self.ws.send(json.dumps(query))

    async def connect(self, assets_moex):
        async with websockets.connect(self.uri_wss_api) as self.ws:
            self.connected = True
            self.assets = asyncio.create_task(self.add_query_asset(assets_moex))
            try:
                while True:
                    response = await self.ws.recv()
                    if await self.parse_assets_out(json.loads(response)):
                        pass
                        # print(f'Debug alor: {response}')
                    else:
                        pass
                        # print(f'Debug alor Error: {response}')
            except websockets.exceptions.ConnectionClosedError:
                self.connected = False

    async def parse_assets(self, data: dict, volume: float | None = None) -> bool:
        '''
        Method parse trading data from alor exchange and save it in self.alor_assets_data
        :param data:
        :param volume:
        :return:
        '''
        # for asset, guid in self.alor_guids.items():
        for asset, guid in td.trading_data.instrument_uid.items():
            if data.get('guid', None) == guid:
                self.alor_assets_data[asset] = data
                # print(f"Debug Alor: {data}")
                return True
        return False

    async def parse_assets_out(self, data: dict) -> bool:
        '''
        Method parse trading data from alor exchange and save it in self.alor_assets_data_out
        :param data:
        :param volume:
        :return:
        '''
        for asset, guid in td.trading_data.guid.items():
            if data.get('guid', None) == guid:
                trading_data = data['data']
                td.trading_data.order_book[guid] = td.OrderBook(
                    instrument_uid=data.get('guid'),
                    bids=trading_data.get('bids', []),
                    asks=trading_data.get('asks', []),
                    timestamp=trading_data.get('timestamp', 0)
                )
