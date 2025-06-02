import asyncio
import os
import json

from PySide6.QtCore import QObject, Signal

from tasks.calculate import Calculate, convert_tv_to_moex_features, safe_float_convert
from tasks.investing import Investing
from tasks.alor import Alor
from tasks.forex import AsyncFixClient as ForexClient


class Worker(QObject):
    finished = Signal()
    data_received_USDRUB = Signal(str, bool)
    data_received_market_fields = Signal(dict)
    update_fields = Signal(dict)

    def __init__(self):
        super().__init__()
        self.loop = None
        self.tasks = []
        self.running = False

        self.assets_file = 'assets.json'
        self.assets = self.load_assets()
        self.stocks = self.assets[0]     # ['TATN-TATNP', 'MTLR-MTLRP', 'SBER-SBERP']
        self.features = self.assets[1]   # {'ED1!': 1000, 'EURUSD': 100000, 'SV1!': 10, 'XAGUSD': 5000 'GD1!': 1, 'XAUUSD': 100}
        self.features_subst = {'SV': 'SILV', 'GD': 'GOLD', 'NA': 'NASD', 'SF': 'SPYF', 'PT': 'PLT', 'PD': 'PLD'}
        self.assets_moex = self.parse_assets_by_one()   # ['TATN', 'TATNP', 'MTLR', 'MTLRP', 'SBER', 'SBERP', 'ED-3.25', 'SILV-3.25', 'GOLD-3.25']
        self.investing = Investing()
        self.forex = ForexClient()
        self.alor = Alor(self.assets_moex)
        self.calculate = Calculate(self.alor.alor_assets_data, self.forex.md, self.features_subst, self.features)

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.task_manager())
        except Exception as e:
            print(f'Error in CalculateWorker: {e}')
        finally:
            self.loop.run_until_complete(self.shutdown())  # Ensure cleanup
            self.loop.close()

    async def task_manager(self):
        """Start async tasks and keep them running."""
        self.running = True
        self.tasks = [
            asyncio.create_task(self.fetch_data_usdrub()),
            asyncio.create_task(self.fetch_data_market_fields()),
            asyncio.create_task(self.investing.wss_connect()),
            asyncio.create_task(self.alor.connect(self.assets_moex)),
            asyncio.create_task(self.forex.start())
        ]
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            print("Tasks cancelled during shutdown.")
        finally:
            self.finished.emit()  # Signal that tasks are done

    async def fetch_data_usdrub(self) -> None:
        """Simulated async task that runs continuously."""
        try:
            while self.running:
                self.data_received_USDRUB.emit(f'{str(self.investing.USDRUB)}', self.investing.connected)
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("fetch_data_usdrub was cancelled.")
        finally:
            print("fetch_data_usdrub exited.")

    async def fetch_data_market_fields(self) -> None:
        """Simulated async task that runs continuously."""
        try:
            while self.running:
                if self.alor.connected:
                    self.data_received_market_fields.emit(f'{str(self.calculate.data)}')
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("fetch_data_market_fields was cancelled.")
        finally:
            print("fetch_data_market_fields exited.")

    async def shutdown(self):
        """Gracefully stops all running tasks before closing the event loop."""
        if not self.running:
            return  # Prevent duplicate shutdown calls

        print("Shutting down CalculateWorker...")
        self.running = False

        # Cancel all running tasks
        for task in self.tasks:
            task.cancel()

        # Wait for all tasks to finish
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.CancelledError:
            print("Some tasks were forcefully cancelled.")

        print("CalculateWorker stopped.")

    def stop(self):
        """Triggers the shutdown process safely from the main thread."""
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self.shutdown(), self.loop)

    def load_assets(self) -> tuple:
        '''
        Load assets from file to self variables.
        :return:
        '''
        if not os.path.exists(self.assets_file):
            print('Config file not found')
            return ()
        try:
            with open(self.assets_file, 'r') as f:
                all_assets = json.load(f)
                STOCKS = all_assets['STOCKS']                                # Stocks on prop
                FEATURES_FOREX = all_assets['FEATURES_FOREX']                # Stocks prop + features forex
                FEATURES_PROP = all_assets['FEATURES_PROP']                  # Long and short features on prop
                FEATURES_PROP_STOCKS = all_assets['FEATURES_PROP_STOCKS']    # Features and stocks on prop
                return (STOCKS, FEATURES_FOREX, FEATURES_PROP, FEATURES_PROP_STOCKS)
        except json.JSONDecodeError:
            print("JSON file format error")
            return ()

    # Parse input dict and return list of stocks and features in MOEX format
    def parse_assets_by_one(self) -> list:
        assets = []
        for stock in self.stocks:       # ['TATN-TATNP', 'MTLR-MTLRP', 'SBER-SBERP']
            st = stock.split('-')
            for i in st:
                assets.append(i)
        for feature in self.features:   # {'ED1!/EURUSD': {'ED1!': 1000, 'EURUSD': 100000}, 'SV1!/XAGUSD': {'SV1!': 10, 'XAGUSD': 5000}, 'GD1!/XAUUSD': {'GD1!': 1, 'XAUUSD': 100}}
            fe = feature.split('/')
            for i in fe:
                if i[-2:-1].isdigit() and i[-1:] == '!':
                    assets.append(convert_tv_to_moex_features(self.features_subst, i))
                    # assets.append(convert_tv_to_moex_features(self.features_subst, feature))
        return assets                   # ['TATN', 'TATNP', 'MTLR', 'MTLRP', 'SBER', 'SBERP', 'ED-3.25', 'SILV-3.25', 'GOLD-3.25']
