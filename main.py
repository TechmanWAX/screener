import os
import asyncio
import time
import json

from tasks.alor import Alor
from tasks.forex import AsyncFixClient as ForexClient


assets_file = 'assets.json'
all_assets = ''
tasks = []
features_subst = {'SV': 'SILV', 'GD': 'GOLD', 'NA': 'NASD', 'SF': 'SPYF', 'PT': 'PLT', 'PD': 'PLD'}
assets_moex = ['TATN', 'TATNP', 'MTLR', 'MTLRP', 'SBER', 'SBERP', 'ED-3.25', 'SILV-3.25', 'GOLD-3.25']

forex = ForexClient()
alor = Alor(assets_moex)


def load_assets() -> tuple:
    if not os.path.exists(assets_file):
        print(f'Config file {assets_file} not found')
        return ()
    try:
        with open(assets_file, 'r') as f:
            all_assets = json.load(f)
            STOCKS = all_assets['STOCKS']  # Stocks on prop
            FEATURES_FOREX = all_assets['FEATURES_FOREX']  # Stocks prop + features forex
            FEATURES_PROP = all_assets['FEATURES_PROP']  # Long and short features on prop
            FEATURES_PROP_STOCKS = all_assets['FEATURES_PROP_STOCKS']  # Features and stocks on prop
            return (STOCKS, FEATURES_FOREX, FEATURES_PROP, FEATURES_PROP_STOCKS)
    except json.JSONDecodeError:
        print("JSON file format error")
        return ()


assets = load_assets()
stocks = assets[0]  # ['TATN-TATNP', 'MTLR-MTLRP', 'SBER-SBERP']
features = assets[1]  # {'ED1!': 1000, 'EURUSD': 100000, 'SV1!': 10, 'XAGUSD': 5000 'GD1!': 1, 'XAUUSD': 100}


async def shutdown():
    # Cancel all running tasks
    for task in tasks:
        task.cancel()

    # Wait for all tasks to finish
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        print("Some tasks were forcefully cancelled.")

    print("Screener stopped.")


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(task_manager())
    except Exception as e:
        print(f'Error in CalculateWorker: {e}')
    finally:
        loop.run_until_complete(shutdown())  # Ensure cleanup
        loop.close()


async def task_manager():
    tasks = [
        asyncio.create_task(alor.connect(assets_moex)),
        asyncio.create_task(forex.start())
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("Tasks cancelled during shutdown.")
    while True:
        await asyncio.sleep(1)


if __name__ == '__main__':
    main()
