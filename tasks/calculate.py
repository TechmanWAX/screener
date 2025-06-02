from datetime import datetime, timedelta


class Calculate:
    def __init__(self, moex_trading_data: dict, forex_trading_data: dict, features_subst: dict, features: dict):
        '''

        :param moex_trading_data: Trading data from alor broker
        :param forex_trading_data: Trading data from Xpbee
        :param features_subst: dict of features to substitute {'SV': 'SILV', 'GD': 'GOLD'}
        :param features: a dictionary containing futures and their lot size {"ED1!/EURUSD": {"ED1!": 1000, "EURUSD": 100000}, "SV1!/XAGUSD": {"SV1!": 10, "XAGUSD": 5000}, "GD1!/XAUUSD": {"GD1!": 1, "XAUUSD": 100}},
        '''
        # self.data = {'usdrub': '83.4508', 'payout': '7', 'mc_stocks': '118000', 'mc_features': '236000', 'order1': {'asset': 'ED1!/EURUSD', 'lot1': '10', 'price1': '1.03', 'lot2': '1', 'price2': '1.04', 'ent_av': '990.38', 'cur_av': '', 'exit': '', 'profit': '', 'mc': '0.7472'}, 'order2': {'asset': 'SV1!/XAGUSD', 'lot1': '-75', 'price1': '31.54', 'lot2': '0.15', 'price2': '31.164', 'ent_av': '1012.07', 'cur_av': '', 'exit': '', 'profit': '', 'mc': '27.76932'}, 'order3': {'asset': 'MTLR-MTLRP', 'lot1': '-100', 'price1': '101', 'lot2': '100', 'price2': '100', 'ent_av': '1.0', 'cur_av': '-4.27', 'exit': '', 'profit': '', 'mc': '591.0'}, 'order4': {'asset': 'TATN-TATNP', 'lot1': '100', 'price1': '735', 'lot2': '-100', 'price2': '715', 'ent_av': '20.0', 'cur_av': '45.1', 'exit': '', 'profit': '', 'mc': '-570.0'}, 'order5': {'asset': 'TATN-TATNP', 'lot1': '', 'price1': '', 'lot2': '', 'price2': '', 'ent_av': '', 'cur_av': '', 'exit': '', 'profit': '', 'mc': ''}, 'order6': {'asset': 'TATN-TATNP', 'lot1': '', 'price1': '', 'lot2': '', 'price2': '', 'ent_av': '', 'cur_av': '', 'exit': '', 'profit': '', 'mc': ''}}
        self.data_number = 6
        self.data = {}
        self.usd = 0
        self.payout = 0
        self.mc_stocks = 0
        self.mc_features = 0
        self.features_subst = features_subst
        self.features = features
        self.moex_trading_data = moex_trading_data
        self.forex_trading_data = forex_trading_data

    def start(self, stocks):
        '''
        :param stocks: ['TATN-TATNP', 'MTLR-MTLRP', 'SBER-SBERP']
        :return:
        '''
        self.usd = safe_float_convert(self.data['usdrub'])
        self.payout = safe_float_convert(self.data['payout'])
        self.mc_stocks = safe_float_convert(self.data['mc_stocks'])
        self.mc_features = safe_float_convert(self.data['mc_features'])
        for order in self.data['orders']:
            if self.check_fields(order):    # skip empty orders
                # assets_type -1 error, 0 - Moex stocks-stocks, 1 - Moex stocks/features, 2 - Moex features/Forex features
                assets_type = self.check_arb_type(order['asset'])
                if assets_type:
                    # Convert all necessary strings in order to Float
                    convert_order(order)
                    # Calculate full positions
                    positions = self.calc_position(order, assets_type)
                    order['position1'] = positions[0]
                    order['position2'] = positions[1]
                    # Calculate enter average
                    order['ent_av'] = self.calc_av([order['price1'], order['price2'], assets_type[2]])
                    if not order['price1'] and not order['price2'] or order['price1'] == 0 and order['price2'] == 0:
                        order['lot1'] = -order['lot2']
                        order['lot2'] = -order['lot2']
                    prices = self.get_prices(order, assets_type)
                    order['cur_price1'] = prices[0]
                    order['cur_price2'] = prices[1]
                    # Calculate current average
                    order['cur_av'] = self.calc_av([prices[0], prices[1], assets_type[2]])
                    # Calculate margin call level
                    self.calc_mc(order, assets_type)
                    # Calculate profit
                    order['profit'] = self.calc_profit(order, assets_type)
                    # print(f"Debug start: {order}")

        return self.data

    # Calculate profit
    def calc_profit(self, order, assets_type: list):
        position1 = order.get('position1')
        position2 = order.get('position2')
        ent_av = order.get('ent_av')
        cur_av = order.get('cur_av')
        ent_price1 = order.get('price1')
        ent_price2 = order.get('price2')
        cur_price1 = order.get('cur_price1')
        cur_price2 = order.get('cur_price2')
        profit = ''
        exit_flag = False
        if order.get('exit'):
            cur_av = safe_float_convert(order.get('exit'))
            exit_flag = True
        if position1 and (ent_av and cur_av) or (position2 and ent_price1 and ent_price2 and cur_price1 and cur_price2):
            if assets_type[2] == 0:
                profit = round((cur_av - ent_av) * position1 * (100 - self.payout) / 100, 2)
            elif assets_type[2] == 2:
                if not exit_flag:
                    first_leg = (cur_price1 - ent_price1) * position1
                    if first_leg > 0:
                        first_leg *= round((100 - self.payout) / 100, 2)
                    second_leg = (cur_price2 - ent_price2) * position2
                    profit = round((first_leg + second_leg) * self.usd, 2)
                else:
                    if cur_av != 0:
                        profit = -(ent_av / cur_av * 100 - 100)
                        profit = round(profit * position1 * ent_price1 / 100 * self.usd, 2)
        return profit

    # Calculate margin call level
    def calc_mc(self, order: dict, assets_type: list):
        '''
        :param order:   order string. Data from form line per line
        :param assets_type: list or assets and it's type
        :return: None
        '''
        if assets_type[2] == 0:
            order = self.calc_mc_stocks(order)
        elif assets_type[2] == 2:
            order = self.calc_mc_features_prop_forex(order)
        pass

    def calc_mc_stocks(self, order: dict) -> dict:
        position1 = order.get('position1')
        position2 = order.get('position2')
        ent_av = order.get('ent_av')
        if position1 and position2 and ent_av:
            try:
                mc = self.mc_stocks
                positions = abs(position1) + abs(position2)
                if position1 > 0 > position2:
                    order['mc'] = round(ent_av - (mc / positions), 2)
                elif position1 < 0 < position2:
                    order['mc'] = round(ent_av + (mc / positions), 2)
            except Exception as e:
                print('Error in calc_mc_stocks', e)
        return order

    # calculate margin call for features
    def calc_mc_features_prop_forex(self, order: dict,) -> dict:
        price1 = order.get('price1')
        position1 = order.get('position1')
        # If we have price, position, margincall level and USDRUB
        if price1 and position1 and self.usd and self.mc_features:
            try:
                if position1 > 0:
                    mc = price1 + self.mc_features / position1 / self.usd
                elif position1 < 0:
                    mc = price1 - self.mc_features / position1 / self.usd
                else:
                    mc = 0
                mc = round(mc, 2)
                order['mc'] = mc
            except Exception as e:
                print('Error in calc_mc_features_prop_forex', e)
        return order

    # calculate full positions lot * asset position
    def calc_position(self, order, assets_type):
        position1 = ''
        position2 = ''
        if order.get('lot1'):
            position1 = order.get('lot1')
        if order.get('lot2'):
            position2 = order.get('lot2')
        positions = []
        asset_type = assets_type[2]
        if asset_type == 1 or asset_type == 2:    # Calculate for Moex/Forex features
            assets = self.features.get(order['asset'])
            position1 = round(assets[assets_type[0]] * position1, 3)
            position2 = round(assets[assets_type[1]] * position2, 3)
        positions.append(position1)
        positions.append(position2)
        return positions

    # Return price from the stock book
    def get_prices(self, order, assets) -> list:
        prices = []
        lots1 = order['lot1']
        lots2 = order['lot2']
        if assets[-1] == 2:
            assets[0] = convert_tv_to_moex_features(self.features_subst, assets[0])
        cur_price1 = self.moex_trading_data.get(assets[0])
        if not assets[-1]:
            cur_price2 = self.moex_trading_data.get(assets[1])
        else:
            cur_price2 = self.forex_trading_data.get(assets[1])
        if cur_price1:
            cur_price1 = cur_price1.get('data')
            if cur_price1:
                if lots1 < 0:
                    cur_price1 = cur_price1.get('asks')
                else:
                    cur_price1 = cur_price1.get('bids')
                cur_price1 = self.get_best_price(cur_price1, lots1)
        if cur_price2:
            cur_price2 = cur_price2.get('data')
            if cur_price2:
                if lots2 < 0:
                    cur_price2 = cur_price2.get('asks')
                else:
                    cur_price2 = cur_price2.get('bids')
                cur_price2 = self.get_best_price(cur_price2, lots2)
        return [cur_price1, cur_price2]

    # Get the best price from the list of prices dicts
    def get_best_price(self, prices, lots):
        cur_lots = 0
        for price in prices:
            cur_lots += price.get('volume')
            if cur_lots >= lots:
                return price.get('price')
            return price.get('price')

    # Calculate average
    def calc_av(self, prices: list) -> float | None:
        '''
        :param prices: list of two prices
        :param assets_type:
        :return:
        '''
        price1 = safe_float_convert(prices[0])
        price2 = safe_float_convert(prices[1])
        assets_type = prices[2]
        if assets_type == 0:
            return round(price1 - price2, 2)
        if price2 == 0:
            return 0
        if assets_type == 1 or assets_type == 2:
            return round(price1 / price2, 5)
        return None

    # Check the type of arbitrage
    @staticmethod
    def check_arb_type(assets: str) -> list | bool:
        '''
        :param assets: 'ED1!/EURUSD'
        :return: False if error, or list [asset1, asset2, assets_type] where assets_type
        0 - Moex stocks-stocks, 1 - Moex stocks/features, 2 - Moex features/Forex features
        '''
        if assets.endswith('USD'):
            assets = assets.split('/')
            assets.append(2)
            return assets
        if '/' in assets:
            assets = assets.split('/')
            assets.append(1)
            return assets
        if '-' in assets:
            assets = assets.split('-')
            assets.append(0)
            return assets
        return False

    # check if necessary field is filled
    @staticmethod
    def check_fields(order):
        # if order['price1'] and order['price2'] and order['lot1'] and order['lot2']:
        if order['lot1'] and order['lot2']:
            return True
        order['cur_av'] = ''
        order['ent_av'] = ''
        order['profit'] = ''
        order['mc'] = ''
        return False


# safe convert from string to float
def safe_float_convert(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        # print(f"Conversion error for value: {value}")
        return default


# Convert names from Tradingview to Moex format
def convert_tv_to_moex_features(features_subst, tradingview_code: str) -> str:
    '''
    Convert features names from TradingView format (SV1!) to Moex format (SILV-3.25)
    :param tradingview_code:
    :return:
    '''
    if not tradingview_code[-2:-1].isdigit() and not tradingview_code[-1:] == '!':
        raise ValueError("Invalid TradingView code format")

    # quarter_offset = int(tradingview_code[-2:-1]) - 1  # Convert 1! -> 0, 2! -> 1
    if tradingview_code[:-2] in features_subst.keys():
        new_code = features_subst[tradingview_code[:-2]]
    else:
        new_code = tradingview_code[:-2]

    current_date = datetime.now()
    # current_date = datetime(2025, 3, 20, hour=19)
    # Define quarter end months (March, June, September, December)
    quarter_ends = [3, 6, 9, 12]

    # Find the current quarter's end month
    current_month = current_date.month
    current_year = current_date.year
    next_quarter_end = min([m for m in quarter_ends if m >= current_month], default=3)
    # If we're in December, move to next year's March
    if next_quarter_end == 3 and current_month > 3:
        current_year += 1

    # Set target date to 3rd of the quarter's last month
    target_date = datetime(current_year, next_quarter_end,1)
    # Skip 2 first weeks
    target_date = target_date + timedelta(weeks=2)
    # Find the next Thursday
    while target_date.weekday() != 3:  # 3 represents Thursday (0 = Monday)
        target_date += timedelta(days=1)
    # If this date is before current date, move to next quarter
    if target_date < current_date or (target_date == current_date and current_date.hour >= 19):
        # Calculate months to add (3 months forward)
        new_month = next_quarter_end + 3
        new_year = current_year
        if new_month > 12:
            new_month -= 12
            new_year += 1
        target_date = datetime(new_year, new_month, 1)
        # Skip 2 weeks
        target_date = target_date + timedelta(weeks=2)
        # Find next Thursday
        while target_date.weekday() != 3:
            target_date += timedelta(days=1)
    return f"{new_code}-{target_date.month}.{str(target_date.year)[2:]}"


# Safe convert necessary  strings from order to floats
def convert_order(order):
    lot1 = order.get('lot1')
    price1 = order.get('price1')
    lot2 = order.get('lot2')
    price2 = order.get('price2')
    order['lot1'] = safe_float_convert(lot1)
    order['price1'] = safe_float_convert(price1)
    order['lot2'] = safe_float_convert(lot2)
    order['price2'] = safe_float_convert(price2)
