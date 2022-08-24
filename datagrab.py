import os
import pandas as pd
import glob

from collect.types.trading import ListingId
from collect.listing_store.utils import is_linear, is_inverse
from matplotlib import pyplot as plt
from pandas import DataFrame
from pickle_listing_store import PickleStore

location_input = r'/Users/fedja-ii/PycharmProjects/py_first_at/results' #location result files
store = PickleStore(r'/Users/fedja-ii/PycharmProjects/datagrab/listing_store.pickle')
file_output = 'total_results.csv' # name output file

fee_maker_bps = -1 # for calculation fees
fee_taker_bps = 5 # for calculation fees
b_larger = 100 # number full production times larger in current pair
b_coins = 5 # full production number of pairs
f_hedge = 'DERIBTC-USD' # hedge name for splitting with and without hedging


class style():
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'


def list_files(location):
    return glob.glob(os.path.join(location, "*.csv.gz")) + glob.glob(os.path.join(location, "*.csv"))


def main():
    data = {'date': [],
            'pair': [],
            'coin': [],
            'profit_ex_cost': [],
            'volume_maker': [],
            'volume_taker': [],
            'volume': [],
            'fee_maker': [],
            'fee_taker': [],
            'fee_total': [],
            'profit': [],
            'margin': []

            }

    df = pd.DataFrame(data)
    df_a = pd.DataFrame()
    files = list_files(location_input)

    for file in files:
        try:
            file_1 = file.rsplit("/", 1)[1]
            coin = file_1.split("_")[2]
            l = store.lookup(ListingId.from_str(coin))

            contract_size = l.contract_size
            inv = is_inverse(l)
            df_a = pd.read_csv(file)
            df_a.columns = ['datetime', 'name', 'mic', 'feedcode', 'side', 'price', 'size', 'b7', 'b8', 'b9', 'profit',
                            'b11', 'takertrade']

            df_tr = df_a.query("name == 'trade'")

            df_tr = df_tr.astype({'size': 'float64'})
            df_tr = df_tr.astype({'price': 'float64'})
            df_tr['size'] = abs(df_tr['size'])

            df_tr['size_l'] = 0
            df_tr = df_tr.astype({'size_l': 'float64'})

            df_tr['size_l'] = df_tr['size']

            if not inv:
                df_tr.loc[df_tr['side'] == 'Sell', 'size_l'] = df_tr['size'] * df_tr['price']
                df_tr.loc[df_tr['side'] == 'Buy', 'size_l'] = df_tr['size'] * df_tr['price']

            df_tr['size_l'] = df_tr['size_l'] * float(contract_size)

            df_tr['volume_maker_raw'] = 0
            df_tr = df_tr.astype({'volume_maker_raw': 'float64'})
            df_tr.loc[df_tr['takertrade'] != 'True', 'volume_maker_raw'] = df_tr['size_l']

            df_tr['volume_taker_raw'] = 0
            df_tr = df_tr.astype({'volume_taker_raw': 'float64'})
            df_tr.loc[df_tr['takertrade'] == "True", 'volume_taker_raw'] = df_tr['size_l']

            df_tr['volume_maker_cum_raw'] = df_tr['volume_maker_raw'].cumsum()
            volume_maker = df_tr.iloc[-1]['volume_maker_cum_raw']

            df_tr['volume_taker_cum_raw'] = df_tr['volume_taker_raw'].cumsum()
            volume_taker = df_tr.iloc[-1]['volume_taker_cum_raw']

            df_it = df_a.query("name == 'interval'")
            profit = df_it.iloc[-1][10]
            file_1 = file.rsplit("/", 1)[1]
            date_run = file_1.split("_")[1]
            coin = file_1.split("_")[2]
            pair = (file_1.split("_")[5] + "_" + file_1.split("_")[6]).split(".")[0]

            volume_t = volume_maker + volume_taker
            fee_maker = volume_maker * fee_maker_bps / 10000
            fee_taker = volume_taker * fee_taker_bps / 10000
            fee_total = fee_maker + fee_taker
            if inv:
                price_new = df_it.iloc[-1][9]
                profit = profit* price_new
            profit_fee = profit - fee_total
            margin = profit_fee / volume_t * 10000
            new_row = {'date': date_run, 'pair': pair, 'coin': coin[:-5], 'profit_ex_cost': profit, 'volume_maker': volume_maker, 'volume_taker': volume_taker, 'volume': volume_t, 'fee_maker': fee_maker, 'fee_taker': fee_taker, 'fee_total': fee_total, 'profit': profit_fee, 'margin': margin}
            df = df.append(new_row, ignore_index=True)
        except:
            print("CSV contains no usable data")

    df.to_csv(file_output)

    df.sort_values(by=['date'], inplace=True)

    print(df)
    df.to_csv(file_output)

    total_pnl_tot = round(df["profit"].sum(), 2)
    avg_a_pnl_tot = round(total_pnl_tot / df.shape[0], 2)
    total_vo_tot = round(df["volume"].sum(), 2)
    avg_a_vo_tot = round(total_vo_tot / df.shape[0], 2)
    total_monthly_reach_tot = round(avg_a_pnl_tot * 30 * b_larger * b_coins / 1000, 2)
    total_monthly_vl_tot = round(avg_a_vo_tot * 30 * b_larger * b_coins / 1000000, 2)
    total_fees_tot =  round(df["fee_total"].sum(), 2)
    avg_a_fees_tot = round(total_fees_tot / df.shape[0], 2)
    total_monthly_fees_tot = round(avg_a_fees_tot * 30 * b_larger * b_coins / 1000, 2)

    df_c2 = df.query("coin != '" + f_hedge + "'")
    total_pnl = round(df_c2["profit"].sum(), 2)
    avg_a_pnl = round(total_pnl / df_c2.shape[0], 2)
    total_vo = round(df_c2["volume"].sum(), 2)
    avg_a_vo = round(total_vo / df_c2.shape[0], 2)
    total_monthly_reach = round(avg_a_pnl * 30 * b_larger * b_coins / 1000, 2)
    total_monthly_vl = round(avg_a_vo * 30 * b_larger * b_coins / 1000000, 2)
    total_fees = round(df["fee_total"].sum(), 2)
    avg_a_fees = round(total_fees / df.shape[0], 2)
    total_monthly_fees = round(avg_a_fees * 30 * b_larger * b_coins / 1000, 2)

    print("")
    print("")
    print(style.WHITE + "\t \t \t \t \t \t" + "Results total" + "\t" + "Results no hedging")
    print(style.GREEN + "Total Profit" + "\t \t \t \t \t" + str(total_pnl_tot) + "\t \t \t \t" + str(total_pnl))
    print("AVG Profit /c /d" + "\t \t \t \t" + str(avg_a_pnl_tot) + "\t \t \t \t" + str(avg_a_pnl))
    print("Total Profit monthly k " + "\t \t \t" + str(total_monthly_reach_tot) + "\t \t \t \t" + str(total_monthly_reach))
    print(style.CYAN + "Total Fee'" + "\t \t \t \t \t \t" + str(total_fees_tot) + "\t \t \t \t" + str(total_fees))
    print("AVG Fee /c /d" + "\t \t \t \t \t" + str(avg_a_fees_tot) + "\t \t \t \t" + str(avg_a_fees))
    print("Total Fee monthly k" + "\t \t \t \t" + str(total_monthly_fees_tot) + "\t \t \t \t" + str(total_monthly_fees))
    print(style.YELLOW + "Total Volume" + "\t \t \t \t \t" + str(total_vo_tot) + "\t \t \t" + str(total_vo))
    print("AVG Volume /c /d" + "\t \t \t \t" + str(avg_a_vo_tot) + "\t \t \t" + str(avg_a_vo))
    print("Total Volume monthly M " + "\t \t \t" + str(total_monthly_vl_tot) + "\t \t \t \t" + str(total_monthly_vl))
    print(style.WHITE + "Monthly total is" + str(b_larger) + " times larger and for " + str(b_coins) + " coins ")

    df_a1 = df.query("coin != '" + f_hedge + "'")
    df_a1 = df_a1.reset_index(drop=True)

    df_a2 = df.query("coin == '" + f_hedge + "'")
    df_a2 = df_a2.reset_index(drop=True)

    plt.subplot(2, 1, 1)
    y2Points = df_a1['profit'].cumsum()
    y3Points = df_a2['profit'].cumsum()
    plt.plot(DataFrame({'trade': y2Points, 'hedge': y3Points}))
    plt.ylabel('profit')
    plt.legend(["trade", "hedge"])

    plt.subplot(2, 1, 2)
    y2Points = df_a1['volume'].cumsum()
    y3Points = df_a2['volume'].cumsum()
    plt.plot(DataFrame({'trade': y2Points, 'hedge': y3Points}))
    plt.ylabel('volume')
    plt.legend(["trade", "hedge"])
    plt.show()

if __name__ == '__main__':
    main()
