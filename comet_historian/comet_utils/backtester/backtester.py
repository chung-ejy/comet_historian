import pandas as pd
from datetime import datetime, timedelta
from comet_utils.processor.processor import Processor as p
from time import sleep
from comet_utils.analyzer.entry_strategy import EntryStrategy
from comet_utils.analyzer.exit_strategy import ExitStrategy


class Backtester(object):

    @classmethod
    def backtest(self,start,end,params,prices):
        status = "loads"
        symbols = params["symbols"]
        rt = params["retrack_days"]
        s = params["signal"]
        r = params["req"]
        value = params["value"]
        conservative = params["conservative"]
        entry_strat = params["entry_strategy"]
        exit_strat = params["exit_strategy"]
        market = prices.pivot_table(index="date",columns="crypto",values="close").reset_index()
        market = p.column_date_processing(market)
        market = market.fillna(method="ffill")
        sim = market.melt(id_vars="date").copy()
        ns = []
        for crypto in [x.lower() for x in symbols]:
            crypto_sim = sim[sim["crypto"]==crypto].copy()
            crypto_sim.sort_values("date",inplace=True)
            crypto_sim["signal"] = crypto_sim["value"].pct_change(rt)
            crypto_sim["velocity"] = crypto_sim["signal"].pct_change(rt)
            crypto_sim["inflection"] = crypto_sim["velocity"].pct_change(rt)
            crypto_sim["p_sign_change"] = [row[1]["velocity"] * row[1]["inflection"] < 0 for row in crypto_sim.iterrows()]
            ns.append(crypto_sim)
        final = pd.concat(ns)
        final = final[(final["date"] < end)]
        signal = float(s/100)
        req = float(r/100)
        date = start
        trades = []
        while date < end:
            try:
                status = "entries"
                offerings = EntryStrategy.backtest_entry_analysis(date,entry_strat,final,signal,value,conservative)
                if offerings.index.size < 1:
                    date = date + timedelta(days=1)
                else:
                    status = "exits"
                    trade = offerings.iloc[0]
                    trade = ExitStrategy.backtest_exit_analysis(exit_strat,final,trade,rt,req)
                    trade["signal"] = signal
                    trade["req"] = req
                    trade["retrack_days"] = rt
                    trade["value"] = value
                    trade["conservative"] = conservative
                    trade["entry_strategy"] = entry_strat
                    trade["exit_strategy"] = exit_strat
                    trades.append(trade)
                    status = "date adding"
                    date = trade["sell_date"] + timedelta(days=1)
            except Exception as e:
                print(date,status,str(e))
                date = date + timedelta(days=1)

        return pd.DataFrame(trades)