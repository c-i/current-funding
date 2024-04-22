import requests
import json
import asyncio
import aiohttp
import logging
import pandas as pd
import numpy as np




AEVO_ENDPOINT = "https://api.aevo.xyz"
DYDX_ENDPOINT = "https://api.dydx.exchange"
HYPER_ENDPOINT = "https://api.hyperliquid.xyz"

LOGGER_FORMAT = "%(asctime)s %(message)s"
logging.basicConfig(format=LOGGER_FORMAT, datefmt="[%H:%M:%S]")
log = logging.getLogger()
log.setLevel(logging.INFO)




async def aio_request(url, headers, loop, limit_sem, delay=0.1, post=False, payload=None):
    async with limit_sem:
        await asyncio.sleep(delay)

        async with aiohttp.ClientSession(loop=loop) as session:
            if post:
                async with session.post(url, json=payload, headers=headers) as response:
                    status = response.status
                    log.info(f"{url}: {status}")

                    return await response.text()

            else:
                async with session.get(url, headers=headers) as response:
                    status = response.status
                    log.info(f"{url}: {status}")

                    return await response.text()


# hourly funding payments
# current rate is for the hourly rate
class Aevo:
    def __init__(self):
        self.name = "aevo"
        self.markets = self.markets()
        aevo_assets = []

        for market in self.markets:
            if market["is_active"]:
                if "pre_launch" in market:
                    if market["pre_launch"]:
                        continue
                
                aevo_assets.append(market["instrument_name"])

        loop = asyncio.new_event_loop()
        hourly = np.array(loop.run_until_complete(self.current_funding(aevo_assets, loop))) * 100
        loop.close()

        self.assets = []
        for asset in aevo_assets:
            self.assets.append(asset.split("-")[0])

        daily = hourly * 24
        annual = daily * 365

        self.current_funding = pd.DataFrame(data=np.array([hourly, daily, annual]).T, index=self.assets, columns=["1hr%", "24hr%", "1yr%"]).sort_values(by=["1hr%"], ascending=False)


    def markets(self, asset="", instrument_type="PERPETUAL"):
        url = f"{AEVO_ENDPOINT}/markets/?asset={asset}&instrument_type={instrument_type}"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        return json.loads(response.text)


    async def current_funding(self, assets, loop):
        headers = {"accept": "application/json"}

        limit_sem = asyncio.Semaphore(20)
        delay = 0.5

        current_funding = []

        responses = await asyncio.gather(*[aio_request(f"{AEVO_ENDPOINT}/funding?instrument_name={asset}", headers, loop, limit_sem, delay=delay) for asset in assets])
        
        for response in responses:
            current_funding.append(float(json.loads(response)["funding_rate"]))

        return current_funding
            



# hourly funding payments
# current rate is for the hourly rate
class Dydxv3:
    def __init__(self):
        self.name = "dydx_v3"
        self.markets = self.markets()["markets"]
        dydx_assets = [market for market in self.markets.keys()]
        self.assets = []
        for asset in dydx_assets:
            self.assets.append(asset.split("-")[0])

        rates = []
        for market in self.markets:
            rate = float(self.markets[market]["nextFundingRate"])
            rates.append([rate, rate * 2400, rate * 2400 * 365])

        rates_array = np.array(rates)

        self.current_funding = pd.DataFrame(data=rates_array, index=self.assets, columns=["1hr%", "24hr%", "1yr%"]).sort_values(by=["1hr%"], ascending=False)
    
    
    def markets(self):
        url = f"{DYDX_ENDPOINT}/v3/markets"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        return json.loads(response.text)

    



# hourly funding payments
# current rate is for the hourly rate
class Hyperliquid:
    def __init__(self):
        self.name = "hyperliquid"
        self.markets = self.markets()
        self.assets = [market["name"] for market in self.markets[0]["universe"]]
        funding_dict = {}

        rates = []
        for i, asset in enumerate(self.assets):
            rate = float(self.markets[1][i]["funding"])
            rates.append([rate, rate * 2400, rate * 2400 * 365])

        rates_array = np.array(rates)

        self.current_funding = pd.DataFrame(data=rates_array, index=self.assets, columns=["1hr%", "24hr%", "1yr%"]).sort_values(by=["1hr%"], ascending=False)


    def markets(self):
        url = f"{HYPER_ENDPOINT}/info"
        headers = {"content-type": "application/json"}

        payload = {"type": "metaAndAssetCtxs"}

        response = requests.post(url, json=payload, headers=headers)

        return json.loads(response.text)




def differenced_rates(*exchanges):
    differences = []
    for i, exchange in enumerate(exchanges):
        rate_s = exchange.current_funding["1hr%"]
        j = i + 1
        while j < len(exchanges):
            next_rate_s = exchanges[j].current_funding["1hr%"]
            difference_s = rate_s.sub(next_rate_s, axis="index").dropna()
            difference_s.name = f"{exchange.name}-{exchanges[j].name}"
            differences.append(difference_s)
            j += 1

    return differences




def best_differences(differences):
    indices_set = set()
    for series in differences:
        indices_set.update(set(series.index))

    best_diff_df = pd.DataFrame(0.0, index=list(indices_set), columns=["difference", "exchange"])
    best_diff_df["exchange"] = ""

    for diff_s in differences:
        for index in diff_s.index:
            if abs(diff_s.loc[index]) > abs(best_diff_df.loc[index, "difference"]):
                best_diff_df.loc[index, "difference"] = diff_s.loc[index]
                best_diff_df.loc[index, "exchange"] = diff_s.name

    return best_diff_df.sort_values(by="difference", ascending=False)
            



def main():
    aevo = Aevo()
    dydx = Dydxv3()
    hyper = Hyperliquid()
    print(len(aevo.assets))
    print(len(dydx.assets))
    print(len(hyper.assets))

    differences = differenced_rates(aevo, dydx, hyper)
    print(differences, "\n\n\n")
    best_diff_df = best_differences(differences)
    print(best_diff_df)

    n = 10

    print(f"\n\n\ntop {n} rates:")
    print("\naevo:\n", aevo.current_funding.head(n))
    print("\ndydx:\n", dydx.current_funding.head(n))
    print("\nhyperliquid:\n", hyper.current_funding.head(n))

    print(f"\n\n\nlowest {n} rates:")
    print("\naevo:\n", aevo.current_funding.tail(n))
    print("\ndydx:\n", dydx.current_funding.tail(n))
    print("\nhyperliquid:\n", hyper.current_funding.tail(n))
    
    


if __name__ == "__main__":
    main()