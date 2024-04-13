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
        self.markets = self.markets()
        self.assets = []

        for market in self.markets:
            if market["is_active"]:
                if "pre_launch" in market:
                    if market["pre_launch"]:
                        continue

                self.assets.append(market["instrument_name"])

        loop = asyncio.new_event_loop()
        hourly = np.array(loop.run_until_complete(self.current_funding(loop))) * 100
        loop.close()

        daily = hourly * 24
        annual = daily * 365

        self.current_funding = pd.DataFrame(data=np.array([hourly, daily, annual]).T, index=self.assets, columns=["1hr%", "24hr%", "1yr%"]).sort_values(by=["1hr%"], ascending=False)


    def markets(self, asset="", instrument_type="PERPETUAL"):
        url = f"{AEVO_ENDPOINT}/markets/?asset={asset}&instrument_type={instrument_type}"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        return json.loads(response.text)


    async def current_funding(self, loop):
        headers = {"accept": "application/json"}

        limit_sem = asyncio.Semaphore(20)
        delay = 0.5

        current_funding = []

        responses = await asyncio.gather(*[aio_request(f"{AEVO_ENDPOINT}/funding?instrument_name={asset}", headers, loop, limit_sem, delay=delay) for asset in self.assets])
        
        for response in responses:
            current_funding.append(float(json.loads(response)["funding_rate"]))

        return current_funding
            



# hourly funding payments
# current rate is for the hourly rate
class Dydxv3:
    def __init__(self):
        self.markets = self.markets()["markets"]
        self.assets = [market for market in self.markets.keys()]

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

        




def main():
    aevo = Aevo()
    dydx = Dydxv3()
    hyper = Hyperliquid()

    n = 10

    print(f"\n\n\ntop {n} rates:")
    print("\naevo:\n", aevo.current_funding.head(n))
    print("\ndydx:\n", dydx.current_funding.head(n))
    print("\nhyperliquid:\n", hyper.current_funding.head(n))

    print(f"\n\n\nlowest {n} rates:")
    print("\naevo:\n", aevo.current_funding.tail(n))
    print("\ndydx:\n", dydx.current_funding.tail(n))
    print("\nhyperliquid:\n", hyper.current_funding.tail(n))
    
    # TODO: turn current_funding into dict with column labels being interval for funding, eg col1: 8hour, col2: daily, col3: annual




if __name__ == "__main__":
    main()