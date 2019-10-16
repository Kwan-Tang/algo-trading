import requests,json
from pandas.io.json import json_normalize
from datetime import datetime
from pandas import to_datetime
from pandas import DataFrame
from config import *

base_url = 'https://paper-api.alpaca.markets'
account_url = f'{base_url}/v2/account'
order_url = f'{base_url}/v2/orders'
clock_url = f'{base_url}/v2/clock'
position_url = f'{base_url}/v2/positions'
asset_url = f'{base_url}/v2/assets'
iex_url = 'https://api.iextrading.com/1.0'

def convert_est(d):
    d =  to_datetime(d,unit="ms")
    d = d.dt.tz_localize('GMT').dt.tz_convert('America/New_York').dt.strftime("%Y/%m/%d %I:%M:%S %p")
    return d

def convert_epoch(d):
    if d:
        dt = datetime.strptime(d,"%Y/%m/%d")
        epoch = datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)
    else:
        return None

def send_requests(url,arg,request_='get'):
    if arg:
        url+=f'/{arg}'
    if request_=='get':
        r = requests.get(url=url,headers=alpaca_headers)
        return json.loads(r.content)
    elif request_=='delete':
        r = requests.delete(url=url,headers=alpaca_headers)
        return r

def retrieve_data(endpoint=None,**kwargs):
    payload={}
    payload.update(td_api)
    for key,value in kwargs.items():
        payload[key] = value
    content = requests.get(url=endpoint,params=payload)
    return content.json()

class PaperAccount:
    def __init__(self):
        pass

    def account_info(self):
        print(send_requests(account_url,None))

    def get_orders(self,order_id=None):
        url = order_url
        r = send_requests(url=url,arg=order_id)
        print(json_normalize(r).transpose())

    def cancel_orders(self,order_id=None):
        url = order_url
        r = send_requests(url=url,arg=order_id,request_='delete')
        if r.status_code == 207:
            print('Success!')
        else:
            print("No trades to cancel!")

    def get_assets(self,symbol=None):
        url = asset_url
        r = send_requests(url=url,arg=symbol)
        print(json_normalize(r).transpose())

    def show_positions(self,symbol=None):
        url = position_url
        r = send_requests(url=url,arg=symbol)
        print(json_normalize(r).tranpose())

    def liquidate_positions(self,symbol=None):
        url = position_url
        r = send_requests(url=url,request_='delete',arg=symbol)
        return r

    def replace_order(self,order_id=None,qty=None,time_in_force=None,limit_price=None,stop_price=None):
        url= order_url + "/" + "{}".format(order_id)
        order_info = get_orders(order_id)
        if qty==None:
            qty = order_info['qty']
        if limit_price==None:
            limit_price = order_info['limit_price']
        if time_in_force==None:
            time_in_force= order_info['time_in_force']
        ticket = {
            'qty':qty
            ,'time_in_force': time_in_force
            ,'limit_price':limit_price
            ,'stop_price':stop_price
        }
        r = requests.patch(url,json=ticket,headers=headers)
        return r

    class Stock:
        def __init__(self,symbol):
            self.symbol = symbol

        def trade(self,qty,side,type_='limit',time_in_force='day',limit_price=None,stop_price=None):
            ticket = {
                'symbol':self.symbol.upper()
                ,'qty':qty
                ,'side':side.lower()
                ,'type':type_.lower()
                ,'time_in_force':time_in_force
                ,'limit_price':limit_price
                ,'stop_price':stop_price
            }
            r = requests.post(order_url,json=ticket,headers=alpaca_headers)
            return json.loads(r.content)

        def price_history(self,periodType='day',period=1,frequencyType='minute',frequency=1, endDate=convert_epoch(
                                            datetime.now().date().strftime("%Y/%m/%d")),startDate=None):
            endpoint = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(self.symbol.upper())
            data = retrieve_data(endpoint=endpoint,symbol=symbol,periodType=periodType,period=period,frequencyType=frequencyType,endDate=endDate,startDate=startDate)
            data = json_normalize(data['candles'])
            data.sort_values(by='datetime',ascending=True,inplace=True)
            data['datetime'] = convert_est(data['datetime'])
            data.reset_index(inplace=True,drop=True)
            return data

        def get_quotes(self):
            endpoint = r"https://api.tdameritrade.com/v1/marketdata/quotes"
            quote = retrieve_data(endpoint=endpoint,symbol=self.symbol)
            data = DataFrame(list(quote.values()),index=quote.keys())
            data['regularMarketTradeTimeInLong'] = convert_est(data['regularMarketTradeTimeInLong'])
            data['tradeTimeInLong'] = convert_est(data['tradeTimeInLong'])
            data['quoteTimeInLong'] = convert_est(data['quoteTimeInLong'])
            data = data.transpose()
            return data

        def live_quotes(self):
            url = 'https://api.iextrading.com/1.0/tops/last'
            payload = {'symbols':self.symbol}
            r = requests.get(url=url,params=payload)
            df = json_normalize(json.loads(r.content))
            df['time'] = convert_est(df['time'])
            return df


        def get_fundamentals(self,projection='fundamental'):
            endpoint = r"https://api.tdameritrade.com/v1/instruments"
            data = retrieve_data(endpoint=endpoint,symbol=self.symbol,projection=projection)
            i=0
            for value in data.values():
                if i == 0:
                    df = json_normalize(value['fundamental'])
                else:
                    df = df.append(json_normalize(value['fundamental']).iloc[0])
                i+=1
            df = df.transpose()
            df.columns = data.keys()
            return df

        def get_options(self):
            endpoint = r"https://api.tdameritrade.com/v1/marketdata/chains"
            data = retrieve_data(symbol=self.symbol,endpoint=endpoint)
            return data

    class Market:
        def __init__(self):
            pass

        def get_movers(self,index="$SPX.X",direction=None,change='percent'):
            endpoint = f"https://api.tdameritrade.com/v1/marketdata/{index}/movers"
            data = json_normalize(retrieve_data(endpoint=endpoint,direction=direction,change=change))
            data.sort_values(by='change',ascending=False,inplace=True)
            data['change'] = data['change'].map(lambda x:'{0:.2f}%'.format(x*100))
            data.reset_index(inplace=True,drop=True)
            return data

def main(symbol):
    account = PaperAccount()
    market = account.Market()
    stock = account.Stock(symbol=symbol)
    print(account.account_info())

if __name__ == '__main__':
    
