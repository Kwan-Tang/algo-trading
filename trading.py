import requests
from pandas.io.json import json_normalize
from datetime import datetime,date,timedelta
from pandas import to_datetime,DataFrame
from math import sqrt,acos,pi
import matplotlib.pyplot as plt
from time import sleep
from pytz import timezone
from dotenv import load_dotenv
from os import getenv

load_dotenv()
alpaca_headers = {'APCA-API-KEY-ID':getenv("APCA-API-KEY-ID"), 'APCA-API-SECRET-KEY':getenv("APCA-API-SECRET-KEY")}
td_api = {'apikey':getenv('apikey')}
eastern = timezone('America/New_York')
base_url = 'https://paper-api.alpaca.markets'
account_url = f'{base_url}/v2/account'
order_url = f'{base_url}/v2/orders'
clock_url = f'{base_url}/v2/clock'
position_url = f'{base_url}/v2/positions'
asset_url = f'{base_url}/v2/assets'
iex_url = 'https://api.iextrading.com/1.0'

def convert_est(d):
    d =  to_datetime(d,unit="ms")
    d = d.dt.tz_localize('GMT').dt.tz_convert('America/New_York')
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
        r = requests.get(url=url,headers=alpaca_headers).json()
    elif request_=='delete':
        r = requests.delete(url=url,headers=alpaca_headers).json()
    return r

def retrieve_data(endpoint=None,**kwargs):
    payload={}
    payload.update(td_api)
    for key,value in kwargs.items():
        payload[key] = value
    content = requests.get(url=endpoint,params=payload).json()
    return content

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
        print(r)

    def get_assets(self,symbol=None):
        url = asset_url
        r = send_requests(url=url,arg=symbol)
        print(json_normalize(r).transpose())

    def show_positions(self,symbol=None):
        url = position_url
        r = send_requests(url=url,arg=symbol)
        print(json_normalize(r).transpose())

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
        r = requests.post(order_url,json=ticket,headers=alpaca_headers).json()
        return r

    def price_history(self,periodType='day',period=1,frequencyType='minute',frequency=1, endDate=convert_epoch(
                                        datetime.now().date().strftime("%Y/%m/%d")),startDate=None):
        endpoint = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(self.symbol.upper())
        data = retrieve_data(endpoint=endpoint,symbol=self.symbol,periodType=periodType,period=period,frequencyType=frequencyType,endDate=endDate,startDate=startDate)
        data = DataFrame(data['candles'])
        data['datetime'] = convert_est(data['datetime'])
        data.sort_values(by='datetime',ascending=True,inplace=True)
        data.set_index('datetime',inplace=True)
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
        r = requests.get(url=url,params=payload).json()
        df = DataFrame(r)
        df['time'] = convert_est(df['time'])
        df.set_index('time',drop=True,inplace=True)
        return df

    def get_fundamentals(self,projection='fundamental'):
        endpoint = r"https://api.tdameritrade.com/v1/instruments"
        data = retrieve_data(endpoint=endpoint,symbol=self.symbol,projection=projection)
        data = list(data.values())[0]['fundamental']
        df = DataFrame([data])
        return df.transpose()

    def get_options(self):
        endpoint = r"https://api.tdameritrade.com/v1/marketdata/chains"
        data = retrieve_data(symbol=self.symbol,endpoint=endpoint)
        return data

    def run_algo(self,t=480):
        print("*******Algo has started.  Good luck!*******")
        df = DataFrame()
        angles_df = DataFrame()
        account = PaperAccount()
        for i in range(t):
            df = df.append(self.live_quotes())
            if len(df) >=480:
                a,b,c = self.abc(df)
                calc = self.find_angles(a,b,c)
                calc['time'] = df.tail(1).index
                calc.set_index('time',inplace=True,drop=True)
                angles_df = angles_df.append(calc)
                if calc[3][0] == 0:
                    self.trade(qty=100,side='buy',type_='market')
                    p1 = self.live_quotes().price[0]
                    for i in range(120):
                        p2 = self.live_quotes().price[0]
                        if abs(p2 - p1)>=0.04:
                            self.trade(qty=100,side='sell',type_='market')
                            break
                        sleep(1)
                    if account.show_positions() != None:
                        self.trade(qty=100,side='sell',type_='market')
            if len(df) >=500:
                df=DataFrame()
            sleep(1)
        return angles_df
        print("*******Algo has ended.  Good bye!*******")

    def angles(self,ab,bc,ac):
        angle_c = acos((ab**2+bc**2-ac**2)/(2*ab*bc))*180/pi
        angle_b = acos((ab**2+ac**2-bc**2)/(2*ab*ac))*180/pi
        angle_a = acos((ac**2+bc**2-ab**2)/(2*ac*bc))*180/pi
        return angle_a,angle_b,angle_c

    def lengths_xy(self,a,b,c):
        ab = sqrt((b[0]-a[0])**2+(b[1]-a[1])**2)
        bc = sqrt((c[0]-b[0])**2+(c[1]-b[1])**2)
        ac = sqrt((c[0]-a[0])**2+(c[1]-a[1])**2)
        return ab,bc,ac

    def abc(self,df):
        curr_time = datetime.now().astimezone(eastern)
        c = df.tail(1).price[0]
        b = df[df.index>=curr_time-timedelta(minutes=4)]
        a = df[df.index>=curr_time-timedelta(minutes=6)]
        return (2,a.price[0]),(4,b.price[0]),(8,c)

    def find_angles(self,a,b,c):
        a1,b1,c1 = self.lengths_xy(a,b,c)
        angles_=DataFrame()
        if a1+b1<=c1 or a1+c1<=b1 or b1+c1<=a1:
            angles_= angles_.append([a[1],b[1],c[1],0])
        else:
            angles_ = angles_.append([a[1],b[1],c[1],float(self.angles(a1,b1,c1)[2])])
        return angles_.transpose()

class Market:
    def __init__(self):
        pass

    def get_movers(self,index="$SPX.X",direction=None,change='percent'):
        endpoint = f"https://api.tdameritrade.com/v1/marketdata/{index}/movers"
        data = DataFrame(retrieve_data(endpoint=endpoint,direction=direction,change=change))
        data.sort_values(by='change',ascending=False,inplace=True)
        data['change'] = data['change'].map(lambda x:'{0:.2f}%'.format(x*100))
        data.reset_index(inplace=True,drop=True)
        return data

def main(symbol):
    account = PaperAccount()
    market = Market()
    stock = Stock(symbol=symbol)
    df = stock.run_algo()
    return df

if __name__ == '__main__':
    df = main('SPY')
