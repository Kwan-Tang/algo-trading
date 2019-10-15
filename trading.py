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

def send_requests(url,arg,request_='get'):
    if arg:
        url+=f'/{arg}'
    if request_=='get':
        r = requests.get(url=url,headers=alpaca_headers)
        return json.loads(r.content)
    elif request_=='delete':
        r = requests.delete(url=url,headers=alpaca_headers)
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

def stock(symbol):
    account = PaperAccount()
    account.get_assets('ICSH')

if __name__ == '__main__':
    stock('BAC')
