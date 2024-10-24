import requests
import pandas as pd
from datetime import datetime, timedelta
from slack_notifier import send_text_to_slack

from wh_urls import webhook_url, webhook_error_url, webhook_details_url 

def get_sym_df(pgno=0):
    # Define the API endpoint and headers
    url = 'https://ow-scanx-analytics.dhan.co/customscan/fetchdt'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://dhan.co/',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://dhan.co',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Priority': 'u=4',
        'TE': 'trailers'
    }
    
    data = {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 500,
            "params": [
                {"field": "OgInst", "op": "", "val": "ES"},
                {"field": "Exch", "op": "", "val": "NSE"}
            ],
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue",
                "Year1RevenueGrowth", "NetProfitMargin", "YoYLastQtrlyProfitGrowth",
                "Year1ROCE", "EBIDTAMargin", "volume", "PricePerchng1year",
                "PricePerchng3year", "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild",
                "Eps", "DaySMA50CurrentCandle", "DaySMA200CurrentCandle",
                "DayRSI14CurrentCandle", "Year1ROCE", "Year1ROE", "Sym"
            ],
            "pgno": pgno
        }
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        json_data = response.json()
        
        # Assuming the data you want is in a specific part of the JSON
        # Adjust the following as needed depending on the JSON structure
        if 'data' in json_data:
            items = json_data['data']
            
            # Create a DataFrame from the JSON data
            sym_list_df = pd.DataFrame(items)
            return sym_list_df
        else:
            print("Unexpected JSON structure or missing data")
    else:
        print(f"Failed to retrieve data: {response.status_code}")

def get_df_from_yahoo(sym, days=70, interval='1d', start_date=None, end_date=None):
    try:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}.NS'
        
        if start_date is None:
            today = datetime.today()
            period1_date = today - timedelta(days=days)
        else:
            period1_date = start_date
        
        if end_date is None:
            period2_date = today + timedelta(days=1)
        else:
            period2_date = end_date
        
        period1 = int(period1_date.timestamp())
        period2 = int(period2_date.timestamp())
        
        params = {
            'period1': period1,
            'period2': period2,
            'interval': interval,
            'includePrePost': 'true',
            'events': 'div|split|earn',
            'lang': 'en-US',
            'region': 'US'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://finance.yahoo.com/quote/TATAMOTORS.NS/chart/?guccounter=1',
            'Origin': 'https://finance.yahoo.com',
            'Connection': 'keep-alive'
        }
        
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            result = data['chart']['result'][0]
            # print(result)
            timestamps = result['timestamp']
            indicators = result['indicators']['quote'][0]
            
            df = pd.DataFrame(indicators)
            df['date'] = pd.to_datetime(timestamps, unit='s')
            df['date_string'] = df['date'].dt.strftime('%-d %b %y')
            df.set_index('date', inplace=True)
            return df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - data api error - ' + str(e))
        return None

def get_day_ohlc(sec_id, duration="5d", delay=True, delay_by=15):
    url = 'https://openweb-ticks.dhan.co/getDayOHLC'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://dhan.co/',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://dhan.co',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Priority': 'u=4'
    }
    
    data = {
        "EXCH": "NSE",
        "SEG": "E",
        "INST": "EQUITY",
        "SEC_ID": sec_id,
        "DURATION": duration,
        "Delay": delay,
        "Delayby": delay_by
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return None

def get_dhan_nifty50_data(params=None):
    try:
        url = 'https://ow-scanx-analytics.dhan.co/customscan/fetchdt'
        
        default_data = {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 1000,
            "params": [
                {"field": "idxlist.Indexid", "op": "", "val": "13"},
                {"field": "Exch", "op": "", "val": "NSE"},
                {"field": "OgInst", "op": "", "val": "ES"}
            ],
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth",
                "NetProfitMargin", "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "volume",
                "PricePerchng1year", "PricePerchng3year", "PricePerchng5year", "Ind_Pe",
                "Pb", "DivYeild", "Eps", "DaySMA50CurrentCandle", "DaySMA200CurrentCandle",
                "DayRSI14CurrentCandle", "ROCE", "Roe", "Sym", "PricePerchng1mon", "PricePerchng3mon"
            ],
            "pgno": 1
        }

        if params:
            default_data.update(params)

        request_data = {"data": default_data}

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://dhan.co/',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': 'https://dhan.co',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Priority': 'u=4',
            'TE': 'trailers'
        }

        response = requests.post(url, json=request_data, headers=headers)
        response.raise_for_status()
        return response.json().get('data')
    except requests.RequestException as error:
        print(f"Error fetching data from Dhan API: {error}")
        raise

# Example usage:
# ohlc_data = get_day_ohlc(4963)
# if ohlc_data:
#     print(ohlc_data)
