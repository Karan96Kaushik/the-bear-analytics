File content
Pasted content

20.21 KB • 604 extracted lines

Formatting may be inconsistent from source.
import requests
import pandas as pd

SEND_NOTIF = True

def analyse_data(df, sym):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        # for i in range()[-1:]:
        if True:
            i = len(df) - 1
            # 
            if pd.notna(df.iloc[i]['44_day_ma']):  # Ensure the MA value is not NaN

                if  df.iloc[i-1]['44_day_ma'] < df.iloc[i]['44_day_ma'] and \
                    df.iloc[i-2]['44_day_ma'] < df.iloc[i-1]['44_day_ma'] and \
                    df.iloc[i-3]['44_day_ma'] < df.iloc[i-2]['44_day_ma'] and \
                    df.iloc[i-4]['44_day_ma'] < df.iloc[i-3]['44_day_ma'] and \
                    \
                    ( abs(df.iloc[i]['44_day_ma'] - df.iloc[i]['low']) < (df.iloc[i]['44_day_ma']*0.01) or \
                        ( df.iloc[i]['44_day_ma'] > df.iloc[i]['low'] and df.iloc[i]['44_day_ma'] < df.iloc[i]['high'] ) ) and \
                    \
                    ( df.iloc[i]['close'] > df.iloc[i]['open'] or \
                      (df.iloc[i]['high'] - df.iloc[i]['close']) < (df.iloc[i]['close'] - df.iloc[i]['low']) ):
                    
                    events.append(df.iloc[i-2])
                    events.append(df.iloc[i-1])
                    events.append(df.iloc[i])

        if len(events) > 0:
            events_df = pd.DataFrame(events)
            return events_df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - processing error - ' + str(e))
        # send_text_to_slack(webhook_error_url, sym + ' ' + str(e))
        print(e, sym)

        return None


def analyse_data_downward(df, sym):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        # for i in range()[-1:]:
        if True:
            i = len(df) - 1
            # 
            if pd.notna(df.iloc[i]['44_day_ma']):  # Ensure the MA value is not NaN

                if  df.iloc[i-1]['44_day_ma'] > df.iloc[i]['44_day_ma'] and \
                    df.iloc[i-2]['44_day_ma'] > df.iloc[i-1]['44_day_ma'] and \
                    df.iloc[i-3]['44_day_ma'] > df.iloc[i-2]['44_day_ma'] and \
                    df.iloc[i-4]['44_day_ma'] > df.iloc[i-3]['44_day_ma'] and \
                    \
                    ( abs(df.iloc[i]['44_day_ma'] - df.iloc[i]['high']) < (df.iloc[i]['44_day_ma']*0.01) or \
                        ( df.iloc[i]['44_day_ma'] > df.iloc[i]['low'] and df.iloc[i]['44_day_ma'] < df.iloc[i]['high'] ) ) and \
                    \
                    ( df.iloc[i]['close'] < df.iloc[i]['open'] or \
                      (df.iloc[i]['high'] - df.iloc[i]['close']) > (df.iloc[i]['close'] - df.iloc[i]['low']) ):
                    
                    events.append(df.iloc[i-2])
                    events.append(df.iloc[i-1])
                    events.append(df.iloc[i])

        if len(events) > 0:
            events_df = pd.DataFrame(events)
            return events_df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - processing error - ' + str(e))
        # send_text_to_slack(webhook_error_url, sym + ' ' + str(e))
        print(e, sym)

        return None
    
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_df_from_yahoo(sym):
    try:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}.NS'
        
        today = datetime.today()
        period1_date = today - timedelta(days=70)
        period2_date = today #- timedelta(days=2)
        
        period1 = int(period1_date.timestamp())
        period2 = int(period2_date.timestamp())

        # print(period2)
        
        params = {
            'period1': period1,
            'period2': period2,
            'interval': '1d',
            'includePrePost': 'true',
            'events': 'div|split|earn',
            'lang': 'en-US',
            'region': 'US'
        }
        
        # Headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://finance.yahoo.com/quote/TATAMOTORS.NS/chart/?guccounter=1',
            'Origin': 'https://finance.yahoo.com',
            'Connection': 'keep-alive'
        }
        
        # Make the request
        response = requests.get(url, params=params, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Extract the results data
            result = data['chart']['result'][0]
            # print('----***----')
            # print(result)
            # print('--------')
            timestamps = result['timestamp']
            indicators = result['indicators']['quote'][0]
            
            # Create a DataFrame from the results
            df = pd.DataFrame(indicators)
            df['date'] = pd.to_datetime(timestamps, unit='s')
            # df['date_string'] = df['date'].dt.strftime('%Y-%m-%d')
            df['date_string'] = df['date'].dt.strftime('%-d %b %y')
            # df['date_string'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df.set_index('date', inplace=True)
            return df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - data api error - ' + str(e))
        # print(sym, '--------------', e)


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
        
# from google.oauth2 import service_account
# from googleapiclient.discovery import build

# # Path to the service account key file
# SERVICE_ACCOUNT_FILE = '/Users/karankaushik/Documents/gitworkspace/EV-Charging-DRL/top-glass-226920-ff9fb14e6f4f.json'

# # Define the scopes required for the Sheets API
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# # Authenticate using the service account credentials
# credentials = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# # Build the Sheets API service
# service = build('sheets', 'v4', credentials=credentials)

# # Define the new spreadsheet details
# spreadsheet = {
#     'properties': {
#         'title': 'My New Spreadsheet'
#     }
# }

# # Create the new spreadsheet
# request = service.spreadsheets().create(body=spreadsheet)
# response = request.execute()

# # Print the URL of the newly created spreadsheet
# print(f"Spreadsheet created: {response.get('spreadsheetUrl')}")
# print(f"Spreadsheet ID: {response.get('spreadsheetId')}")


# from google.oauth2 import service_account
# from googleapiclient.discovery import build

# SERVICE_ACCOUNT_FILE = '/Users/karankaushik/Documents/gitworkspace/EV-Charging-DRL/top-glass-226920-ff9fb14e6f4f.json'

# SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
#           'https://www.googleapis.com/auth/drive']

# credentials = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# service = build('sheets', 'v4', credentials=credentials)


# permissions = {
#     'type': 'user',
#     'role': 'writer',  
#     'emailAddress': 'karankaushik69@gmail.com'  
# }

# spreadsheet_id = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'

# permission_request = drive_service.permissions().create(
#     fileId=spreadsheet_id,
#     body=permissions,
#     fields='id'
# )
# permission_response = permission_request.execute()

# print(f"Permission ID: {permission_response.get('id')}")

from google.oauth2 import service_account
from googleapiclient.discovery import build
import numpy as np

SERVICE_ACCOUNT_FILE = 'top-glass-226920-ff9fb14e6f4f.json'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)

SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'
READ_RANGE = 'Sheet1!A1:DZ5000'  # Adjust the range as needed

# Function to read data from the sheet
def read_sheet_data():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=READ_RANGE).execute()
    return result.get('values', [])

# Function to update a specific cell
def update_cell(row, col, value):
    range_name = f'Sheet1!{chr(65 + col)}{row + 1}'  # Convert col index to letter
    body = {
        'values': [[value]]
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_name,
        valueInputOption='RAW', body=body).execute()
    return result

def bulk_update_cells(updates):
    """
    :param updates: A list of dictionaries, each with 'range' and 'values'.
                    Example: [{'range': 'Sheet1!A1', 'values': [['New Value']]},
                              {'range': 'Sheet1!B2', 'values': [['Another Value']]}]
    """
    body = {
        'valueInputOption': 'RAW',
        'data': updates
    }
    
    # Perform the bulk update
    result = service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()
    
    return result


def update_stock(stock, date, price, volume):
    row = list(row_headers).index(stock)
    # print(row)
    col = list(col_headers).index(date)
    col_price = col
    col_vol = col+1

    update_cell(row, col_price, price)
    update_cell(row, col_vol, volume)

def get_stock_loc(stock, date):
    row = list(row_headers).index(stock)
    # print(row)
    # print(col_headers)
    col = list(col_headers).index(date)
    col_price = col
    col_vol = col+1

    return row, col_price

def number_to_excel_column(n):
    column = ""
    n+=1
    while n > 0:
        n -= 1  # Adjust for 1-based index
        column = chr(n % 26 + 65) + column
        n //= 26
    return column
    
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_df_from_yahoo(sym):
    try:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}.NS'
        
        today = datetime.today()
        period1_date = today - timedelta(days=70)
        period2_date = today #- timedelta(days=2)
        
        period1 = int(period1_date.timestamp())
        period2 = int(period2_date.timestamp())

        # print(period2)
        
        params = {
            'period1': period1,
            'period2': period2,
            'interval': '1d',
            'includePrePost': 'true',
            'events': 'div|split|earn',
            'lang': 'en-US',
            'region': 'US'
        }
        
        # Headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://finance.yahoo.com/quote/TATAMOTORS.NS/chart/?guccounter=1',
            'Origin': 'https://finance.yahoo.com',
            'Connection': 'keep-alive'
        }
        
        # Make the request
        response = requests.get(url, params=params, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Extract the results data
            result = data['chart']['result'][0]
            # print('----***----')
            # print(result)
            # print('--------')
            timestamps = result['timestamp']
            indicators = result['indicators']['quote'][0]
            
            # Create a DataFrame from the results
            df = pd.DataFrame(indicators)
            df['date'] = pd.to_datetime(timestamps, unit='s')
            # df['date_string'] = df['date'].dt.strftime('%Y-%m-%d')
            df['date_string'] = df['date'].dt.strftime('%-d %b %y')
            # df['date_string'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df.set_index('date', inplace=True)
            return df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - data api error - ' + str(e))
        # print(sym, '--------------', e)
        # 
        # def analyse_data(df, sym):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        # for i in range()[-1:]:
        if True:
            i = len(df) - 1
            # 
            if pd.notna(df.iloc[i]['44_day_ma']):  # Ensure the MA value is not NaN

                if  df.iloc[i-1]['44_day_ma'] < df.iloc[i]['44_day_ma'] and \
                    df.iloc[i-2]['44_day_ma'] < df.iloc[i-1]['44_day_ma'] and \
                    df.iloc[i-3]['44_day_ma'] < df.iloc[i-2]['44_day_ma'] and \
                    df.iloc[i-4]['44_day_ma'] < df.iloc[i-3]['44_day_ma'] and \
                    \
                    ( abs(df.iloc[i]['44_day_ma'] - df.iloc[i]['low']) < (df.iloc[i]['44_day_ma']*0.01) or \
                        ( df.iloc[i]['44_day_ma'] > df.iloc[i]['low'] and df.iloc[i]['44_day_ma'] < df.iloc[i]['high'] ) ) and \
                    \
                    ( df.iloc[i]['close'] > df.iloc[i]['open'] or \
                      (df.iloc[i]['high'] - df.iloc[i]['close']) < (df.iloc[i]['close'] - df.iloc[i]['low']) ):
                    
                    events.append(df.iloc[i-2])
                    events.append(df.iloc[i-1])
                    events.append(df.iloc[i])

        if len(events) > 0:
            events_df = pd.DataFrame(events)
            return events_df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - processing error - ' + str(e))
        # send_text_to_slack(webhook_error_url, sym + ' ' + str(e))
        print(e, sym)

        return None
        
import json

webhook_url = "https://hooks.slack.com/services/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"
webhook_error_url = "https://hooks.slack.com/services/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"
webhook_details_url = "https://hooks.slack.com/services/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"

def concise_json_to_slack_blocks(text, json_data):
    blocks = []
    
    # Header block for the overview
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": text,
            "emoji": True
        }
    })
    
    # Section blocks for each user entry
    for entry in json_data:
        # Extract basic details
        sym = entry.get("sym", "Unknown")
        ma = entry.get("44_day_ma", "N/A")
        vol_ma = entry.get("7_vol_ma", "N/A")
        vol = entry.get("volume", "N/A")
        low = entry.get("low", "Unknown")
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{sym}* | MA: {round(ma, 2)} | Low: {round(low, 2)} || Avg Vol: {round(vol_ma, 2)} | Vol: {round(vol, 2)}"
            }
        })
    blocks.append({"type": "divider"})
    
    return blocks

def send_to_slack(webhook_url, blocks):
    data = {"blocks": blocks}
    response = requests.post(webhook_url, json=data)
    if response.status_code != 200:
        print(response.text)
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

def send_text_to_slack(webhook_url, text):
    payload = {
        'text': text
    }

    print(json.dumps(payload))

    # Send the POST request to Slack
    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )
    

data = read_sheet_data()


row_headers = first_column = [row[0] if len(row) > 0 else '' for row in data]
col_headers = np.array(data[0]).flatten()
col_headers_2 = np.array(data[1]).flatten()


block_data = []
sym_list_csv = []


update_data = []

# row_headers = ['','', 'IDEA']

for sym in row_headers:
    if len(sym) < 1:
        continue

    df1 = get_df_from_yahoo(sym)

    if df1 is None:
        continue

    row_json = df1.iloc[-1].to_dict()

    r,c = get_stock_loc(sym, row_json['date_string'])

    r += 1
    cn = number_to_excel_column(c+1)
    c = number_to_excel_column(c)

    if np.isnan(row_json['close']):
        row_json['close'] = '-'
    if np.isnan(row_json['volume']):
        row_json['volume'] = '-'

    update_data.append({
        'range': f'Sheet1!{c}{r}',
        'values': [[row_json['close']]]
    })
    update_data.append({
        'range': f'Sheet1!{cn}{r}',
        'values': [[row_json['volume']]]
    })

    # display(df1)

    downward = False
    e = None
    # e = analyse_data(df1, sym)

    if e is None:
        e = analyse_data_downward(df1, sym)
        downward = True

    if e is not None:
        print(sym)
        # display(e)
        row_json = e.iloc[-1].to_dict()

        if downward:
            downward = ' - 📉 '
        else:
            downward = ' - 📈 '

        row_json['sym'] = sym + downward

        if row_json['volume'] < 100000:
            continue

        block_data.append(row_json)
        sym_list_csv.append(sym)

        if len(block_data) > 30 and SEND_NOTIF:
            blocks = concise_json_to_slack_blocks(':mega:  Alert - Stock Worksheet - ' + row_json['date_string'] + '  :rotating_light: ', block_data)
            
            send_to_slack(webhook_details_url, blocks)
            send_text_to_slack(webhook_url, ':mega:  Alert - Stock Worksheet  ' + row_json['date_string'] + '  :rotating_light: \n' + '\n'.join(sym_list_csv))

            sym_list_csv = []
            block_data = []
            

bulk_update_cells(update_data)

if len(block_data) > 0 and SEND_NOTIF:
    blocks = concise_json_to_slack_blocks(':mega:  Alert - Stock Worksheet - ' + row_json['date_string'] + '  :rotating_light: ', block_data)

    send_to_slack(webhook_details_url, blocks)
    send_text_to_slack(webhook_url, ':mega:  Alert - Stock Worksheet  ' + row_json['date_string'] + '  :rotating_light: \n' + '\n'.join(sym_list_csv))
