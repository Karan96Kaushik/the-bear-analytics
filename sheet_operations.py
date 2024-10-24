from google.oauth2 import service_account
from googleapiclient.discovery import build

SERVICE_ACCOUNT_FILE = 'top-glass-226920-ff9fb14e6f4f.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'
READ_RANGE = 'Sheet1!A1:DZ5000'
READ_RANGE_TARGET = 'REPLICA!L1:Y200'

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)

def read_sheet_data():
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=READ_RANGE).execute()
    return result.get('values', [])

def read_sheet_data_by_range(range):
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range).execute()
    return result.get('values', [])

def read_target_sheet_data():
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=READ_RANGE_TARGET).execute()
    return result.get('values', [])

def update_cell(row, col, value):
    range_name = f'Sheet1!{chr(65 + col)}{row + 1}'
    body = {'values': [[value]]}
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_name,
        valueInputOption='RAW', body=body).execute()
    return result

def bulk_update_cells(updates):
    body = {
        'valueInputOption': 'RAW',
        'data': updates
    }
    result = service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()
    return result

def get_stock_loc(stock, column, row_headers, col_headers):
    row = list(row_headers).index(stock)
    col_num = list(col_headers).index(column)
    # col_price = col
    # col_vol = col+1
    return row, col_num

def number_to_excel_column(n):
    column = ""
    n += 1
    while n > 0:
        n -= 1
        column = chr(n % 26 + 65) + column
        n //= 26
    return column