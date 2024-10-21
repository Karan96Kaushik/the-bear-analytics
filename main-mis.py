import pandas as pd
from data_fetcher import get_df_from_yahoo
from data_analyzer_mins import analyse_data_past_candle, analyse_data_past_candle_downward
from sheet_operations import read_sheet_data, bulk_update_cells, get_stock_loc, number_to_excel_column
from slack_notifier import send_to_slack, send_text_to_slack, concise_json_to_slack_blocks
import numpy as np
from wh_urls import webhook_url, webhook_error_url, webhook_details_url 

SEND_NOTIF = True
SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'


def main():
    data = read_sheet_data()
    row_headers = [row[0] if len(row) > 0 else '' for row in data]
    col_headers = np.array(data[0]).flatten()
    col_headers_2 = np.array(data[1]).flatten()

    block_data = []
    sym_list_csv = []
    update_data = []

    

    for sym in row_headers:
        if len(sym) < 1:
            continue

        df1 = get_df_from_yahoo(sym, days=15, interval='15m')
        
        if df1 is None:
            continue
        
        row_json = df1.iloc[-1].to_dict()
        r, c = get_stock_loc(sym, row_json['date_string'], row_headers, col_headers)
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

        downward = False
        e = analyse_data_past_candle(df1, sym, tolerance=0.005)


        if e is None:
            e = analyse_data_past_candle_downward(df1, sym, tolerance=0.005)

            downward = True

        if e is not None:
            row_json = e.iloc[-1].to_dict()
            # row_json['sym'] = sym + (' - 📉 ' if downward else ' - 📈 ')
            row_json['sym'] = sym
            row_json['dir'] = '📉' if downward else '📈'

            row_json_prev = e.iloc[-2].to_dict()
            row_json['prev'] = row_json_prev

            # if row_json['volume'] < 100000:
            #     continue

            block_data.append(row_json)
            sym_list_csv.append(sym)

            if len(block_data) > 30 and SEND_NOTIF:
                send_notifications(block_data, row_json['date_string'], sym_list_csv)
                sym_list_csv = []
                block_data = []

    bulk_update_cells(update_data)

    if len(block_data) > 0 and SEND_NOTIF:
        send_notifications(block_data, row_json['date_string'], sym_list_csv)

def send_notifications(block_data, date_string, sym_list_csv):
    blocks = concise_json_to_slack_blocks(f':mega:  Alert - Stock Worksheet - MIS - {date_string}  :rotating_light: ', block_data)
    send_to_slack(webhook_details_url, blocks)
    send_text_to_slack(webhook_url, f':mega:  Alert - Stock Worksheet - MIS - {date_string}  :rotating_light: \n' + '\n'.join(sym_list_csv))

if __name__ == "__main__":
    main()