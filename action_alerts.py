import pandas as pd
from data_fetcher import get_df_from_yahoo
from data_analyzer import analyse_data, analyse_data_downward
from sheet_operations import read_sheet_data, read_target_sheet_data, bulk_update_cells, get_stock_loc, number_to_excel_column
from slack_notifier import send_to_slack, send_text_to_slack, target_json_to_slack_blocks
import numpy as np
from wh_urls import webhook_url, webhook_error_url, webhook_action_alerts_url 

SEND_NOTIF = True
SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'


def main():
    data = read_target_sheet_data()
    row_headers = [row[0] if len(row) > 0 else '' for row in data]
    col_headers = np.array(data[0]).flatten()
    # col_headers_2 = np.array(data[1]).flatten()

    print(row_headers)
    print(col_headers)
    # print(col_headers_2)

    # return


    block_data_sl = []
    block_data_target = []

    for row in data:
        try:
            sym = row[0]

            if len(sym) < 1:
                continue

            df1 = get_df_from_yahoo(sym, days=1, interval='1h')
            if df1 is None:
                continue
            
            row_json = df1.iloc[-1].to_dict()

            _, c_sl = get_stock_loc(sym, 'Stop Loss', row_headers, col_headers)
            _, c_status = get_stock_loc(sym, 'STATUS', row_headers, col_headers)
            c_target = c_sl + 1

            sl = float(row[c_sl])
            target = float(row[c_target])
            print(c_status, len(row))

            status = '' #row[c_status]
            if (len(row) > c_status):
                print(len(row) , c_status)
                status = row[c_status]

            low = float(row_json['low'])
            high = float(row_json['high'])

            if (low <= sl and len(status) == 0):
                print('SL Hit', sym)
                block_data_sl.append({
                    'sym': sym,
                    'point': sl,
                    'price': low,
                })

            print(status)

            if (high >= target and len(status) == 0):
                print('Target Hit', sym)
                block_data_target.append({
                    'sym': sym,
                    'point': target,
                    'price': high,
                })

        except Exception as e:
            send_text_to_slack(webhook_error_url, sym + ' - data action alerts error - ' + str(e))
            return None


    if (len(block_data_target) > 0 or len(block_data_sl) > 0) and SEND_NOTIF:

        
        data_batches = [
            [
                block_data_target[0:30],
                block_data_target[30:60],
                block_data_target[60:90],
                block_data_target[90:120],
            ],
            [
                block_data_sl[0:30],
                block_data_sl[30:60],
                block_data_sl[60:90],
                block_data_sl[90:120],
            ]
        ]

        for i, batch in enumerate(data_batches):
            
            print(i, len(batch))
            if i == 0:
                trend = 'Target'
            else:
                trend = 'Stoploss'

            for block_data in batch:
                if len(block_data) > 0:
                    
                    blocks = target_json_to_slack_blocks(f':mega:  Alert - {trend} Hit' + '  :rotating_light: ', block_data)

                    send_to_slack(webhook_action_alerts_url, blocks)

        # c_target = number_to_excel_column(c+1)
        # c_sl = number_to_excel_column(c)

        # print(r, c_sl, c_target)

        # print(row_json)
        # r, c = get_stock_loc(sym, row_json['date_string'], row_headers, col_headers)
        # r += 1
        # cn = number_to_excel_column(c+1)
        # c = number_to_excel_column(c)


# def send_notifications(block_data, date_string, sym_list_csv):
#     blocks = concise_json_to_slack_blocks(f':mega:  Alert - Stock Worksheet - {date_string}  :rotating_light: ', block_data)
#     send_to_slack(webhook_details_url, blocks)
#     send_text_to_slack(webhook_url, f':mega:  Alert - Stock Worksheet  {date_string}  :rotating_light: \n' + '\n'.join(sym_list_csv))

if __name__ == "__main__":
    main()