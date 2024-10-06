import pandas as pd
from data_fetcher import get_df_from_yahoo, get_sym_df
from data_analyzer import analyse_data, analyse_data_downward
from sheet_operations import read_sheet_data, bulk_update_cells, get_stock_loc, number_to_excel_column
from slack_notifier import send_to_slack, send_text_to_slack, concise_json_to_slack_blocks
import numpy as np
from wh_urls import webhook_url, webhook_error_url, webhook_wide_details_url

SEND_NOTIF = True
PAGES = 10

def main():
    block_data_up = []
    sym_list_csv_up = []
    block_data_down = []
    sym_list_csv_down = []


    for pg in range(PAGES):
        df_syms = get_sym_df(pgno=pg)
        for index, row in df_syms.iterrows():
            sym = row['Sym']

            df1 = get_df_from_yahoo(sym)

            if df1 is None:
                continue

            row_json = df1.iloc[-1].to_dict()

            downward = False
            e = None
            e = analyse_data(df1, sym)

            if e is None:
                e = analyse_data_downward(df1, sym)
                downward = True

            if e is not None:
                print(sym, pg)
                # display(e)
                row_json = e.iloc[-1].to_dict()

                # if downward:
                #     downward = ' - 📉 '
                # else:
                #     downward = ' - 📈 '

                row_json['sym'] = sym # + downward

                row_json_prev = e.iloc[-2].to_dict()
                row_json['prev'] = row_json_prev

                if row_json['volume'] < 100000:
                    continue
                
                if downward:
                    block_data_down.append(row_json)
                    sym_list_csv_down.append(sym)
                else:
                    block_data_up.append(row_json)
                    sym_list_csv_up.append(sym)

                # if len(block_data) > 30 and SEND_NOTIF:
                #     blocks = concise_json_to_slack_blocks(':mega:  Alert - Mega List - ' + row_json['date_string'] + '  :rotating_light: ', block_data)
                    
                #     send_to_slack(webhook_wide_details_url, blocks)
                #     # send_text_to_slack(webhook_url, ':mega:  Alert - Mega List  ' + row_json['date_string'] + '  :rotating_light: \n' + '\n'.join(sym_list_csv))

                #     sym_list_csv = []
                #     block_data = []
    
    if (len(block_data_up) > 0 or len(block_data_down) > 0) and SEND_NOTIF:

        data_batches = [
            [
                block_data_up[0:30],
                block_data_up[30:60],
                block_data_up[60:90],
                block_data_up[90:120],
            ],
            [
                block_data_down[0:30],
                block_data_down[30:60],
                block_data_down[60:90],
                block_data_down[90:120],
            ]
        ]

        for i, batch in enumerate(data_batches):
            
            print(i, len(batch))
            if i == 0:
                trend = 'Upward'
            else:
                trend = 'Downward'

            for block_data in batch:
                if len(block_data) > 0:
                    
                    blocks = concise_json_to_slack_blocks(f':mega:  Alert - Mega List - {trend} Trend - ' + row_json['date_string'] + '  :rotating_light: ', block_data)

                    send_to_slack(webhook_wide_details_url, blocks)
        # send_text_to_slack(webhook_url, ':mega:  Alert - Mega List  ' + row_json['date_string'] + '  :rotating_light: \n' + '\n'.join(sym_list_csv))


if __name__ == "__main__":
    main()