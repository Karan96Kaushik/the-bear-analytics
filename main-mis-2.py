import pandas as pd
import numpy as np
from data_fetcher import get_df_from_yahoo
from slack_notifier import send_to_slack, send_text_to_slack, concise_json_to_slack_blocks, candle_json_to_slack_blocks
from wh_urls import webhook_zaire_url, webhook_error_url
from data_fetcher import get_dhan_nifty50_data
from datetime import datetime, timezone
from sheet_operations import read_sheet_data_by_range

SEND_NOTIF = True
SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'

def check_ma_rising(df, window=5):
    ma_values = df['44_day_ma'].tail(window)
    return all(ma_values.iloc[i] < ma_values.iloc[i+1] for i in range(len(ma_values)-1))

def check_candle_conditions(row, ma_value, tolerance=0.01):
    open_price, close_price, high, low = row['open'], row['close'], row['high'], row['low']
    
    condition1 = close_price > open_price and abs(close_price - open_price) / open_price < 0.05
    condition2 = close_price > (high + low) / 2
    condition3 = (abs(ma_value - low) < (ma_value * tolerance)) or (ma_value > low and ma_value < high)
    
    return (condition1 or condition2) and condition3

def check_ma_falling(df, window=5):
    ma_values = df['44_day_ma'].tail(window)
    return all(ma_values.iloc[i] > ma_values.iloc[i+1] for i in range(len(ma_values)-1))

def check_reverse_candle_conditions(row, ma_value, tolerance=0.01):
    open_price, close_price, high, low = row['open'], row['close'], row['high'], row['low']
    
    condition1 = close_price < open_price and abs(close_price - open_price) / open_price < 0.05
    condition2 = close_price < (high + low) / 2
    condition3 = (abs(ma_value - high) < (ma_value * tolerance)) or (ma_value < high and ma_value > low)
    
    return (condition1 or condition2) and condition3

def scan_intraday_stocks(stock_list):
    selected_stocks = []
    
    for sym in stock_list:
        try:
            end_date = datetime.now().astimezone(timezone.utc).replace(second=10, microsecond=0)
            df = get_df_from_yahoo(sym, days=5, interval='15m', end_date=end_date)
            
            if df is None or df.empty:
                continue
            
            df['44_day_ma'] = df['close'].rolling(window=44).mean()
            df = df.dropna()
            
            is_rising = "UP" if check_ma_rising(df) else "DOWN" if check_ma_falling(df) else None
            if not is_rising:
                continue
            
            first_candle = df.iloc[-3]
            print(first_candle)
            ma_value = first_candle['44_day_ma']
            
            conditions_met = check_candle_conditions(first_candle, ma_value) if is_rising == "UP" else check_reverse_candle_conditions(first_candle, ma_value)
            
            if conditions_met:
                selected_stocks.append({
                    'sym': sym,
                    'open': first_candle['open'],
                    'close': first_candle['close'],
                    'high': first_candle['high'],
                    'low': first_candle['low'],
                    '44_day_ma': ma_value,
                    'volume': first_candle['volume'],
                    'direction': is_rising
                })
        except Exception as e:
            print(f"Error processing {sym}: {str(e)}")
    
    return selected_stocks

def main():
    stock_list = read_sheet_data_by_range('Nifty!A1:A500')
    stock_list = [item for sublist in stock_list for item in sublist]
    
    selected_stocks = scan_intraday_stocks(stock_list)
    
    if selected_stocks:
        up_stocks = [stock for stock in selected_stocks if stock['direction'] == 'UP']
        down_stocks = [stock for stock in selected_stocks if stock['direction'] == 'DOWN']
        
        if up_stocks:
            blocks = candle_json_to_slack_blocks(':mega: Zaire  Stock Scanner Results :chart_with_upwards_trend:', up_stocks)
            send_to_slack(webhook_zaire_url, blocks)
            sym_list = [stock['sym'] for stock in up_stocks]
            send_text_to_slack(webhook_zaire_url, f':mega: Zaire Stock Scanner Results :chart_with_upwards_trend:\n' + '\n'.join(sym_list))
        else:
            send_text_to_slack(webhook_zaire_url, "No stocks met the Zaire scanning criteria today.")
        
        if down_stocks:
            blocks = candle_json_to_slack_blocks(':mega: Reverse Zaire Stock Scanner Results :chart_with_downwards_trend:', down_stocks)
            send_to_slack(webhook_zaire_url, blocks)
            sym_list = [stock['sym'] for stock in down_stocks]
            send_text_to_slack(webhook_zaire_url, f':mega: Reverse Zaire Stock Scanner Results :chart_with_downwards_trend:\n' + '\n'.join(sym_list))
        else:
            send_text_to_slack(webhook_zaire_url, "No stocks met the reverse Zaire scanning criteria today.")
    else:
        send_text_to_slack(webhook_zaire_url, "No stocks met any scanning criteria today.")

if __name__ == "__main__":
    main()
