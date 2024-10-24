import pandas as pd
import numpy as np
from data_fetcher import get_df_from_yahoo
from slack_notifier import send_to_slack, send_text_to_slack, concise_json_to_slack_blocks, candle_json_to_slack_blocks
from wh_urls import webhook_url, webhook_error_url, webhook_details_url
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
    
    # Green candle (closing price higher than opening) with the closing and opening price gap less than 5%
    condition1 = close_price > open_price and abs(close_price - open_price) / open_price < 0.05

    # a candle where the closing price is higher than the average of the highest and lowest price of the day
    condition2 = close_price > (high + low) / 2

    # the candle either touches the 44 moving average or is between 0-1% higher than the 44 Moving average
    condition3 = (abs(ma_value - low) < (ma_value * tolerance)) or (ma_value > low and ma_value < high)
    
    return (condition1 or condition2) and condition3

def check_ma_falling(df, window=5):
    ma_values = df['44_day_ma'].tail(window)
    return all(ma_values.iloc[i] > ma_values.iloc[i+1] for i in range(len(ma_values)-1))

def check_reverse_candle_conditions(row, ma_value, tolerance=0.01):
    open_price, close_price, high, low = row['open'], row['close'], row['high'], row['low']
    
    # Red candle (closing price lower than opening) with the closing and opening price gap less than 5%
    condition1 = close_price < open_price and abs(close_price - open_price) / open_price < 0.05

    # a candle where the closing price is lower than the average of the highest and lowest price of the day
    condition2 = close_price < (high + low) / 2

    # the candle either touches the 44 moving average or is between 0-1% lower than the 44 Moving average
    condition3 = (abs(ma_value - high) < (ma_value * tolerance)) or (ma_value < high and ma_value > low)
    
    return (condition1 or condition2) and condition3

def scan_intraday_stocks(stock_list):
    selected_stocks = []
    
    for sym in stock_list:
        
        # print(sym)

        end_date = datetime.now().astimezone(timezone.utc).replace(hour=4, minute=1, second=0, microsecond=0)
        # print(end_date.isoformat())
        df = get_df_from_yahoo(sym, days=5, interval='15m', end_date=end_date)
        
        if df is None or df.empty:
            continue

        # print(df)
        
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        
        if not check_ma_rising(df):
            continue
        
        first_candle = df.iloc[-1]  # Assuming the last row is the 4:01 candle
        ma_value = first_candle['44_day_ma']
        
        if check_candle_conditions(first_candle, ma_value):
            selected_stocks.append({
                'sym': sym,
                'open': first_candle['open'],
                'close': first_candle['close'],
                'high': first_candle['high'],
                'low': first_candle['low'],
                '44_day_ma': ma_value,
                'volume': first_candle['volume']
            })
    
    return selected_stocks

def scan_reverse_intraday_stocks(stock_list):
    selected_stocks = []
    
    for sym in stock_list:
        
        
        end_date = datetime.now().astimezone(timezone.utc).replace(hour=4, minute=0, second=0, microsecond=0)
        df = get_df_from_yahoo(sym, days=5, interval='15m', end_date=end_date)
        
        if df is None or df.empty:
            continue
        
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        
        if not check_ma_falling(df):
            continue
        
        first_candle = df.iloc[-1]  # Assuming the last row is the 4:00 candle
        ma_value = first_candle['44_day_ma']
        
        if check_reverse_candle_conditions(first_candle, ma_value):
            selected_stocks.append({
                'sym': sym,
                'open': first_candle['open'],
                'close': first_candle['close'],
                'high': first_candle['high'],
                'low': first_candle['low'],
                '44_day_ma': ma_value,
                'volume': first_candle['volume']
            })
    
    return selected_stocks

def main():
    # Define your list of stocks here
    stock_list = read_sheet_data_by_range('Nifty!A1:A500')

    stock_list = [item for sublist in stock_list for item in sublist]
    
    selected_stocks = scan_intraday_stocks(stock_list)
    
    if selected_stocks:
        blocks = candle_json_to_slack_blocks(':mega: Intraday Stock Scanner Results :chart_with_upwards_trend:', selected_stocks)
        send_to_slack(webhook_details_url, blocks)
        
        sym_list = [stock['sym'] for stock in selected_stocks]
        send_text_to_slack(webhook_url, f':mega: Intraday Stock Scanner Results :chart_with_upwards_trend:\n' + '\n'.join(sym_list))
    else:
        send_text_to_slack(webhook_url, "No stocks met the intraday scanning criteria today.")

    selected_stocks = scan_reverse_intraday_stocks(stock_list)
    
    if selected_stocks:
        blocks = candle_json_to_slack_blocks(':mega: Reverse Intraday Stock Scanner Results :chart_with_downwards_trend:', selected_stocks)
        send_to_slack(webhook_details_url, blocks)
        sym_list = [stock['sym'] for stock in selected_stocks]
        send_text_to_slack(webhook_url, f':mega: Reverse Intraday Stock Scanner Results :chart_with_downwards_trend:\n' + '\n'.join(sym_list))
    else:
        send_text_to_slack(webhook_url, "No stocks met the reverse intraday scanning criteria today.")

if __name__ == "__main__":
    main()
