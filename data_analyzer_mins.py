import pandas as pd
import numpy as np
from slack_notifier import send_text_to_slack
from wh_urls import webhook_url, webhook_error_url, webhook_details_url 

def analyse_data_past_candle(df, sym, tolerance=0.01):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        i = len(df) - 1

        print(df)
        print(df.iloc[i-1])

        if pd.notna(df.iloc[i]['44_day_ma']):
            if (
                df.iloc[i-2]['44_day_ma'] < df.iloc[i-1]['44_day_ma'] and   # 44 day ma is going up
                df.iloc[i-3]['44_day_ma'] < df.iloc[i-2]['44_day_ma'] and   # 44 day ma is going up
                df.iloc[i-4]['44_day_ma'] < df.iloc[i-3]['44_day_ma'] and   # 44 day ma is going up
                df.iloc[i-5]['44_day_ma'] < df.iloc[i-4]['44_day_ma'] and   # 44 day ma is going up
                (abs(df.iloc[i-1]['44_day_ma'] - df.iloc[i-1]['low']) < (df.iloc[i-1]['44_day_ma']*tolerance) or   # price is near 44 day ma
                 (df.iloc[i-1]['44_day_ma'] > df.iloc[i-1]['low'] and df.iloc[i-1]['44_day_ma'] < df.iloc[i-1]['high'])) and   # price is between low and high
                (df.iloc[i-1]['close'] > df.iloc[i-1]['open'] or   # closing price is up
                 (df.iloc[i-1]['high'] - df.iloc[i-1]['close']) < (df.iloc[i-1]['close'] - df.iloc[i-1]['low']))):   # close is near high than low
                
                events.append(df.iloc[i-2])
                events.append(df.iloc[i-1])
                events.append(df.iloc[i])

        if len(events) > 0:
            events_df = pd.DataFrame(events)
            return events_df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - processing error - ' + str(e))
        print(e, sym)
        return None

def analyse_data_past_candle_downward(df, sym, tolerance=0.01):
    try:

        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        i = len(df) - 1

        if pd.notna(df.iloc[i]['44_day_ma']):
            if (
                df.iloc[i-2]['44_day_ma'] > df.iloc[i-1]['44_day_ma'] and   # 44 day ma is going down
                df.iloc[i-3]['44_day_ma'] > df.iloc[i-2]['44_day_ma'] and   # 44 day ma is going down
                df.iloc[i-4]['44_day_ma'] > df.iloc[i-3]['44_day_ma'] and   # 44 day ma is going down
                df.iloc[i-5]['44_day_ma'] > df.iloc[i-4]['44_day_ma'] and   # 44 day ma is going up
                (abs(df.iloc[i-1]['44_day_ma'] - df.iloc[i-1]['high']) < (df.iloc[i-1]['44_day_ma']*tolerance) or   # price is near 44 day ma
                 (df.iloc[i-1]['44_day_ma'] > df.iloc[i-1]['low'] and df.iloc[i-1]['44_day_ma'] < df.iloc[i-1]['high'])) and   # price is between low and high
                (df.iloc[i-1]['close'] < df.iloc[i-1]['open'] or   # closing price is down
                 (df.iloc[i-1]['high'] - df.iloc[i-1]['close']) > (df.iloc[i-1]['close'] - df.iloc[i-1]['low']))):   # close is near high than low
                
                events.append(df.iloc[i-2])
                events.append(df.iloc[i-1])
                events.append(df.iloc[i])

        if len(events) > 0:
            events_df = pd.DataFrame(events)
            return events_df
    except Exception as e:
        send_text_to_slack(webhook_error_url, sym + ' - processing error - ' + str(e))
        print(e, sym)
        return None

def identify_double_bottom(df, window=20, tolerance=0.03):
    """
    Identify double bottom patterns in the given DataFrame.
    
    :param df: DataFrame with 'low' prices
    :param window: Number of periods to look for the pattern
    :param tolerance: Tolerance for price difference between two bottoms
    :return: DataFrame with potential double bottom patterns
    """
    # ... existing code ...

    def find_local_minima(series, window):
        return series == series.rolling(window=window, center=True).min()

    df['is_local_min'] = find_local_minima(df['low'], window)
    
    potential_patterns = []
    
    for i in range(len(df) - window):
        window_df = df.iloc[i:i+window]
        local_mins = window_df[window_df['is_local_min']]
        
        if len(local_mins) >= 2:
            first_bottom = local_mins.iloc[0]
            second_bottom = local_mins.iloc[-1]
            
            price_diff = abs(first_bottom['low'] - second_bottom['low']) / first_bottom['low']
            
            if price_diff <= tolerance:
                peak_between = window_df.loc[first_bottom.name:second_bottom.name, 'high'].max()
                
                if peak_between > first_bottom['low'] and peak_between > second_bottom['low']:
                    potential_patterns.append({
                        'start_date': first_bottom.name,
                        'end_date': second_bottom.name,
                        'first_bottom': first_bottom['low'],
                        'second_bottom': second_bottom['low'],
                        'peak_between': peak_between
                    })
    
    return pd.DataFrame(potential_patterns)

# ... rest of the existing code ...
