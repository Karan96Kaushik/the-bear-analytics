import pandas as pd
from slack_notifier import send_text_to_slack
from wh_urls import webhook_url, webhook_error_url, webhook_details_url 

def analyse_data(df, sym, tolerance=0.01):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        i = len(df) - 1
        
        if pd.notna(df.iloc[i]['44_day_ma']):
            if (df.iloc[i-1]['44_day_ma'] < df.iloc[i]['44_day_ma'] and
                df.iloc[i-2]['44_day_ma'] < df.iloc[i-1]['44_day_ma'] and
                df.iloc[i-3]['44_day_ma'] < df.iloc[i-2]['44_day_ma'] and
                df.iloc[i-4]['44_day_ma'] < df.iloc[i-3]['44_day_ma'] and
                (abs(df.iloc[i]['44_day_ma'] - df.iloc[i]['low']) < (df.iloc[i]['44_day_ma']*tolerance) or
                 (df.iloc[i]['44_day_ma'] > df.iloc[i]['low'] and df.iloc[i]['44_day_ma'] < df.iloc[i]['high'])) and
                (df.iloc[i]['close'] > df.iloc[i]['open'] or
                 (df.iloc[i]['high'] - df.iloc[i]['close']) < (df.iloc[i]['close'] - df.iloc[i]['low']))):
                
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

def analyse_data_downward(df, sym, tolerance=0.01):
    try:
        df['44_day_ma'] = df['close'].rolling(window=44).mean()
        df['7_vol_ma'] = df['volume'].rolling(window=7).mean()
        events = []
        i = len(df) - 1
        
        if pd.notna(df.iloc[i]['44_day_ma']):
            if (df.iloc[i-1]['44_day_ma'] > df.iloc[i]['44_day_ma'] and
                df.iloc[i-2]['44_day_ma'] > df.iloc[i-1]['44_day_ma'] and
                df.iloc[i-3]['44_day_ma'] > df.iloc[i-2]['44_day_ma'] and
                df.iloc[i-4]['44_day_ma'] > df.iloc[i-3]['44_day_ma'] and
                (abs(df.iloc[i]['44_day_ma'] - df.iloc[i]['high']) < (df.iloc[i]['44_day_ma']*tolerance) or
                 (df.iloc[i]['44_day_ma'] > df.iloc[i]['low'] and df.iloc[i]['44_day_ma'] < df.iloc[i]['high'])) and
                (df.iloc[i]['close'] < df.iloc[i]['open'] or
                 (df.iloc[i]['high'] - df.iloc[i]['close']) > (df.iloc[i]['close'] - df.iloc[i]['low']))):
                
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