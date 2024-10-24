import pandas as pd
from data_fetcher import get_df_from_yahoo
from sheet_operations import read_sheet_data
from slack_notifier import send_text_to_slack
from wh_urls import webhook_url

SPREADSHEET_ID = '14FUaVzQfXqeGTd2HEKXMBwtofQNOebqiSdCK02GAYkA'

def find_local_minima(series: pd.Series, window: int) -> pd.Series:
    return (series == series.rolling(window=window, center=True).min()) & \
           (series.diff() < 0) & (series.diff().shift(-1) > 0)

def calculate_depth(peak: float, bottom: float) -> float:
    return (peak - bottom) / peak

def identify_double_bottom(df: pd.DataFrame, window: int = 30, tolerance: float = 0.001, depth_threshold: float = 0.1) -> pd.DataFrame:
    df['is_local_min'] = find_local_minima(df['low'], window)
    potential_patterns = []

    for i in range(len(df) - window):
        window_df = df.iloc[i:i+window]
        local_mins = window_df[window_df['is_local_min']]

        if len(local_mins) >= 2:
            first_bottom, second_bottom = local_mins.iloc[0], local_mins.iloc[-1]
            price_diff = abs(first_bottom['low'] - second_bottom['low']) / first_bottom['low']

            if price_diff <= tolerance:
                peak_between = window_df.loc[first_bottom.name:second_bottom.name, 'high'].max()
                first_depth = calculate_depth(peak_between, first_bottom['low'])
                second_depth = calculate_depth(peak_between, second_bottom['low'])

                # Additional checks for pattern validation
                if (peak_between > first_bottom['low'] and peak_between > second_bottom['low'] and
                    first_depth >= depth_threshold and second_depth >= depth_threshold and
                    validate_volume(df, first_bottom.name, second_bottom.name) and
                    confirm_breakout(df, second_bottom.name)):

                    potential_patterns.append({
                        'start_date': first_bottom.name,
                        'end_date': second_bottom.name,
                        'first_bottom': first_bottom['low'],
                        'second_bottom': second_bottom['low'],
                        'peak_between': peak_between
                    })

    return pd.DataFrame(potential_patterns)

def validate_volume(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    # Example logic: Ensure volume increases at the second bottom
    volume_increase = df.loc[end, 'volume'] > df.loc[start:end, 'volume'].mean()
    return volume_increase

def confirm_breakout(df: pd.DataFrame, end: pd.Timestamp) -> bool:
    # Example logic: Confirm breakout by checking if price exceeds peak after the second bottom
    breakout = df.loc[end:, 'close'].max() > df.loc[end, 'high']
    return breakout

def main():
    data = read_sheet_data()
    row_headers = [row[0] for row in data if row]

    double_bottom_stocks = []

    for sym in row_headers:
        if not sym:
            continue

        df = get_df_from_yahoo(sym, days=5, interval='1m')
        
        if df is None:
            print(f"Data for {sym} could not be fetched.")
            continue
        
        patterns = identify_double_bottom(df)
        
        if not patterns.empty:
            latest_pattern = patterns.iloc[-1]
            double_bottom_stocks.append({
                'symbol': sym,
                'start_date': latest_pattern['start_date'],
                'end_date': latest_pattern['end_date'],
                'first_bottom': latest_pattern['first_bottom'],
                'second_bottom': latest_pattern['second_bottom'],
                'peak_between': latest_pattern['peak_between']
            })

    if double_bottom_stocks:
        send_double_bottom_notifications(double_bottom_stocks)

def send_double_bottom_notifications(double_bottom_stocks: list):
    message = ":chart_with_upwards_trend: Double Bottom Patterns Detected :chart_with_upwards_trend:\n\n"
    for stock in double_bottom_stocks:
        message += f"*{stock['symbol']}*\n"
        message += f"Start Date: {stock['start_date']}\n"
        message += f"End Date: {stock['end_date']}\n"
        message += f"First Bottom: {stock['first_bottom']:.2f}\n"
        message += f"Second Bottom: {stock['second_bottom']:.2f}\n"
        message += f"Peak Between: {stock['peak_between']:.2f}\n\n"

    send_text_to_slack(webhook_url, message)

if __name__ == "__main__":
    main()
