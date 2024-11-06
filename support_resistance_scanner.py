import pandas as pd
import numpy as np
from data_fetcher import get_df_from_yahoo, get_sym_df
from slack_notifier import send_to_slack, send_text_to_slack
from wh_urls import webhook_url, webhook_error_url
import numpy as np
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

SEND_NOTIF = True
PAGES = 10
DEFAULT_TOLERANCE = 0.02  # 2% tolerance by default

def find_support_resistance_levels(df, window_size=20, min_touches=2):
    """Find support and resistance levels using price action"""
    highs = df['high'].rolling(window=window_size, center=True).max()
    lows = df['low'].rolling(window=window_size, center=True).min()
    
    resistance_levels = []
    resistance_touches = {}  # New dictionary to store touches count
    support_levels = []
    support_touches = {}    # New dictionary to store touches count
    
    # Find resistance levels
    for i in range(window_size, len(df) - window_size):
        if highs.iloc[i] == df['high'].iloc[i]:
            price_level = df['high'].iloc[i]
            touches = sum((df['high'].iloc[i-window_size:i+window_size] >= price_level * 0.995) & 
                         (df['high'].iloc[i-window_size:i+window_size] <= price_level * 1.005))
            
            if touches >= min_touches:
                rounded_level = round(price_level, 2)
                resistance_levels.append(rounded_level)
                resistance_touches[rounded_level] = touches  # Store touch count
    
    # Find support levels
    for i in range(window_size, len(df) - window_size):
        if lows.iloc[i] == df['low'].iloc[i]:
            price_level = df['low'].iloc[i]
            touches = sum((df['low'].iloc[i-window_size:i+window_size] <= price_level * 1.005) & 
                         (df['low'].iloc[i-window_size:i+window_size] >= price_level * 0.995))
            
            if touches >= min_touches:
                rounded_level = round(price_level, 2)
                support_levels.append(rounded_level)
                support_touches[rounded_level] = touches  # Store touch count
    
    # Remove duplicates and sort
    resistance_levels = sorted(list(set([round(x, 2) for x in resistance_levels])))
    support_levels = sorted(list(set([round(x, 2) for x in support_levels])))
    
    return support_levels, resistance_levels, support_touches, resistance_touches

def is_near_level(price, level, tolerance):
    """Check if price is near a support/resistance level within tolerance"""
    return abs(price - level) <= (level * tolerance)

def scan_support_resistance(tolerance=DEFAULT_TOLERANCE):
    near_support = []
    near_resistance = []
    total_stocks = 0
    processed_stocks = 0
    start_time = datetime.now()
    
    logging.info(f"Starting scan with tolerance: {tolerance*100}%")
    
    for pg in range(PAGES):
        logging.info(f"Fetching page {pg + 1}/{PAGES}")
        df_syms = get_sym_df(pgno=pg)
        page_stocks = len(df_syms) if df_syms is not None else 0
        total_stocks += page_stocks
        
        if df_syms is None or df_syms.empty:
            logging.warning(f"No symbols found on page {pg + 1}")
            continue
            
        for _, row in df_syms.iterrows():
            processed_stocks += 1
            sym = row['Sym']
            
            if processed_stocks % 10 == 0:  # Log progress every 10 stocks
                elapsed = (datetime.now() - start_time).total_seconds()
                stocks_per_second = processed_stocks / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: {processed_stocks}/{total_stocks} stocks processed "
                           f"({stocks_per_second:.2f} stocks/sec)")
            
            try:
                logging.debug(f"Processing {sym}")
                df = get_df_from_yahoo(sym, days=60)
                
                if df is None or df.empty:
                    logging.debug(f"No data available for {sym}")
                    continue
                
                current_price = df['close'].iloc[-1]
                
                if df['volume'].iloc[-1] < 100000:
                    logging.debug(f"Skipping {sym} due to low volume")
                    continue
                
                support_levels, resistance_levels, support_touches, resistance_touches = find_support_resistance_levels(df)
                
                # Check support levels
                for level in support_levels:
                    if is_near_level(current_price, level, tolerance):
                        distance = round((current_price - level) / level * 100, 2)
                        touches = support_touches[level]  # Get touch count
                        logging.info(f"Found {sym} near support: price={current_price:.2f}, "
                                   f"level={level:.2f}, distance={distance}%, touches={touches}")
                        near_support.append({
                            'symbol': sym,
                            'current_price': current_price,
                            'support_level': level,
                            'distance': distance,
                            'touches': touches  # Add touches to output
                        })
                        break
                
                # Check resistance levels
                for level in resistance_levels:
                    if is_near_level(current_price, level, tolerance):
                        distance = round((level - current_price) / level * 100, 2)
                        touches = resistance_touches[level]  # Get touch count
                        logging.info(f"Found {sym} near resistance: price={current_price:.2f}, "
                                   f"level={level:.2f}, distance={distance}%, touches={touches}")
                        near_resistance.append({
                            'symbol': sym,
                            'current_price': current_price,
                            'resistance_level': level,
                            'distance': distance,
                            'touches': touches  # Add touches to output
                        })
                        break
                
            except Exception as e:
                logging.error(f"Error processing {sym}: {str(e)}")
                continue
    
    # Log final statistics
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"\nScan completed in {duration:.1f} seconds")
    logging.info(f"Total stocks processed: {processed_stocks}")
    logging.info(f"Stocks near support: {len(near_support)}")
    logging.info(f"Stocks near resistance: {len(near_resistance)}")
    
    return near_support, near_resistance

def format_slack_message(stocks, level_type):
    """Format the stocks data into a Slack message"""
    blocks = []
    
    header = {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Stocks Near {level_type.title()} Levels 📊",
            "emoji": True
        }
    }
    blocks.append(header)
    
    for stock in stocks:
        level_value = stock.get(f'{level_type}_level')
        text = (f"*{stock['symbol']}*\n"
                f"Current Price: {stock['current_price']:.2f}\n"
                f"{level_type.title()} Level: {level_value:.2f}\n"
                f"Distance: {stock['distance']}%\n"
                f"Touches: {stock['touches']}")  # Add touches to message
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        })
        
        blocks.append({"type": "divider"})
    
    return blocks

def main(tolerance=DEFAULT_TOLERANCE):
    logging.info("Starting support/resistance scanner")
    near_support, near_resistance = scan_support_resistance(tolerance)
    
    if SEND_NOTIF:
        if near_support:
            logging.info(f"Sending notification for {len(near_support)} stocks near support")
            support_blocks = format_slack_message(near_support, 'support')
            send_to_slack(webhook_url, support_blocks)
        
        if near_resistance:
            logging.info(f"Sending notification for {len(near_resistance)} stocks near resistance")
            resistance_blocks = format_slack_message(near_resistance, 'resistance')
            send_to_slack(webhook_url, resistance_blocks)
        
        summary = (f"Found {len(near_support)} stocks near support and "
                  f"{len(near_resistance)} stocks near resistance "
                  f"(tolerance: {tolerance*100}%)")
        logging.info("Sending summary notification")
        send_text_to_slack(webhook_url, summary)
    
    logging.info("Scan completed successfully")

if __name__ == "__main__":
    main() 