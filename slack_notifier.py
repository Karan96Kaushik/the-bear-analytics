import requests
import json

def concise_json_to_slack_blocks(text, json_data):
    blocks = []
    
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": text,
            "emoji": True
        }
    })
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*SYM | MA | AvgVol | Vol | Low | LowPrev | High | HighPrev*"
        }
    })

    for entry in json_data:
        sym = entry.get("sym", "Unknown")
        ma = entry.get("44_day_ma", "N/A")
        vol_ma = entry.get("7_vol_ma", "N/A")
        vol = entry.get("volume", "N/A")
        low = entry.get("low", "Unknown")
        high = entry.get("high", "Unknown")
        prev = entry.get("prev", {})

        low_prev = prev.get("low")
        high_prev = prev.get("high")

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{sym}* | {round(ma, 2)} | {round(vol_ma, 2)} | {round(vol, 2)} | {round(low, 2)} | {round(low_prev, 2)} | {round(high, 2)} | {round(high_prev, 2)}"
            }
        })

    blocks.append({"type": "divider"})
    
    return blocks

def target_json_to_slack_blocks(text, json_data):
    blocks = []
    
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": text,
            "emoji": True
        }
    })
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*SYM | Price | AlertValue*"
        }
    })

    for entry in json_data:
        sym = entry.get("sym", "Unknown")
        price = entry.get("price", "Unknown")
        point = entry.get("point", "Unknown")


        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{sym}* | {round(price, 2)} | {round(point, 2)}"
            }
        })

    blocks.append({"type": "divider"})
    
    return blocks

def candle_json_to_slack_blocks(text, json_data):
    blocks = []
    
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": text,
            "emoji": True
        }
    })
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*SYM | Open | Close | High | Low | MA | Volume*"
        }
    })

    for entry in json_data:
        sym = entry.get("sym", "Unknown")
        open_price = entry.get("open", "N/A")
        close_price = entry.get("close", "N/A")
        high = entry.get("high", "N/A")
        low = entry.get("low", "N/A")
        ma = entry.get("44_day_ma", "N/A")
        volume = entry.get("volume", "N/A")

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{sym}* | {round(open_price, 2)} | {round(close_price, 2)} | {round(high, 2)} | {round(low, 2)} | {round(ma, 2)} | {round(volume, 2)}"
            }
        })

    blocks.append({"type": "divider"})
    
    return blocks

def send_to_slack(webhook_url, blocks):
    data = {"blocks": blocks}
    response = requests.post(webhook_url, json=data)
    if response.status_code != 200:
        print(response.text)
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

def send_text_to_slack(webhook_url, text):
    payload = {
        'text': text
    }

    print(json.dumps(payload))

    # Send the POST request to Slack
    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )
    
