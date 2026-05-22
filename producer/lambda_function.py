import json
import boto3
import urllib.request
import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')
STREAM_NAME = 'crypto-price-stream'
COINS = 'bitcoin,ethereum,solana'

def lambda_handler(event, context):
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={COINS}&vs_currencies=usd"
        f"&include_24hr_change=true&include_market_cap=true"
    )

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())

    timestamp = datetime.datetime.utcnow().isoformat()

    records = []
    for coin, metrics in data.items():
        record = {
            "coin": coin,
            "price_usd": metrics["usd"],
            "change_24h": metrics["usd_24h_change"],
            "market_cap": metrics["usd_market_cap"],
            "timestamp": timestamp
        }
        records.append({
            "Data": json.dumps(record).encode("utf-8"),
            "PartitionKey": coin
        })

    kinesis.put_records(StreamName=STREAM_NAME, Records=records)
    print(f"Pushed {len(records)} records at {timestamp}")
    return {"statusCode": 200}