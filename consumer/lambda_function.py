import json
import boto3
import base64
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-1').Table('crypto-prices')
sns = boto3.client('sns', region_name='us-east-1')
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:<YOUR_ACCOUNT_ID>:crypto-alerts'

THRESHOLDS = {
    "bitcoin":  5.0,
    "ethereum": 6.0,
    "solana":   8.0
}

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(
            base64.b64decode(record['kinesis']['data']).decode('utf-8')
        )

        coin      = payload['coin']
        price     = payload['price_usd']
        change    = payload['change_24h']
        timestamp = payload['timestamp']

        # Write to DynamoDB
        dynamodb.put_item(Item={
            "coin":       coin,
            "timestamp":  timestamp,
            "price_usd":  Decimal(str(price)),
            "change_24h": Decimal(str(round(change, 4))),
            "market_cap": Decimal(str(payload['market_cap']))
        })

        # Check anomaly threshold
        threshold = THRESHOLDS.get(coin, 5.0)
        if abs(change) >= threshold:
            direction = "UP" if change > 0 else "DOWN"
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Crypto Alert: {coin.upper()} moved {direction}",
                Message=(
                    f"Coin:       {coin.upper()}\n"
                    f"Price:      ${price:,.2f}\n"
                    f"24h Change: {change:.2f}%\n"
                    f"Threshold:  +-{threshold}%\n"
                    f"Time:       {timestamp}"
                )
            )
            print(f"ALERT sent for {coin}: {change:.2f}%")

    return {"statusCode": 200}