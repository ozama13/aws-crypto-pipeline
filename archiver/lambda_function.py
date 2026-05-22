import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('crypto-prices')

BUCKET = 'YOUR_BUCKET-NAME'

def lambda_handler(event, context):

    response = table.scan()
    items = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    if not items:
        print("No records found in DynamoDB")
        return {"statusCode": 200, "body": "No records to archive"}

    df = pd.DataFrame(items)
    df['price_usd'] = df['price_usd'].astype(float)
    df['change_24h'] = df['change_24h'].astype(float)
    df['market_cap'] = df['market_cap'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    s3_path = f"s3://<YOUR_BUCKET_NAME>/raw/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="append"
    )

    print(f"Archived {len(df)} records to {s3_path}")
    return {"statusCode": 200, "body": f"Archived {len(df)} records"}