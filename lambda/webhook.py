import boto3
import json
import os

firehose = boto3.client("firehose")
stream_name = os.environ["FIREHOSE_STREAM"]


def handler(event, content):
    try:
        body = event.get("body")
        events = json.loads(body)
        for event in events:
            kinesis_data = f'{json.dumps(event)}\n'
            firehose.put_record(DeliveryStreamName=stream_name,
                                Record={'Data': kinesis_data}
                                )
            print(kinesis_data)
        status_code = 200
        resp = {"description": "Success"}
    except Exception as e:
        print(e)
        status_code = 500
        resp = {"description": str(e)}
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "text/plain"
        },
        "body": json.dumps(resp)
    }
