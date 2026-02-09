import os
import json
from google.cloud import pubsub_v1

from process import process_message

PROJECT_ID = os.getenv("PROJECT_ID")
INPUT_TOPIC_ID = os.getenv("INPUT_TOPIC_ID")
OUTPUT_TOPIC_ID = os.getenv("OUTPUT_TOPIC_ID")

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subs_path = subscriber.subscription_path(PROJECT_ID,INPUT_TOPIC_ID)
pubs_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)

def callback(input):
    processed_result = process_message(input)

    if processed_result:
        try:
            databytes = json.dumps(processed_result).encode("utf-8")
            future = publisher.publish(pubs_path, data=databytes)
            future.result()

            input.ack()
            print("처리 및 결과 전송 ACK")

        except Exception as e:
            print(e)
        
    else:
        input.nack()

if __name__ == "__main__" :
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)

    streaming_pull_future = subscriber.subscribe(
        subs_path,
        callback=callback,
        flow_control=flow_control
    )

    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            print("streaming 종료")
