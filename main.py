import os
import json
from google.cloud import pubsub_v1
from utils import get_secret
from database import update_record
from process import process_message

PROJECT_ID = os.getenv("PROJECT_ID")
INPUT_SUBS_ID = get_secret("INPUT_SUBS_ID")
OUTPUT_TOPIC_ID = get_secret("OUTPUT_TOPIC_ID")

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subs_path = subscriber.subscription_path(PROJECT_ID,INPUT_SUBS_ID)
pubs_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    db_id = data.get("db_id")

    processed_result = process_message(message)

    if processed_result and isinstance(processed_result, dict):
        try:
            vm_output_value = processed_result.get("vm_output")
            success = update_record(db_id, vm_output_value)

            if success:
                print("성공")

            databytes = json.dumps(processed_result).encode("utf-8")
            publisher.publish(pubs_path, data=databytes)

            message.ack()
            print("처리 및 결과 전송 ACK")

        except Exception as e:
            print(e)
            message.nack()
        
    else:
        message.nack()

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
