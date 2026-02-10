import os
import json
from google.cloud import pubsub_v1, secretmanager

from process import process_message

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "agentgo-studio"
    resource_name=f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    try:
        response = client.access_secret_version(request={"name": resource_name})
        return response.payload.data.decode("utf-8")
    except Exception as e:
        print(e)
        return None

PROJECT_ID = os.getenv("PROJECT_ID")
INPUT_TOPIC_ID = get_secret("INPUT_TOPIC_ID")
OUTPUT_TOPIC_ID = get_secret("OUTPUT_TOPIC_ID")

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
