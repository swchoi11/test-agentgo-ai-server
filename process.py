import json

def process_message(raw_input):
    try:
        data = json.loads(raw_input.data.decode("utf-8"))
        user_name = data.get("user_name")
        user_input = data.get("user_input")

        print(f"수신된 요청 : user_name [{user_name}], user_input [{user_input}]")

        processed_value = f"{user_input}000"

        result_payload = f"{user_name}_{processed_value}"

        return result_payload

    except Exception as e:
        print(e)