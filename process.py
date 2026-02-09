import json

def process_message(user_input):
    try:
        data = json.loads(user_input.data.decode("utf-8"))
        user_name = data.get("user_name")
        number = data.get("number")

        print(f"수신된 요청 : user_name [{user_name}], number [{number}]")

        processed_value = f"{number}000"

        result_payload = f"{user_name}_{processed_value}"

        return result_payload

    except Exception as e:
        print(e)