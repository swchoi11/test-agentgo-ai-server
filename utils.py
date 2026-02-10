from google.cloud import secretmanager

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "agentgo-studio"
    resource_name=f"projects/agentgo-studio/secrets/{secret_id}/versions/latest"

    try:
        response = client.access_secret_version(request={"name": resource_name})
        return response.payload.data.decode("utf-8")
    except Exception as e:
        print(e)
        return None
