from config.sqs import sqs_client, QUEUE_URL

async def send_message_to_sqs(message_body: str) -> dict:
    try:
        response = sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message_body
        )
        return {
            "MessageId": response["MessageId"],
            "ResponseMetadata": response["ResponseMetadata"]
        }
    except Exception as e:
        return {"error": str(e)}
