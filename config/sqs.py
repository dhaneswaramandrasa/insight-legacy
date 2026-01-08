#config/sqs.py
import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize SQS client
sqs_client = boto3.client(
    'sqs',
    region_name=os.getenv('SQS_REGION'),
    aws_access_key_id=os.getenv('SQS_KEY'),
    aws_secret_access_key=os.getenv('SQS_SECRET')
)

# Queue URL from AWS SQS
QUEUE_URL = os.getenv('SQS_URL')
