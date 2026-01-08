import os
import boto3
import json
from dotenv import load_dotenv
load_dotenv('.env')
import pandas as pd
import botocore
import pyarrow

def get_s3_config():
    config = {
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region_name': os.getenv('AWS_DEFAULT_REGION'),
    }
    return config

def get_s3_client():
    s3_config = get_s3_config()
    s3 = boto3.client('s3', **s3_config)
    return s3

def upload_file_to_s3(upload_file_path, bucket_name, file_path):
    s3_client = get_s3_client()
    try:
        response = s3_client.upload_file(upload_file_path, bucket_name, file_path)
        print(f"File {file_path} berhasil diunggah ke bucket {bucket_name}")
    except Exception as e:
        print(f"Error saat mengunggah file ke bucket {bucket_name}: {e}")

def download_file_from_s3(file_path):
    # Initialize the S3 client
    s3 = get_s3_client()
    AWS_BUCKET = os.getenv('AWS_BUCKET')
    # print(AWS_BUCKET)
    
    # Local temporary file path
    local_file_path = f'{os.path.basename(file_path)}'
    
    # Debugging: print the paths being used
    # print(f"Constructed file path: {file_path}")
    # print(f"Local temporary file path: {local_file_path}")
    
     # Download the file from S3
    # print(f"Downloading {file_path} from S3 bucket {AWS_BUCKET} to {local_file_path}")
    try:
        # Download the file from S3
        s3.download_file(AWS_BUCKET, file_path, local_file_path)
        
        # Check if the file was downloaded
        if os.path.exists(local_file_path):
            # Check file size to ensure it is not empty or too small
            file_size = os.path.getsize(local_file_path)
            if file_size == 0:
                print(f"Downloaded file is empty: {local_file_path}")
                return pd.DataFrame()
            elif file_size < 1024:  # Misalnya, file terlalu kecil (bisa disesuaikan)
                print(f"Downloaded file is too small to be valid: {local_file_path}")
                return pd.DataFrame()

            # Attempt to read the Feather file into a DataFrame
            try:
                df = pd.read_feather(local_file_path)
            except pyarrow.lib.ArrowInvalid as e:
                print(f"Error reading Feather file: {e}")
                return pd.DataFrame()
            
            # Delete the temporary file
            try:
                os.remove(local_file_path)
                print(f"Temporary file deleted: {local_file_path}")
            except FileNotFoundError:
                print(f"File not found for deletion: {local_file_path}")
            
            # Return the DataFrame
            return df
        else:
            print(f"File not found after download attempt: {local_file_path}")
            return pd.DataFrame()
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"Data not found for: {file_path} on S3")
        else:
            print(f"Unexpected error: {e}")
        return pd.DataFrame()


def download_json_file_from_s3(file_path):
    # Initialize the S3 client
    s3 = get_s3_client()
    AWS_BUCKET = os.getenv('AWS_BUCKET')
    
    # Local temporary file path
    local_file_path = os.path.basename(file_path)
    
    # Debugging: print the paths being used
    print(f"Constructed file path: {file_path}")
    print(f"Local temporary file path: {local_file_path}")
    
    # Download the file from S3
    print(f"Downloading {file_path} from S3 bucket {AWS_BUCKET} to {local_file_path}")
    try:
        s3.download_file(AWS_BUCKET, file_path, local_file_path)
    except:
        print(f"File not found for {file_path}")
        return {}
    
    # Debugging: check if the file was downloaded
    if os.path.exists(local_file_path):
        print(f"File downloaded successfully: {local_file_path}")
    else:
        print(f"File not found after download attempt: {local_file_path}")
        raise FileNotFoundError(f"File not found after download attempt: {local_file_path}")
    
    # Read the json file
    print(f"Reading downloaded file {local_file_path}")
    try:
        with open(local_file_path, 'r') as file:
            json_file = json.load(file)
    except json.JSONDecodeError:
        print("Error decoding JSON from the file.")
        return {}
    except Exception as e:
        print(f"Error reading file: {e}")
        return {}
    
    # Remove the local temporary file
    try:
        os.remove(local_file_path)
        print(f"Temporary file deleted: {local_file_path}")
    except FileNotFoundError:
        print(f"File not found for deletion: {local_file_path}")
    
    return json_file