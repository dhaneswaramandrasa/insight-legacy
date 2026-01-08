from fastapi import Request,Response
import boto3
from botocore.exceptions import ClientError
from config.s3 import get_s3_config
from services.SalesRecapService import SalesRecapService
import json, os, datetime
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import logging
load_dotenv('.env')

class S3StorageAwsController:
    @staticmethod
    async def DownloadFileFromS3(request: Request):
        try:
            # Mendapatkan konfigurasi S3 dari fungsi yang sudah ada
            s3_config = get_s3_config()
            s3_client = boto3.client('s3', **s3_config)

            body = await request.json()
            store_id = body.get("store_id")
            path = body.get("path")
            aws_bucket = os.getenv('AWS_BUCKET')
            
            if path == "product-populer-vision":
                file_name = f"{store_id}_product_populer_vision.feather"
            elif path == "sales-tracking-vision":
                file_name = f"{store_id}_sales_tracking_vision.feather"
            else:
                return {"error": "Invalid path"}
            
            # Full path di bucket S3
            s3_file_key = f"data-lake-python-feather/{path}/{file_name}"
            print(s3_file_key)
            print(aws_bucket)
            print(file_name)
            # Download file dari S3
            s3_client.download_file(aws_bucket, s3_file_key, file_name)
            
            return {"message": f"File {file_name} berhasil diunduh dari bucket {aws_bucket} ke {file_name}"}
        except ClientError as e:
            return {"error": f"Error saat mengunduh file dari bucket {aws_bucket}: {e}"}
        except Exception as e:
            return {"error": f"Error yang tidak diketahui saat mengunduh file dari bucket {aws_bucket}: {e}"}
        
    @staticmethod
    async def ReadFileFromS3(request: Request):
        try:
            # Mendapatkan konfigurasi S3 dari fungsi yang sudah ada
            s3_config = get_s3_config()
            s3_client = boto3.client('s3', **s3_config)

            # Mendapatkan data dari request
            body = await request.json()
            store_id = body.get("store_id")
            start_date = body.get("start_date")
            path = body.get("path")
            download = body.get("download")
            aws_bucket = os.getenv('AWS_BUCKET')
            
            # Validasi path dan set nama file berdasarkan path
            if path == "product-populer-vision":
                file_name = f"{store_id}_{start_date}_product_populer_vision.feather"
            elif path == "sales-tracking-vision":
                file_name = f"{store_id}_{start_date}_sales_tracking_vision.feather"
            elif path == 'summary-sales-product-vision':
                file_name = f"{store_id}_summary_sales_product_vision.feather"
            elif path == 'sales-order':
                file_name = f"{store_id}_sales_order_6_months_ago.feather"
            elif path == 'summary-sales-vision':
                file_name =  f"{store_id}_summary_sales_vision.feather"
            else:
                logging.error("Invalid path provided.")
                return {"error": "Invalid path"}

            # Validasi bahwa aws_bucket tidak None
            if not aws_bucket:
                logging.error("AWS bucket is not specified.")
                return {"error": "AWS bucket is not specified."}

            # Full path di bucket S3
            s3_file_key = f"data-lake-python-feather/{path}/{file_name}"
            logging.info(f"Attempting to access S3 object: Bucket={aws_bucket}, Key={s3_file_key}")

            # Mengunduh file dari S3 ke objek BytesIO
            file_stream = BytesIO()
            try:
                s3_client.download_fileobj(aws_bucket, s3_file_key, file_stream)
                file_stream.seek(0)
                logging.info(f"File {file_name} downloaded successfully.")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    logging.error(f"File not found: {s3_file_key} in bucket {aws_bucket}")
                    return {"error": f"File not found: {s3_file_key} in bucket {aws_bucket}"}
                else:
                    logging.error(f"ClientError: {e}")
                    return {"error": f"Error saat mengunduh file dari bucket {aws_bucket}: {e}"}

            # Membaca file Feather menjadi DataFrame pandas
            try:
                df = pd.read_feather(file_stream)
                logging.info(f"File {file_name} read into DataFrame.")

                if 'date_trx_days' in df.columns:
                    def convert_to_date(val):
                        if isinstance(val, (int, float)):  # Jika nilai adalah Unix timestamp dalam milidetik
                            return pd.to_datetime(val / 1000, unit='s').strftime('%Y-%m-%d')
                        elif isinstance(val, (datetime.datetime, datetime.date)):  # Jika nilai sudah berupa tanggal
                            return val.strftime('%Y-%m-%d')
                        else:
                            return val  # Jika tipe data tidak diketahui, kembalikan nilai asli
                    
                    # Terapkan konversi pada kolom
                    df['date_trx_days'] = df['date_trx_days'].apply(convert_to_date)
            except Exception as e:
                logging.error(f"Error reading Feather file: {e}")
                return {"error": f"Error reading Feather file: {e}"}

            # (Lanjutkan dengan logika pengolahan data)
            if download:
                # Simpan DataFrame ke file Excel
                excel_stream = BytesIO()
                try:
                    with pd.ExcelWriter(excel_stream, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='Sheet1')
                    excel_stream.seek(0)

                    headers = {
                        'Content-Disposition': f'attachment; filename={file_name.replace(".feather", ".xlsx")}',
                        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    }
                    return Response(content=excel_stream.read(), headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                except Exception as e:
                    logging.error(f"Error generating Excel file: {e}")
                    return {"error": f"Error generating Excel file: {e}"}
            else:
                # Mengonversi DataFrame menjadi JSON
                try:
                    json_data = df.to_json(orient='records')
                    return {"data": json.loads(json_data)}
                except Exception as e:
                    logging.error(f"Error converting DataFrame to JSON: {e}")
                    return {"error": f"Error converting DataFrame to JSON: {e}"}

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return {"error": f"Unexpected error: {e}"}
        
    async def CreateFile(request: Request):
        try:
            request_data = await request.json()  # Extract JSON data from the request
            SalesRecapService.createFile(request_data)
            return {"status_code":200, "message": "success", "detail": "success create file"}
        except Exception as e:
            raise e
