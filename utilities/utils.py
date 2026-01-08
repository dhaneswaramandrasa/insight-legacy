import datetime as dt
from datetime import datetime
import pandas as pd
from fastapi import HTTPException
from typing import Any
from utilities.upload.S3_utils import S3_utils
import os, json,pytz,logging

def validate_date(date_str):
    try:
        return dt.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD or the Date doesn't exist")

def get_date_comparison(start_date_obj, end_date_obj):
    
    # Calculate the length of the date
    date_length = (end_date_obj - start_date_obj).days + 1

    start_comp_date = start_date_obj - dt.timedelta(days=date_length)
    end_comp_date = start_date_obj - dt.timedelta(days=1)
    
    return start_comp_date, end_comp_date

def get_summary(df, date_col, val_col, start_date_obj, end_date_obj):
    # Hitung Total All time
    total_all_time = int(df[val_col].sum())

    # Menghitung Tanggal untuk data komparasi
    start_date_comp, end_date_comp = get_date_comparison(start_date_obj, end_date_obj)

    # Ubah data string ke datetime type
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d")
    min_date = df[date_col].min()
    max_date = df[date_col].max()
    is_possible_to_compare = min_date <= start_date_comp
    is_possible_to_visualize = start_date_obj <= max_date

    if is_possible_to_visualize:
        # Data untuk Grafik filter data
        df_graph = df.loc[(df[date_col] >= start_date_obj) & (df[date_col] <= end_date_obj), :]
        df_graph = df_graph.groupby(date_col, as_index=False)[val_col].sum()
        
        total_this_period = int(df_graph[val_col].sum())
        max_val = int(df_graph[val_col].max())
        min_val = int(df_graph[val_col].min())
        mean_val = float(df_graph[val_col].mean())

        # DataFrame untuk Perbandingan
        if is_possible_to_compare:
            df_comparison = df.loc[(df[date_col] >= start_date_comp) & (df[date_col] <= end_date_comp), :]
            total_period_before = int(df_comparison[val_col].sum())
            try:
                growth_rate = (total_this_period - total_period_before) / (total_period_before) * 100
                growth_rate = float(growth_rate)
            except:
                growth_rate = float(100.0)
        else:
            growth_rate = float(100.0)
        
        df_graph = df_graph.loc[:, [date_col, val_col]]
        df_graph[date_col] = df_graph[date_col].dt.strftime("%Y-%m-%d")
        return total_this_period, total_all_time, growth_rate, max_val, min_val, mean_val, df_graph
    else:
        return 0, total_all_time, 0, 0, 0, 0, pd.DataFrame({date_col: [], val_col: []})
    
# @staticmethod
# def WriteFeatherFile(data: Any = None, filename: Any = None, columns: Any = None):
#     # ubah data menjadi dataframe pandas
#     df = pd.DataFrame(data, columns=columns)
#     # save dataframe ke file feather
#     result = df.to_feather(filename)
#     # print(f"Data tersimpan dalam file {filename}")
#     return result

# @staticmethod
def SetColumnForFeatherFile(flag: int=0):
     # Define columns
    if flag == 0:  # columnsSalesOrder
        columns = [
            "id", 
            "store_id",
            "total_amount",
            "total_cost_amount",
            "customer_id",
            "order_date",
            "order_time",
            "shipping_cost",
            "service_charge_amount",
            "tax_amount",
            "exchange_rate",
            "payment_amount_credit_payment",
            "payment_date_credit_payment",
            "total_amount_return",
            "total_cost_amount_return",
            "exchange_rate_return",
            "return_date",
            "order_status",
            "is_paid",
            "credit_payment_status",
            "sales_return_status",
            "payment_type_id",
            "order_source"
        ]
    elif flag == 1:  # columnsSalesOrderItem
        columns = [
            "id",
            "store_id",
            "product_id",
            "product_variant_id",
            "product_combo_id",
            "sales_order_id",
            "qty",
            "order_time",
            "status"
        ]
    elif flag == 2: #product
        columns = [
            "id",
            "store_id",
            "name", 
            "category_id",
            "status",
            "buy_price"
        ]
    elif flag == 3: #category
        columns = [
            "id",
            "name"
        ]

    elif flag == 4: #purchase order
        columns = [
            "id",
            "store_id",
            "purchase_date",
            "purchase_time",
            "discount_amount",
            "shipping_cost",
            "tax_amount",
            "exchange_rate",
            "status",
            "is_paid"
        ]
    elif flag == 5:
        columns = [
            "id",
            "trans_no",
            "amount",
            "store_id",
            "trans_date",
            "status",
            "status_trans_type"
        ]
    elif flag == 6: # storing data sales from Janvier
        columns = [
            "total_sales",
            "sales_projection",
            "sales_forecasting",
            "transaction_date"
        ]
    elif flag == 7: # stockInOutItems
        columns = [
           "store_id",
           "in_out_id",
            "product_id",
            "product_variant_id",
            "qty",
            "new_buy_price",
            "last_buy_price",
            "last_qty",
            "avg_buy_price",
            "date"
        ]
    elif flag == 8: # stockOpnameItems
        columns = [
            "store_id",
            "opname_id",
            "product_id",
            "product_variant_id",
            "qty",
            "qty_sys",
            "date" 
        ]
    elif flag == 9: # materialsProduct dan MaterialsProductVariant
        columns = [
            "id",
            "product_id",
            "product_variant_id",
            "material_product_id",
            "material_product_variant_id",
            "qty",
            "uom",
            "uom_conversion"
        ]
    elif flag == 10: # storing data profit from Janvier
        columns = [
            "laba",
            "laba_projection",
            "laba_forecasting",
            "transaction_date"
        ]
    elif flag == 11: # storing data product recommendation from Janvier
        columns = [
            "product_name",
            "product_category",
            "recommendation"
        ]
    elif flag == 12: # storing data product trend by day from Janvier
        columns = [
            "product_variant_concat",
            "day_index",
            "total_revenue",
            "gross_revenue",
            "total_qty"
        ]
    elif flag == 13: # storing data product trend by hour from Janvier
        columns = [
            "product_variant_concat",
            "hour_index",
            "total_revenue",
            "gross_revenue",
            "total_qty"
        ]
    elif flag == 14:
        columns = [
           	"id",
            "store_id",
            "name",
            "avatar",
            "phone",
            "total_trx",
            "total_spending",
            "order_time"
        ]

    elif flag == 15: 
        columns = [
            "order_id",
            "store_id",
            "order_date",
            "order_time",
            "discount_voucher_id" ,
            "total_amount"
        ]
    else:
        columns = []  # Tambahan default jika flag tidak 0 atau 1

    return columns

# 
def WriteFeatherFile(data: Any = None, filename: Any = None, columns: Any = None):
    # ubah data menjadi dataframe pandas
    df = pd.DataFrame(data, columns=columns)
    # save dataframe ke file feather
    result = df.to_feather(filename)
    # print(f"Data tersimpan dalam file {filename}")
    return result


def WrapperCreateFile(sourceData: Any = None, fileName: Any = None, columns: Any=None, aws_bucket: Any=None, path: Any=None):
    s3Utils = S3_utils
    WriteFeatherFile(sourceData, fileName, columns)
    s3Utils.uploadToS3(fileName,aws_bucket, path)

def uploadFile(fileName, aws_bucket, path):
    s3Utils = S3_utils
    s3Utils.uploadToS3(fileName,aws_bucket, path)


def read_log(log_file: Any):
    # Baca file log jika ada, jika tidak, buat log baru
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            return json.load(file)
    return {}

def write_log(log_data: Any, path: Any):
    # Tulis data log ke file
    with open(path, 'w') as file:
        json.dump(log_data, file, indent=4)

def LogginWithTimeZoneJakarta(log_filename, logger_name='custom_logger'):
    # Zona waktu Asia/Jakarta
    jakarta_timezone = pytz.timezone('Asia/Jakarta')

    def format_time(record, datefmt=None):
        # Mengonversi timestamp ke datetime di timezone Jakarta
        date_time = datetime.fromtimestamp(record.created, jakarta_timezone)
        if datefmt:
            s = date_time.strftime(datefmt)
        else:
            try:
                s = date_time.isoformat(timespec='milliseconds')
            except TypeError:
                s = date_time.isoformat()
        return s

    # Buat formatter dengan custom format_time
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    formatter.formatTime = format_time

    # Setup logging ke file
    handler = logging.FileHandler(log_filename)
    handler.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger

def WrapperCreateOrUpdateFile(sourceData: Any = None, fileName: Any = None, columns: Any = None, aws_bucket: Any = None, path: Any = None):
    s3Utils = S3_utils
    local_file_path = fileName  # Simpan sementara di lokal sebelum di-upload
    
    # Cek apakah file sudah ada di S3
    if s3Utils.checkFileExists(aws_bucket, f"{path}/{fileName}"):
        print(f"File {fileName} sudah ada di S3. Menggabungkan data...")

        # Download file dari S3 ke lokal
        s3Utils.downloadFromS3(fileName, aws_bucket, path)

        # Baca file Feather lama
        try:
            existing_df = pd.read_feather(local_file_path)
            print("Data Feather lama berhasil dibaca.")
        except Exception as e:
            print(f"Error membaca file Feather lama: {e}")
            return

        # Buat DataFrame baru dari sourceData
        new_df = pd.DataFrame(sourceData, columns=columns)

        # Debugging: Cek tipe data
        print("Tipe data existing_df:\n", existing_df.dtypes)
        print("Tipe data new_df:\n", new_df.dtypes)

        # Konversi tipe data jika tidak cocok
        for col in new_df.columns:
            if col in existing_df.columns and existing_df[col].dtype != new_df[col].dtype:
                new_df[col] = new_df[col].astype(existing_df[col].dtype)
        
        # Gabungkan data lama dengan data baru
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Simpan ulang file Feather dengan data yang sudah diperbarui
        combined_df.to_feather(local_file_path, version="2")  # Versi 2 lebih kompatibel
    else:
        print(f"File {fileName} belum ada di S3. Membuat file baru...")

        # Buat DataFrame baru dan simpan sebagai Feather
        WriteFeatherFile(sourceData, local_file_path, columns)

    # Upload file yang sudah diperbarui ke S3
    s3Utils.uploadToS3(local_file_path, aws_bucket, path)

    # Hapus file lokal setelah upload
    os.remove(local_file_path)
