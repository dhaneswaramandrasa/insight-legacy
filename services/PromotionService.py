from config.database import get_db_connection
from datetime import datetime
import pytz
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
import json,os
from dateutil.relativedelta import relativedelta

from models import StoreExtSettingsModel, SalesOrderModel

def CheckLogJson(logfile, store_id, log_date):
    try:

        # Cek apakah file log sudah ada
        # try:
        #     with open(logfile, "r") as file:
        #         logs = json.load(file)
        # except (FileNotFoundError, json.JSONDecodeError):
        #     logs = []
        path = "data-lake-python-feather/promotion/logs"
        aws_bucket= os.getenv('AWS_BUCKET')
        logs = S3_utils.readFileJsonFromS3(logfile, aws_bucket, path)
        if logs is None:
            logs = []
            # Cek apakah store_id sudah ada dalam log
        updated = False
        for log in logs:
            if log["store_id"] == store_id:
                log["log_date"] = log_date  # Update log_date ke 23:59:59
                updated = True
                break

          # Jika tidak ditemukan, tambahkan sebagai log baru
        if not updated:
            logs.append({"store_id": store_id, "log_date": log_date})

        # Simpan kembali ke file JSON
        with open(logfile, "w") as file:
            json.dump(logs, file, indent=4)

        # uploadFile(logfile, aws_bucket, path)
        print(f"Log untuk store_id {store_id} berhasil diperbarui ke {log_date}!")
    except Exception as error:
        raise error
    
def ReadLogJson(logfile, store_id):
    try:
        # Cek apakah file log di local ada
        # try:
        #     with open(logfile, "r") as file:
        #         logs = json.load(file)
        # except (FileNotFoundError, json.JSONDecodeError):
        #     print("File log tidak ditemukan atau kosong.")
        #     return None, None  # Jika file tidak ditemukan/kosong, return None

        # Membaca file log langsung dari S3
        path = "data-lake-python-feather/promotion/logs"
        aws_bucket= os.getenv('AWS_BUCKET')
        logs = S3_utils.readFileJsonFromS3(logfile, aws_bucket, path)

        # Jika file tidak ditemukan atau kosong
        if not logs:
            print("File log tidak ditemukan atau kosong.")
            return None, None  

        # Cari log berdasarkan store_id
        for log in logs:
            if log["store_id"] == store_id:
                return log["store_id"], log["log_date"]  # ✅ Return store_id dan log_date

        print(f"Log untuk store_id {store_id} tidak ditemukan.")
        return None, None  # Jika tidak ditemukan, return None

    except Exception as error:
        raise error

def DataPromotion():
    try:
        # Nama file JSON untuk menyimpan log
        LOG_FILE = "log.json"
        pathLog = "data-lake-python-feather/promotion/logs"

        directory = "data-lake-python-feather"
        aws_bucket= os.getenv('AWS_BUCKET')
        classStoreDataInsight = StoreExtSettingsModel.StoreExtSettings
        classSalesOrder = SalesOrderModel.SalesOrder
        getStore = classStoreDataInsight.GetStoreDataInsight()
       

        if not getStore:
            print("store not found")
            return
        
        timezone = pytz.timezone('Asia/Jakarta')
        currentDate = datetime.now(timezone)
        currentYear = currentDate.strftime('%Y') 

        # Buat tanggal awal tahun (1 Januari, jam 00:00:00)
        startOfYear = currentDate.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        startDate = startOfYear.strftime('%Y-%m-%d %H:%M:%S')
        print(startDate)
      
        # Ganti jam, menit, dan detik menjadi 23:59:59
        customDate = currentDate.replace(hour=23, minute=59, second=59, microsecond=0)
        endDate = customDate.strftime('%Y-%m-%d %H:%M:%S') #output 2025-02-13 23:59:59
        print(endDate)
       
        # exit()
        
        for row in getStore:
            store_id = row[0]
            
            fileName = f"{store_id}_{currentYear}_listorderpromo.feather"
            path = f"{directory}/promotion/{currentYear}"

            #check directory, if empty created dir, if not empty continue
            checkDir =  S3_utils.ensure_directory_exists(aws_bucket, path)

            checkLog = ReadLogJson(LOG_FILE,store_id)
            
            if checkLog[1]:
                print('ada log')
                startDateLog = checkLog[1].replace("23:59:59", "00:00:00")
            else:
                print('tak ada log')
                startDateLog = startDate

            sourceData = classSalesOrder.GetDataOrderPromo(store_id,startDateLog, endDate)
            
            CheckLogJson(LOG_FILE, store_id, endDate)
           
            if checkDir:
                print(sourceData)
                WrapperCreateFile(sourceData, fileName, SetColumnForFeatherFile(15), aws_bucket, path)
                # WrapperCreateOrUpdateFile(sourceData,fileName, SetColumnForFeatherFile(15),aws_bucket,path)

        uploadFile(LOG_FILE, aws_bucket, pathLog)
    except Exception as error:
        raise error
