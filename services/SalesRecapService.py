import os,time
import datetime as dt
from models import SalesRecapModel, StoreModel, ProductMaterialsModel, ProductVariantMaterialsModel, ProductModel, StoreExtSettingsModel
from services import ProjectionForecastingCheckService
from fastapi import HTTPException 
import pandas as pd
from typing import Any
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from config.s3 import download_file_from_s3
from config.bugsnag import get_logger
import os
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from sklearn.linear_model import Ridge # new
from sklearn.preprocessing import StandardScaler # new
from sklearn.pipeline import make_pipeline # new
from statsmodels.tsa.arima.model import ARIMA # new
import sys # new
import numpy as np # new
from config.s3 import download_json_file_from_s3

load_dotenv('.env')
logger = get_logger()

from config.database import get_db_connection, get_db_connection_second, get_db_connection_third
conn_generator = get_db_connection()
conn = next(conn_generator)

class SalesRecapService():
    directory = "data-lake-python-feather"

    @staticmethod
    def GetSalesDataVision(payload = None):
        try:
            #store
            # store = StoreModel.Store
            # dataStore = store.GetIndexData()

            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()

            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
        
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/sales-tracking-vision"
            # salesRecap = SalesRecapModel.SalesRecap
            currentYear = datetime.now().year
            
            # Define columns
            columns = ["store_id", "total_amount", "date_trx_days","trx_sales_id_count","flag"]
            #looping data
            for row in dataStore:
                # print(row[0])
                store_id = row[0] #penulisannya variable inline store_id milik row[0], created_date milik row[1]
                # Hitung end_date berdasarkan created_date
                # start_date = created_date.strftime("%Y-%m")
                checkLog = salesRecap.CheckDataRecordLog(store_id,'SalesTrackingVision')
               
                if checkLog:
                    currentDateLog = checkLog[2]
                    newDateLog = currentDateLog + relativedelta(years=1)
                    newDate = newDateLog.strftime("%Y-%m-%d")
                    newDateYear = int(newDateLog.strftime("%Y"))

                    if newDateYear > currentYear:
                        date_logYear = currentYear
                        date_log = datetime.now().strftime("%Y-%m-%d")
                    else:
                        date_logYear = newDateYear
                        date_log = newDate
                else:
                    date_logYear = datetime.now().year
                    date_log = datetime.now().strftime("%Y-%m-%d")
                
                data = salesRecap.SalesTrackingVision(store_id, date_logYear)  # Menunggu hingga GetIndexData selesai

                if data is None:
                    raise ValueError(f"SalesTrackingVision returned None for store_id: {store_id} and year: {date_logYear}")

                filename = f"{store_id}_{date_logYear}_sales_tracking_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns)  # Menunggu hingga WriteFeatherFile selesai
                if s3Utils.uploadToS3(filename,aws_bucket, path): #call utilities s3 dan passing, if jika berhasil upload
                    SalesRecapService.CheckAndUpdateRecord(store_id, date_log, 'SalesTrackingVision') # Check and update record
        except Exception as error:
            raise error
        
    @staticmethod
    def CheckAndUpdateRecord(store_id, end_date_log, modul):
        salesRecap = SalesRecapModel.SalesRecap
        # Cek apakah record sudah ada
        if salesRecap.CheckDataRecordLog(store_id, modul):
            # Jika sudah ada, lakukan update
            salesRecap.UpdateRecordLog(store_id, end_date_log,modul)
        else:
            # Jika belum ada, lakukan create
            salesRecap.CreateRecordLog(store_id, end_date_log,modul)
        return True

    @staticmethod
    def GetProductVision(payload = None):
        try: 
            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()
           
            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
            
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/product-populer-vision"
            currentYear = datetime.now().year
            # print(currentYear)
            columns = [
                "store_id", 
                "product_id", 
                "product_variant_id",
                "product_combo_id",
                "total_qty_return",
                "total_cost_amount_return",
                "total_qty",
                "total_amount",
                "date_trx_days",
                "total_amount_return",
                "total_cost_amount",
                "total_qty_from_item_combo",
                "total_qty_unpaid",
	            "total_cost_amount_unpaid",
	            "total_amount_unpaid"
                ]
            #loop data
            # print(dataStore)
            for row in dataStore:
                store_id  = row[0]
                checkLog = salesRecap.CheckDataRecordLog(store_id,'ProductVision')
                # print(checkLog)
                # print('test')
                # print(store_id)
                if checkLog:
                    currentDateLog = checkLog[2]
                    newDateLog = currentDateLog + relativedelta(years=1)
                    newDate = newDateLog.strftime("%Y-%m-%d")
                    newDateYear = int(newDateLog.strftime("%Y"))

                    if newDateYear > currentYear:
                        date_logYear = currentYear
                        date_log = datetime.now().strftime("%Y-%m-%d")
                    else:
                        print("masuuk else else")
                        date_logYear = newDateYear
                        date_log = newDate
                else:
                    date_logYear = datetime.now().year
                    date_log = datetime.now().strftime("%Y-%m-%d")
                data = salesRecap.ProductPopulerVision(store_id, date_logYear)
                # print(data)
                filename = f"{store_id}_{date_logYear}_product_populer_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns) #write/create data into file feather
                # print(filename)
                # print(data)
                if s3Utils.uploadToS3(filename, aws_bucket, path): #upload file to s3 aws
                    SalesRecapService.CheckAndUpdateRecord(store_id, date_log, 'ProductVision') # Check and update record
        except Exception as error:
            # print(f"Error encountered: {error}")
            raise error

    @staticmethod
    def GetCustomerVision():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()

            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
            
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/customer-tracking-vision"
            # Define columns
            columns = ["store_id", "all_sales_order_id_count","date_trx_days"]

            #loop data
            for row in dataStore:
                store_id, created_date = row[0], row[1]
                date_log = created_date.strftime("%Y")
                data = salesRecap.CustomerTrackingVision(store_id, date_log)
                filename = f"{store_id}_{date_log}_customer_tracking_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns) #write/create data into file feather
                s3Utils.uploadToS3(filename, aws_bucket, path) #upload file to s3 aws
                # os.remove(filename) #remove file temporary after created and uploaded is finished
        except Exception as error:
            raise error
        
    @staticmethod
    def GetAllSummarySales(payload = None):
        try:
            # store = StoreModel.Store
            # dataStore = store.GetIndexData()
            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()

            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
            
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/summary-sales-vision"
            # Define columns
            columns = ["store_id", "total_amount","all_trx_id_count","flag"]
            # Mendapatkan tahun saat ini
            current_year = dt.datetime.now().year
            # Tanggal awal tahun
            start_of_year = dt.date(current_year, 1, 1)
            # Tanggal akhir tahun
            end_of_year = dt.date(current_year, 12, 31)

            #loop data
            for row in dataStore:
                store_id= row[0]
                data = salesRecap.GetAllSummarySales(store_id, start_of_year, end_of_year)
                filename = f"{store_id}_summary_sales_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns) #write/create data into file feather
                s3Utils.uploadToS3(filename, aws_bucket, path) #upload file to s3 aws
        except Exception as error:
            raise error
        
    @staticmethod
    def GetAllSummarySalesProduct(payload = None):
        try:
            # store = StoreModel.Store
            # dataStore = store.GetIndexData()

            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()

            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
        
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/summary-sales-product-vision"
            # Define columns
            columns = [
                    "store_id",
                    "total_qty",
                    "total_qty_return",
                    "flag",
                    "total_qty_from_item_combo",
                    "total_cost_amount",
                    "total_amount",
                    "total_qty_unpaid",
                    "total_cost_amount_unpaid",
                    "total_amount_unpaid",
                    "total_cost_amount_return"
                ]
            # Mendapatkan tahun saat ini
            current_year = dt.datetime.now().year
            # Tanggal awal tahun
            start_of_year = dt.date(current_year, 1, 1)
            # Tanggal akhir tahun
            end_of_year = dt.date(current_year, 12, 31)

            #loop data
            for row in dataStore:
                store_id= row[0]
                data = salesRecap.GetAllSummarySalesProduct(store_id, start_of_year, end_of_year)
                filename = f"{store_id}_summary_sales_product_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns) #write/create data into file feather
                s3Utils.uploadToS3(filename, aws_bucket, path) #upload file to s3 aws
        except Exception as error:
            raise error
    
    @staticmethod
    def WriteFeatherFile(data: Any = None, filename: Any = None, columns: Any = None):
        # ubah data menjadi dataframe pandas
        df = pd.DataFrame(data, columns=columns)
        # save dataframe ke file feather
        result = df.to_feather(filename)
        # print(f"Data tersimpan dalam file {filename}")
        return result

    def createFile(request):
        try:
            s3Utils = S3_utils
            aws_bucket = os.getenv('AWS_BUCKET')
            basePath = f"{SalesRecapService.directory}"
            modelStore = StoreModel.Store
            salesRecap = SalesRecapModel.SalesRecap
            # print(request)
            storeid = request.get('store_id')
            modul = request.get("modul")
            date = request.get("date")

            dataStore = modelStore.FindStore(storeid)
            store_id = dataStore[0]

            if modul == "product":
                path = f"{basePath}/product-populer-vision"
                 # Define columns
                columns = [
                        "store_id",
                        "product_id",
                        "product_variant_id",
                        "product_combo_id",
                        "total_qty_return",
                        "total_cost_amount_return",
                        "total_qty",
                        "total_amount",
                        "date_trx_days",
                        "total_amount_return",
                        "total_cost_amount",
                        "total_qty_from_item_combo",
                        "total_qty_unpaid",
                        "total_cost_amount_unpaid",
                        "total_amount_unpaid"
                    ]
                data = salesRecap.ProductPopulerVision(store_id, date)
                filename = f"{store_id}_{date}_product_populer_vision.feather"
            elif modul == "sales":
                path = f"{basePath}/sales-tracking-vision"
                columns = ["store_id", "total_amount", "date_trx_days","flag"]
                data = salesRecap.SalesTrackingVision(store_id)
                filename = f"{store_id}_sales_tracking_vision.feather"
            elif modul == "customer":
                path = f"{basePath}customer-tracking-vision"
                columns = ["store_id", "all_sales_order_id_count","date_trx_days"]
                data = salesRecap.CustomerTrackingVision(store_id)
                filename = f"{store_id}_customer_tracking_vision.feather"

            SalesRecapService.WriteFeatherFile(data, filename, columns)  # write/create data into file feather
            s3Utils.uploadToS3(filename, aws_bucket, path)  # upload file to s3 aws
            # os.remove(filename)  # remove file temporary after created and uploaded is finished
            return True
        except Exception as error:
            raise error
        
    @staticmethod
    def GetDepositVision(payload = None):
        try:
            #store
            # store = StoreModel.Store
            # dataStore = store.GetIndexData()
            if payload is not None:
                # Ambil store_id dari payload
                store_id = payload.store_id
                dataStore = [[store_id]]  # Mengubah store_id menjadi format yang diinginkan
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()
            
            if not dataStore:
                raise ValueError("dataStore is empty or None. Stopping execution.")
            
            salesRecap = SalesRecapModel.SalesRecap
            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{SalesRecapService.directory}/sales-deposit-tracking-vision"
            currentYear = datetime.now().year

            # Define columns
            columns = ["store_id", "total_deposit", "transaction_date_deposit"]
            #looping data
            for row in dataStore:
                # print(row[0])
                store_id= row[0]#penulisannya variable inline store_id milik row[0], created_date milik row[1]
                checkLog = salesRecap.CheckDataRecordLog(store_id,'SalesDeposit')
                
                if checkLog:
                    currentDateLog = checkLog[2]
                    newDateLog = currentDateLog + relativedelta(years=1)
                    newDate = newDateLog.strftime("%Y-%m-%d")
                    newDateYear = int(newDateLog.strftime("%Y"))

                    if newDateYear > currentYear:
                        date_logYear = currentYear
                        date_log = datetime.now().strftime("%Y-%m-%d")
                    else:
                        date_logYear = newDateYear
                        date_log = newDate
                else:
                    date_logYear = datetime.now().year
                    date_log = datetime.now().strftime("%Y-%m-%d")

                data = salesRecap.GetDeposit(store_id, date_logYear)  # Menunggu hingga GetIndexData selesai
                filename = f"{store_id}_{date_logYear}_deposit_tracking_vision.feather"
                SalesRecapService.WriteFeatherFile(data, filename, columns)  # Menunggu hingga WriteFeatherFile selesai

                if s3Utils.uploadToS3(filename,aws_bucket, path): #call utilities s3 dan passing, if jika berhasil upload
                    SalesRecapService.CheckAndUpdateRecord(store_id, date_log, 'SalesDeposit') # Check and update record
                else:
                    print("Failed to upload to S3. Skipping record update.")

        except Exception as error:
            raise error

    # def GetSalesProjectionForecasting(store_id: int):
    #     current_date = datetime.now()
    #     now_wib = current_date + timedelta(hours=7)
    #     start_date_training = (now_wib - timedelta(days=181)).date() # karena ada lag 1 hari data rekap
    #     end_date_training = (now_wib - timedelta(days=1)).date() # karena ada lag 1 hari data rekap
    #     start_year_penarikan = start_date_training.year
    #     end_year_penarikan = end_date_training.year
    #     years_raw = [start_year_penarikan, end_year_penarikan]

    #     seen = set()
    #     years = []

    #     for item in years_raw:
    #         if item not in seen:
    #             seen.add(item)
    #             years.append(item)

    #     this_month_beginning = now_wib.replace(day = 1)
    #     future_dates = pd.date_range(start=this_month_beginning, periods=93, freq='D').date
        
    #     # Download Data Sales
    #     dfs = []
    #     for year in years:
    #         try:
    #             path_sales = f"{SalesRecapService.directory}/sales-tracking-vision/{store_id}_{year}_sales_tracking_vision.feather"
    #             df_sales = download_file_from_s3(path_sales)
    #             dfs.append(df_sales)
    #             # print(dfs.columns.tolist())
    #             # print(path_sales)
    #         except:
    #             continue
    #     # print('bismillah')
    #     # exit
    #     if dfs:
    #         df_sales = pd.concat(dfs)
    #         sales = df_sales.loc[df_sales['flag'] == 0, :].rename(columns={'total_amount': 'total_sales'})
    #         refunds = df_sales.loc[df_sales['flag'] == 1, ["total_amount", "date_trx_days"]].rename(columns={'total_amount': 'total_refund'})
    #         if refunds.empty:
    #             merged = df_sales.copy()
    #             merged['total'] = merged['total_sales']
    #         else:
    #             merged = pd.merge(sales, refunds, on=['date_trx_days'], how='outer')
    #             merged['total_sales'].fillna(0, inplace=True)
    #             merged['total_refund'].fillna(0, inplace=True)
    #             merged["total"] = merged["total_sales"] - merged["total_refund"]
    #         df_sales = merged.groupby(["date_trx_days"], as_index = False)["total"].sum()
    #         df_sales['date_trx_days'] = pd.to_datetime(df_sales['date_trx_days'])
    #         df_sales['total'] = df_sales['total'].astype(float)
    #         start_date_training = pd.to_datetime(start_date_training)
    #         end_date_training = pd.to_datetime(end_date_training)
    #         training_date_range = pd.date_range(start = start_date_training, end = end_date_training)
    #         df_complete = pd.DataFrame(training_date_range, columns=['date_trx_days'])
    #         df_sales_complete = pd.merge(df_complete, df_sales, on='date_trx_days', how='left')
    #         df_sales_complete['total'].fillna(0, inplace = True)
    #         df_sales_complete = df_sales_complete[(df_sales_complete["date_trx_days"] >= start_date_training) & (df_sales_complete["date_trx_days"] <= end_date_training)]
    #         df_sales_complete = df_sales_complete.rename(columns = {
    #             'total': 'total_amount'
    #         })
    #         train = df_sales_complete.reset_index()
    #         train.set_index("date_trx_days", inplace = True)
    #         train.index.name = "date_trx_days"

    #         # # Calculate mean and standard deviation
    #         # mean = train["total_amount"].mean()
    #         # std = train["total_amount"].std()

    #         # # Identify outliers using Z-scores threshold
    #         # outliers_mask = ((train["total_amount"] - mean).abs() / std) <= 3

    #         # # Filter out outliers
    #         # train_filtered = train[outliers_mask]
    #         train_filtered = train # comment this line jika jadi menggunakan outliers dan uncomment line code di atas

    #         # Create lag features
    #         train_filtered['lag_30'] = train_filtered["total_amount"].shift(30)
    #         train_filtered.dropna(inplace=True)

    #         # Features and target
    #         X_train = train_filtered[['lag_30']]
    #         y_train = train_filtered["total_amount"]

    #         # Initialize Ridge with a pipeline
    #         pipeline = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    #         pipeline.fit(X_train, y_train)

    #         future_df = pd.DataFrame({'date_trx_days': future_dates})
    #         future_df['lag_30'] = 0

    #         # Iteratively update the lag feature
    #         last_total_amount = train_filtered["total_amount"].iloc[-30:]
    #         for i in range(len(future_df)):
    #             if i < 30:
    #                 future_df.at[i, 'lag_30'] = last_total_amount.values[i]
    #             else:
    #                 future_df.at[i, 'lag_30'] = y_pred_projection[i - 30]

    #             X_future = future_df[['lag_30']].iloc[:i+1]
    #             y_pred_projection = pipeline.predict(X_future)
    #         y_pred_1 = pd.DataFrame({
    #             'date_trx_days': future_dates,
    #             'sales_projection': y_pred_projection
    #         })

    #         # Create future dataframe to predict until two month ahead
    #         p = 5
    #         d = 1
    #         q = 0

    #         model_2 = ARIMA(train_filtered["total_amount"], order = (p, d, q))

    #         model_2_fit = model_2.fit()

    #         # Forecast on the future set
    #         forecast_output = model_2_fit.forecast(steps = 93)
    #         y_pred_2 = pd.DataFrame({
    #             'date_trx_days': future_dates,
    #             'sales_forecasting': forecast_output
    #         })
    #         train_filtered = train_filtered.reset_index()
    #         train_filtered['date_trx_days'] = pd.to_datetime(train_filtered['date_trx_days'])
    #         y_pred_1['date_trx_days'] = pd.to_datetime(y_pred_1['date_trx_days'])
    #         y_pred_2['date_trx_days'] = pd.to_datetime(y_pred_2['date_trx_days'])
    #         # Concatenate the training and prediction data
    #         combined_df_1 = pd.merge(train_filtered, y_pred_1, how = "outer", on = "date_trx_days")
    #         combined_df = pd.merge(combined_df_1, y_pred_2, how = "outer", on = "date_trx_days")

    #         combined_df = combined_df.rename(columns = {
    #             'total_amount': 'total_sales',
    #             'date_trx_days': 'transaction_date'
    #         })

    #         df_sales = combined_df[['total_sales', 'sales_projection', 'sales_forecasting', 'transaction_date']]
    #         df_sales['total_sales'].fillna(0, inplace = True)
    #         df_sales['sales_projection'].fillna(0, inplace = True)
    #         df_sales['sales_forecasting'].fillna(0, inplace = True)

    #     else:
    #         df_sales = pd.DataFrame()

    #     df_sales_to_list = list(df_sales.itertuples(index = False, name = None)) # ubah DataFrame menjadi tuple di dalam list
        
    #     return df_sales_to_list

    # # Bikin function baru panggil GetSalesProjectionForecasting
    # def CreateFileFeatherSalesProjectionForecasting():
    #     try:
    #         store = StoreModel.Store
    #         dataStore = store.GetIndexData()
    #         classSalesRecapService = SalesRecapService
    #         bucket_name= os.getenv('AWS_BUCKET')

    #         for row in dataStore:
    #             store_id = row[0]
    #             # sales projection forecasting
    #             salesProjectionForecasting = classSalesRecapService.GetSalesProjectionForecasting(store_id)
    #             file_name = f"{store_id}_sales_projection_and_forecasting.feather"
    #             file_path = f"{SalesRecapService.directory}/ml/sales"
    #             WrapperCreateFile(sourceData = salesProjectionForecasting,
    #                             fileName = file_name,
    #                             columns = SetColumnForFeatherFile(6),
    #                             aws_bucket = bucket_name,
    #                             path = file_path)

    #     except Exception as error:
    #         raise error
    
    # def GetProfitProjectionForecasting(store_id: int):
    #     # try:
    #     current_date = datetime.now()
    #     now_wib = current_date + timedelta(hours=7)
    #     start_date_training = (now_wib - timedelta(days=180)).date()
    #     end_date_training = now_wib.date()
        
    #     this_month_beginning = now_wib.replace(day = 1)
    #     future_dates = pd.date_range(start=this_month_beginning, periods=93, freq='D').date

    #     start_date_training = pd.to_datetime(start_date_training)
    #     end_date_training = pd.to_datetime(end_date_training)
    #     training_date_range = pd.date_range(start = start_date_training, end = end_date_training)

    #     # Download Data Sales
    #     dfs_1 = []
    #     path_sales = f"{SalesRecapService.directory}/sales-order/{store_id}_sales_order_6_months_ago.feather"
        
    #     df_sales = download_file_from_s3(path_sales)
    #     if df_sales is not None:
    #         dfs_1.append(df_sales)

    #         if dfs_1 and not dfs_1[0].empty:
    #             df_sales = pd.concat(dfs_1)
    #             df_for_revenue_non_shipping = df_sales.copy()
    #             df_for_revenue_shipping = df_sales.copy()
    #             df_for_revenue_service_charge = df_sales.copy()
    #             df_for_revenue_tax = df_sales.copy()
    #             df_for_goods_cost = df_sales.copy()
    #             df_sales_credit_payment = df_sales.copy()
    #             df_sales_return = df_sales.copy()

    #             df_for_revenue_non_shipping = df_for_revenue_non_shipping[~df_for_revenue_non_shipping['order_status'].isin(['X', 'D'])]
    #             df_for_revenue_non_shipping = df_for_revenue_non_shipping[df_for_revenue_non_shipping['is_paid'] == 1]
    #             df_for_revenue_non_shipping = df_for_revenue_non_shipping[df_for_revenue_non_shipping['payment_type_id'] != 'CT']
    #             df_for_revenue_non_shipping = df_for_revenue_non_shipping[['order_date', 'total_amount', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate']]
    #             df_for_revenue_non_shipping['revenue_non_shipping'] = (df_for_revenue_non_shipping['total_amount'] - df_for_revenue_non_shipping['shipping_cost'] - df_for_revenue_non_shipping['service_charge_amount'] - df_for_revenue_non_shipping['tax_amount'])/df_for_revenue_non_shipping['exchange_rate']
    #             df_for_revenue_shipping = df_for_revenue_shipping[~df_for_revenue_shipping['order_status'].isin(['X', 'D'])]
    #             df_for_revenue_shipping = df_for_revenue_shipping[df_for_revenue_shipping['is_paid'] == 1]
    #             df_for_revenue_shipping = df_for_revenue_shipping[['order_date', 'shipping_cost', 'exchange_rate']]
    #             df_for_revenue_shipping['revenue_shipping'] = df_for_revenue_shipping['shipping_cost']/df_for_revenue_shipping['exchange_rate']
    #             df_for_revenue_service_charge = df_for_revenue_service_charge[~df_for_revenue_service_charge['order_status'].isin(['X', 'D'])]
    #             df_for_revenue_service_charge = df_for_revenue_service_charge[df_for_revenue_service_charge['is_paid'] == 1]
    #             df_for_revenue_service_charge = df_for_revenue_service_charge[df_for_revenue_service_charge['payment_type_id'] != 'CT']
    #             df_for_revenue_service_charge = df_for_revenue_service_charge[['order_date', 'service_charge_amount', 'exchange_rate']]
    #             df_for_revenue_service_charge['revenue_service_charge'] = df_for_revenue_service_charge['service_charge_amount']/df_for_revenue_service_charge['exchange_rate']
    #             df_for_revenue_tax = df_for_revenue_tax[~df_for_revenue_tax['order_status'].isin(['X', 'D'])]
    #             df_for_revenue_tax = df_for_revenue_tax[df_for_revenue_tax['is_paid'] == 1]
    #             df_for_revenue_tax = df_for_revenue_tax[df_for_revenue_tax['payment_type_id'] != 'CT']
    #             df_for_revenue_tax = df_for_revenue_tax[['order_date', 'tax_amount', 'exchange_rate']]
    #             df_for_revenue_tax['revenue_tax'] = df_for_revenue_tax['tax_amount']/df_for_revenue_tax['exchange_rate']
    #             df_for_goods_cost = df_for_goods_cost[~df_for_goods_cost['order_status'].isin(['X', 'D'])]
    #             df_for_goods_cost = df_for_goods_cost[df_for_goods_cost['is_paid'] == 1]
    #             df_for_goods_cost = df_for_goods_cost[['order_date', 'total_cost_amount', 'exchange_rate']]
    #             df_for_goods_cost['goods_cost'] = df_for_goods_cost['total_cost_amount']/df_for_goods_cost['exchange_rate']

    #             df_for_revenue_non_shipping = df_for_revenue_non_shipping.groupby(["order_date"], as_index=False)["revenue_non_shipping"].sum()
    #             df_for_revenue_shipping = df_for_revenue_shipping.groupby(["order_date"], as_index=False)["revenue_shipping"].sum()
    #             df_for_revenue_service_charge = df_for_revenue_service_charge.groupby(["order_date"], as_index = False)["revenue_service_charge"].sum()
    #             df_for_revenue_tax = df_for_revenue_tax.groupby(["order_date"], as_index = False)["revenue_tax"].sum()
    #             df_for_goods_cost = df_for_goods_cost.groupby(["order_date"], as_index = False)["goods_cost"].sum()

    #             df_for_revenue_non_shipping['order_date'] = pd.to_datetime(df_for_revenue_non_shipping['order_date'])
    #             df_for_revenue_shipping['order_date'] = pd.to_datetime(df_for_revenue_shipping['order_date'])
    #             df_for_revenue_service_charge['order_date'] = pd.to_datetime(df_for_revenue_service_charge['order_date'])
    #             df_for_revenue_tax['order_date'] = pd.to_datetime(df_for_revenue_tax['order_date'])
    #             df_for_goods_cost['order_date'] = pd.to_datetime(df_for_goods_cost['order_date'])

    #             df_for_revenue_non_shipping["revenue_non_shipping"] = df_for_revenue_non_shipping["revenue_non_shipping"].astype(float)
    #             df_for_revenue_shipping["revenue_shipping"] = df_for_revenue_shipping["revenue_shipping"].astype(float)
    #             df_for_revenue_service_charge["revenue_service_charge"] = df_for_revenue_service_charge["revenue_service_charge"].astype(float)
    #             df_for_revenue_tax["revenue_tax"] = df_for_revenue_tax["revenue_tax"].astype(float)
    #             df_for_goods_cost["goods_cost"] = df_for_goods_cost["goods_cost"].astype(float)
                
    #             df_complete_1 = pd.DataFrame(training_date_range, columns=['order_date'])

    #             df_join_1_1 = pd.merge(df_for_revenue_non_shipping, df_for_revenue_shipping, on = 'order_date', how = 'left')
    #             df_join_1_2 = pd.merge(df_join_1_1, df_for_revenue_service_charge, on = 'order_date', how = 'left')
    #             df_join_1_3 = pd.merge(df_join_1_2, df_for_revenue_tax, on = 'order_date', how = 'left')
    #             df_join_1_4 = pd.merge(df_join_1_3, df_for_goods_cost, on = 'order_date', how = 'left')

    #             df_sales_complete = pd.merge(df_complete_1, df_join_1_4, on = 'order_date', how = 'left')
    #             df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']] = df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']].fillna(0)
    #             df_sales_complete = df_sales_complete[(df_sales_complete["order_date"] >= start_date_training) & (df_sales_complete["order_date"] <= end_date_training)]
    #             df_sales_complete = df_sales_complete.rename(columns = {
    #                 'order_date': 'date_trx_days'
    #             })
    #         else:
    #             df_sales = pd.DataFrame(columns=['order_date', 'revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost'])
    #             df_sales_credit_payment = pd.DataFrame(columns=[
    #                 'payment_date_credit_payment', 
    #                 'payment_amount_credit_payment',
    #                 'order_status',
    #                 'credit_payment_status'])
    #             df_sales_return = pd.DataFrame(columns=[
    #                 'return_date',
    #                 'total_amount_return',
    #                 'total_cost_amount_return',
    #                 'exchange_rate',
    #                 'sales_return_status'
    #                 ])
    #             df_complete_1 = pd.DataFrame(training_date_range, columns=['order_date'])
    #             if 'order_date' not in df_sales.columns:
    #                 df_sales['order_date'] = pd.NaT
    #             df_sales_complete = pd.merge(df_complete_1, df_sales, on='order_date', how='left')
    #             df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']] = df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']].fillna(0)
    #             df_sales_complete = df_sales_complete[(df_sales_complete["order_date"] >= start_date_training) & (df_sales_complete["order_date"] <= end_date_training)]
    #             df_sales_complete = df_sales_complete.rename(columns = {
    #                 'order_date': 'date_trx_days'
    #             })
            
    #     dfs_2 = []
    #     path_purchase_order = f"{SalesRecapService.directory}/purchase-order/{store_id}_purchase_order_6_months_ago.feather"
    #     df_purchase_order = download_file_from_s3(path_purchase_order)
    #     dfs_2.append(df_purchase_order)

    #     if dfs_2 and not dfs_2[0].empty:
    #         df_purchase_order = pd.concat(dfs_2)
    #         df_for_purchases_discount = df_purchase_order.copy()
    #         df_for_purchases_shipping = df_purchase_order.copy()
    #         df_for_purchases_tax = df_purchase_order.copy()

    #         df_for_purchases_discount = df_for_purchases_discount[~df_for_purchases_discount['status'].isin(['X', 'D'])]
    #         df_for_purchases_discount = df_for_purchases_discount[['purchase_date', 'discount_amount']]
    #         df_for_purchases_discount['purchases_discount'] = df_for_purchases_discount['discount_amount']
    #         df_for_purchases_shipping = df_for_purchases_shipping[~df_for_purchases_shipping['status'].isin(['X', 'D'])]
    #         df_for_purchases_shipping = df_for_purchases_shipping[df_for_purchases_shipping['is_paid'] == 1]
    #         df_for_purchases_shipping = df_for_purchases_shipping[['purchase_date', 'shipping_cost', 'exchange_rate']]
    #         df_for_purchases_shipping['purchases_shipping'] = df_for_purchases_shipping['shipping_cost']/df_for_purchases_shipping['exchange_rate']
    #         df_for_purchases_tax = df_for_purchases_tax[~df_for_purchases_tax['status'].isin(['X', 'D'])]
    #         df_for_purchases_tax = df_for_purchases_tax[df_for_purchases_tax['is_paid'] == 1]
    #         df_for_purchases_tax = df_for_purchases_tax[['purchase_date', 'tax_amount', 'exchange_rate']]
    #         df_for_purchases_tax['purchases_tax'] = df_for_purchases_tax['tax_amount']/df_for_purchases_tax['exchange_rate']

    #         df_for_purchases_discount = df_for_purchases_discount.groupby(["purchase_date"], as_index = False)['purchases_discount'].sum()
    #         df_for_purchases_shipping = df_for_purchases_shipping.groupby(["purchase_date"], as_index = False)['purchases_shipping'].sum()
    #         df_for_purchases_tax = df_for_purchases_tax.groupby(["purchase_date"], as_index = False)['purchases_tax'].sum()

    #         df_for_purchases_discount['purchase_date'] = pd.to_datetime(df_for_purchases_discount['purchase_date'])
    #         df_for_purchases_shipping['purchase_date'] = pd.to_datetime(df_for_purchases_shipping['purchase_date'])
    #         df_for_purchases_tax['purchase_date'] = pd.to_datetime(df_for_purchases_tax['purchase_date'])

    #         df_for_purchases_discount['purchases_discount'] = df_for_purchases_discount['purchases_discount'].astype(float)
    #         df_for_purchases_shipping['purchases_shipping'] = df_for_purchases_shipping['purchases_shipping'].astype(float)
    #         df_for_purchases_tax['purchases_tax'] = df_for_purchases_tax['purchases_tax'].astype(float)

    #         df_complete_2 = pd.DataFrame(training_date_range, columns=['purchase_date'])

    #         df_join_2_1 = pd.merge(df_for_purchases_discount, df_for_purchases_shipping, on ='purchase_date', how = 'left')
    #         df_join_2_2 = pd.merge(df_join_2_1, df_for_purchases_tax, on = 'purchase_date', how = 'left')

    #         df_purchase_order_complete = pd.merge(df_complete_2, df_join_2_2, on = 'purchase_date', how = 'left')
    #         df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']] = df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']].fillna(0)
    #         df_purchase_order_complete = df_purchase_order_complete[(df_purchase_order_complete["purchase_date"] >= start_date_training) & (df_purchase_order_complete["purchase_date"] <= end_date_training)]
    #         df_purchase_order_complete = df_purchase_order_complete.rename(columns = {
    #             'purchase_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_sales_complete, df_purchase_order_complete, on = 'date_trx_days')
    #     else:
    #         df_purchase_order = pd.DataFrame(columns=['purchase_date', 'purchases_discount', 'purchases_shipping', 'purchases_tax'])
    #         df_complete_2 = pd.DataFrame(training_date_range, columns=['purchase_date'])
    #         if 'purchase_date' not in df_purchase_order.columns:
    #             df_purchase_order['purchase_date'] = pd.NaT
    #         df_purchase_order_complete = pd.merge(df_complete_2, df_purchase_order, on='purchase_date', how='left')
    #         df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']] = df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']].fillna(0)
    #         df_purchase_order_complete = df_purchase_order_complete[(df_purchase_order_complete["purchase_date"] >= start_date_training) & (df_purchase_order_complete["purchase_date"] <= end_date_training)]
    #         df_purchase_order_complete = df_purchase_order_complete.rename(columns = {
    #             'purchase_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_sales_complete, df_purchase_order_complete, on = 'date_trx_days')
        
    #     df_sales_credit_payment = df_sales_credit_payment[~df_sales_credit_payment['order_status'].isin(['X', 'D'])]
    #     df_sales_credit_payment = df_sales_credit_payment[df_sales_credit_payment['credit_payment_status'] != 'X']
    #     df_sales_credit_payment['revenue_credit_payment'] = df_sales_credit_payment['payment_amount_credit_payment']
    #     df_sales_credit_payment = df_sales_credit_payment.groupby(["payment_date_credit_payment"], as_index=False)["revenue_credit_payment"].sum()

    #     if not df_sales_credit_payment.empty:
    #         df_sales_credit_payment['payment_date_credit_payment'] = pd.to_datetime(df_sales_credit_payment['payment_date_credit_payment'])
    #         df_sales_credit_payment['revenue_credit_payment'] = df_sales_credit_payment['revenue_credit_payment'].astype(float)
    #         df_complete_3 = pd.DataFrame(training_date_range, columns=['payment_date_credit_payment'])
    #         df_sales_credit_payment_complete = pd.merge(df_complete_3, df_sales_credit_payment, on='payment_date_credit_payment', how='left')
    #         df_sales_credit_payment_complete['revenue_credit_payment'] = df_sales_credit_payment_complete['revenue_credit_payment'].fillna(0)
    #         df_sales_credit_payment_complete = df_sales_credit_payment_complete[(df_sales_credit_payment_complete["payment_date_credit_payment"] >= start_date_training) & (df_sales_credit_payment_complete["payment_date_credit_payment"] <= end_date_training)]
    #         df_sales_credit_payment_complete = df_sales_credit_payment_complete.rename(columns = {
    #             'payment_date_credit_payment': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_credit_payment_complete, on = 'date_trx_days')
    #     else:
    #         df_sales_credit_payment = pd.DataFrame(columns=['payment_date_credit_payment', 'revenue_credit_payment'])
    #         df_complete_3 = pd.DataFrame(training_date_range, columns=['payment_date_credit_payment'])
    #         if 'payment_date_credit_payment' not in df_sales_credit_payment.columns:
    #             df_sales_credit_payment['payment_date_credit_payment'] = pd.NaT
    #         df_sales_credit_payment_complete = pd.merge(df_complete_3, df_sales_credit_payment, on='payment_date_credit_payment', how='left')
    #         df_sales_credit_payment_complete['revenue_credit_payment'] = df_sales_credit_payment_complete['revenue_credit_payment'].fillna(0)
    #         df_sales_credit_payment_complete = df_sales_credit_payment_complete[(df_sales_credit_payment_complete["payment_date_credit_payment"] >= start_date_training) & (df_sales_credit_payment_complete["payment_date_credit_payment"] <= end_date_training)]
    #         df_sales_credit_payment_complete = df_sales_credit_payment_complete.rename(columns = {
    #             'payment_date_credit_payment': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_credit_payment_complete, on = 'date_trx_days')

    #     df_sales_return = df_sales_return[df_sales_return['sales_return_status'] != 'X']
    #     df_sales_return['return'] = df_sales_return['total_amount_return']/df_sales_return['exchange_rate']
    #     df_sales_return['return_cost'] = df_sales_return['total_cost_amount_return']/df_sales_return['exchange_rate']
    #     df_sales_return = df_sales_return.groupby(["return_date"], as_index=False).agg({
    #         "return": "sum",
    #         "return_cost": "sum"
    #     })

    #     # Check if the result of the groupby is not empty
    #     if not df_sales_return.empty:
    #         df_sales_return['return_date'] = pd.to_datetime(df_sales_return['return_date'])
    #         df_sales_return[['return', 'return_cost']] = df_sales_return[['return', 'return_cost']].astype(float)
    #         df_complete_4 = pd.DataFrame(training_date_range, columns=['return_date'])
    #         df_sales_return_complete = pd.merge(df_complete_4, df_sales_return, on='return_date', how='left')
    #         df_sales_return_complete[['return', 'return_cost']] = df_sales_return_complete[['return', 'return_cost']].fillna(0)
    #         df_sales_return_complete = df_sales_return_complete[(df_sales_return_complete["return_date"] >= start_date_training) & (df_sales_return_complete["return_date"] <= end_date_training)]
    #         df_sales_return_complete = df_sales_return_complete.rename(columns = {
    #             'return_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_return_complete, on = 'date_trx_days')
    #     else:
    #         df_sales_return = pd.DataFrame(columns=['return_date', 'return', 'return_cost'])
    #         df_complete_4 = pd.DataFrame(training_date_range, columns=['return_date'])
    #         if 'return_date' not in df_sales_return.columns:
    #             df_sales_return['return_date'] = pd.NaT
    #         df_sales_return_complete = pd.merge(df_complete_4, df_sales_return, on='return_date', how='left')
    #         df_sales_return_complete[['return', 'return_cost']] = df_sales_return_complete[['return', 'return_cost']].fillna(0)
    #         df_sales_return_complete = df_sales_return_complete[(df_sales_return_complete["return_date"] >= start_date_training) & (df_sales_return_complete["return_date"] <= end_date_training)]
    #         df_sales_return_complete = df_sales_return_complete.rename(columns = {
    #             'return_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_return_complete, on = 'date_trx_days')
        
    #     dfs_3 = [] # ambil dari s3 ci_store_inex_trans
    #     path_inex_trans = f"{SalesRecapService.directory}/inex-trans/{store_id}_inex_trans_6_months_ago.feather"
    #     df_inex_trans = download_file_from_s3(path_inex_trans)
    #     dfs_3.append(df_inex_trans)

    #     # Check if the list dfs_3 is not empty and contains DataFrames
    #     if dfs_3 and not dfs_3[0].empty:
    #         df_inex_trans = pd.concat(dfs_3)
    #         df_inex_trans = df_inex_trans[df_inex_trans['status'] != 'X']
    #         df_inex_trans = df_inex_trans[df_inex_trans['status_trans_type'] == 'E']
    #         df_inex_trans['expense_amount'] = df_inex_trans['amount']
    #         df_inex_trans = df_inex_trans.groupby(["trans_date"], as_index=False)["expense_amount"].sum()
    #         df_inex_trans['trans_date'] = pd.to_datetime(df_inex_trans['trans_date'])
    #         df_inex_trans['expense_amount'] = df_inex_trans['expense_amount'].astype(float)
    #         df_complete_5 = pd.DataFrame(training_date_range, columns=['trans_date'])
    #         df_inex_trans_complete = pd.merge(df_complete_5, df_inex_trans, on='trans_date', how='left')
    #         df_inex_trans_complete['expense_amount'] = df_inex_trans_complete['expense_amount'].fillna(0)
    #         df_inex_trans_complete = df_inex_trans_complete[(df_inex_trans_complete["trans_date"] >= start_date_training) & (df_inex_trans_complete["trans_date"] <= end_date_training)]
    #         df_inex_trans_complete = df_inex_trans_complete.rename(columns = {
    #             'trans_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_inex_trans_complete, on = 'date_trx_days')
    #     else:
    #         df_inex_trans = pd.DataFrame(columns=['trans_date', 'expense_amount'])
    #         df_complete_5 = pd.DataFrame(training_date_range, columns=['trans_date'])
    #         if 'trans_date' not in df_inex_trans.columns:
    #             df_inex_trans['trans_date'] = pd.NaT
    #         df_inex_trans_complete = pd.merge(df_complete_5, df_inex_trans, on='trans_date', how='left')
    #         df_inex_trans_complete['expense_amount'] = df_inex_trans_complete['expense_amount'].fillna(0)
    #         df_inex_trans_complete = df_inex_trans_complete[(df_inex_trans_complete["trans_date"] >= start_date_training) & (df_inex_trans_complete["trans_date"] <= end_date_training)]
    #         df_inex_trans_complete = df_inex_trans_complete.rename(columns = {
    #             'trans_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_inex_trans_complete, on = 'date_trx_days')
        
    #     df_complete['nett_profit_total_1'] = df_complete['revenue_non_shipping'] + df_complete['revenue_service_charge'] + df_complete['revenue_credit_payment'] + df_complete['return_cost'] + df_complete['purchases_discount'] + df_complete['purchases_shipping'] + df_complete['purchases_tax'] - df_complete['return'] - df_complete['goods_cost'] - df_complete['expense_amount']
            
    #     # Lakukan perhitungan variable food delivery dan stock out/opname record
    #     dfs_4 = []
    #     path_sales_for_food_delivery = f"{SalesRecapService.directory}/sales-order/{store_id}_sales_order_6_months_ago.feather"
    #     path_sales_order_item_for_food_delivery = f"{SalesRecapService.directory}/sales-order-item/{store_id}_sales_order_item_6_months_ago.feather"
    #     df_sales_for_food_delivery = download_file_from_s3(path_sales_for_food_delivery)
    #     df_sales_for_food_delivery = df_sales_for_food_delivery.rename(columns = {
    #         'id': 'sales_order_id'
    #     })
    #     df_sales_order_item_for_food_delivery = download_file_from_s3(path_sales_order_item_for_food_delivery)
    #     df_sales_order_item_for_food_delivery['product_variant_id'] = df_sales_order_item_for_food_delivery['product_variant_id'].fillna(0)
    #     df_sales_order_item_for_food_delivery['product_variant_id'] = df_sales_order_item_for_food_delivery['product_variant_id'].astype(int)
    #     df_sales_for_food_delivery = pd.merge(df_sales_for_food_delivery, df_sales_order_item_for_food_delivery, on = 'sales_order_id', how = 'left')
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['order_source'].isin(['X', 'Y', 'Z', 'V'])]
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[~df_sales_for_food_delivery['order_status'].isin(['X', 'D'])]
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['status'] == 'A']
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[
    #         np.where(df_sales_for_food_delivery['product_combo_id'] == 0, 
    #                 df_sales_for_food_delivery['product_id'], 
    #                 999) > 1
    #     ]
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['is_paid'] == 1]
    #     df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['payment_type_id'] != 'CT']
    #     dfs_4.append(df_sales_for_food_delivery)

    #     if dfs_4 and not dfs_4[0].empty:
    #         df_sales_for_food_delivery = pd.concat(dfs_4)
    #         df_sales_for_food_delivery['food_delivery_amount'] = df_sales_for_food_delivery['total_amount']
    #         df_sales_for_food_delivery = df_sales_for_food_delivery.groupby(["order_date"], as_index = False)["food_delivery_amount"].sum()
    #         df_sales_for_food_delivery['order_date'] = pd.to_datetime(df_sales_for_food_delivery['order_date'])
    #         df_sales_for_food_delivery['food_delivery_amount'] = df_sales_for_food_delivery['food_delivery_amount'].astype(float)
    #         df_complete_6 = pd.DataFrame(training_date_range, columns=['order_date'])
    #         df_sales_for_food_delivery_complete = pd.merge(df_complete_6, df_sales_for_food_delivery, on = 'order_date', how = 'left')
    #         df_sales_for_food_delivery_complete['food_delivery_amount'] = df_sales_for_food_delivery_complete['food_delivery_amount'].fillna(0)
    #         df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete[(df_sales_for_food_delivery_complete["order_date"] >= start_date_training) & (df_sales_for_food_delivery_complete["order_date"] <= end_date_training)]
    #         df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete.rename(columns = {
    #             'order_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_for_food_delivery_complete, on = 'date_trx_days')
    #     else:
    #         df_sales_for_food_delivery = pd.DataFrame(columns = ['order_date', 'food_delivery_amount'])
    #         df_complete_6 = pd.DataFrame(training_date_range, columns=['order_date'])
    #         if 'order_date' not in df_sales_for_food_delivery.columns:
    #             df_sales_for_food_delivery['order_date'] = pd.NaT
    #         df_sales_for_food_delivery_complete = pd.merge(df_complete_6, df_sales_for_food_delivery, on = 'order_date', how = 'left')
    #         df_sales_for_food_delivery_complete['food_delivery_amount'] = df_sales_for_food_delivery_complete['food_delivery_amount'].fillna(0)
    #         df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete[(df_sales_for_food_delivery_complete["order_date"] >= start_date_training) & (df_sales_for_food_delivery_complete["order_date"] <= end_date_training)]
    #         df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete.rename(columns = {
    #             'order_date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_sales_for_food_delivery_complete, on = 'date_trx_days')
        
    #     df_complete['food_delivery_amount'] = abs(df_complete['food_delivery_amount'])
    #     df_complete['nett_profit_total_2'] = df_complete['nett_profit_total_1'] - df_complete['food_delivery_amount']

    #     dfs_5 = []
    #     path_stock_out_record = f"{SalesRecapService.directory}/stock-in-out-items/{store_id}_stock_in_out_items_6_months_ago.feather"
    #     df_stock_out_record = download_file_from_s3(path_stock_out_record)
    #     dfs_5.append(df_stock_out_record)

    #     if dfs_5 and not dfs_5[0].empty:
    #         df_stock_out_record = pd.concat(dfs_5)
    #         df_stock_out_record['stock_out_record_amount'] = df_stock_out_record['new_buy_price'] * df_stock_out_record['qty']
    #         df_stock_out_record = df_stock_out_record.groupby(["date"], as_index = False)["stock_out_record_amount"].sum()
    #         df_stock_out_record['date'] = pd.to_datetime(df_stock_out_record['date'])
    #         df_stock_out_record['stock_out_record_amount'] = df_stock_out_record['stock_out_record_amount'].astype(float)
    #         df_complete_7 = pd.DataFrame(training_date_range, columns=['date'])
    #         df_stock_out_record_complete = pd.merge(df_complete_7, df_stock_out_record, on = 'date', how = 'left')
    #         df_stock_out_record_complete['stock_out_record_amount'] = df_stock_out_record_complete['stock_out_record_amount'].fillna(0)
    #         df_stock_out_record_complete = df_stock_out_record_complete[(df_stock_out_record_complete["date"] >= start_date_training) & (df_stock_out_record_complete["date"] <= end_date_training)]
    #         df_stock_out_record_complete = df_stock_out_record_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_stock_out_record_complete, on = 'date_trx_days')
    #     else:
    #         df_stock_out_record = pd.DataFrame(columns = ['date', 'stock_out_record_amount'])
    #         df_complete_7 = pd.DataFrame(training_date_range, columns=['date'])
    #         if 'date' not in df_stock_out_record.columns:
    #             df_stock_out_record['date'] = pd.NaT
    #         df_stock_out_record_complete = pd.merge(df_complete_7, df_stock_out_record, on = 'date', how = 'left')
    #         df_stock_out_record_complete['stock_out_record_amount'] = df_stock_out_record_complete['stock_out_record_amount'].fillna(0)
    #         df_stock_out_record_complete = df_stock_out_record_complete[(df_stock_out_record_complete["date"] >= start_date_training) & (df_stock_out_record_complete["date"] <= end_date_training)]
    #         df_stock_out_record_complete = df_stock_out_record_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_stock_out_record_complete, on = 'date_trx_days')
        
    #     df_complete['nett_profit_total_3'] = df_complete['nett_profit_total_2'] + df_complete['stock_out_record_amount']

    #     dfs_6 = []
    #     path_stock_opname = f"{SalesRecapService.directory}/stock-opname-items/{store_id}_stock_opname_items_6_months_ago.feather"
    #     df_stock_opname = download_file_from_s3(path_stock_opname)
        
    #     dfs_6.append(df_stock_opname)

    #     if dfs_6 and not dfs_6[0].empty:
    #         df_stock_opname = pd.concat(dfs_6)
    #     else:
    #         df_stock_opname = pd.DataFrame(columns = ['opname_id', 'product_id', 'product_variant_id', 'qty', 'qty_sys', 'date'])

    #     dfs_7 = []
    #     path_product_materials = f"{SalesRecapService.directory}/product-materials/{store_id}_product_materials.feather"
    #     df_product_materials = download_file_from_s3(path_product_materials)

    #     dfs_7.append(df_product_materials)

    #     if dfs_7 and not dfs_7[0].empty:
    #         df_product_materials = pd.concat(dfs_7)
    #         df_product_materials = df_product_materials.rename(columns = {
    #             'qty': 'qty_product_materials'
    #         })
    #     else:
    #         df_product_materials = pd.DataFrame(columns = ['id', 'product_id', 'product_variant_id', 'material_product_id', 'material_product_variant_id', 'qty_product_materials', 'uom', 'uom_conversion'])

    #     dfs_8 = []
    #     path_variant_materials = f"{SalesRecapService.directory}/product-variant-materials/{store_id}_product_variant_materials.feather"
    #     df_product_variant_materials = download_file_from_s3(path_variant_materials)

    #     dfs_8.append(df_product_variant_materials)

    #     if dfs_8 and not dfs_8[0].empty:
    #         df_product_variant_materials = pd.concat(dfs_8)
    #         df_product_variant_materials = df_product_variant_materials.rename(columns = {
    #             'qty': 'qty_product_variant_materials'
    #         })
    #     else:
    #         df_product_variant_materials = pd.DataFrame(columns = ['id', 'product_id', 'product_variant_id', 'material_product_id', 'material_product_variant_id', 'qty_product_variant_materials', 'uom', 'uom_conversion'])

    #     dfs_9 = []
    #     path_product_master = f"{SalesRecapService.directory}/product-master/{store_id}_product_master.feather"
    #     df_product_master = download_file_from_s3(path_product_master)

    #     dfs_9.append(df_product_master)

    #     if dfs_9 and not dfs_9[0].empty:
    #         df_product_master = pd.concat(dfs_9)
    #         df_product_master = df_product_master.rename(columns = {
    #             'id': 'product_id'
    #         })
    #     else:
    #         df_product_master = pd.DataFrame(columns = ['product_id', 'store_id', 'name', 'category_id', 'status', 'buy_price'])

    #     dfs_10 = []
    #     path_variant_master = f"{SalesRecapService.directory}/product-variant/{store_id}_product_variant.feather"
    #     df_product_variant_master = download_file_from_s3(path_variant_master)

    #     dfs_10.append(df_product_variant_master)

    #     if dfs_10 and not dfs_10[0].empty:
    #         df_product_variant_master = pd.concat(dfs_10)
    #         df_product_variant_master = df_product_variant_master.rename(columns = {
    #             'id': 'product_variant_id'
    #         })
    #     else:
    #         df_product_variant_master = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'name', 'category_id', 'status', 'buy_price'])

    #     # continue proses join disini
    #     df_to_join_product_variant_master = pd.merge(df_product_variant_materials, df_product_variant_master, how = 'inner', on = 'product_variant_id')
    #     df_to_join_product_master = pd.merge(df_product_materials, df_product_master, how = 'inner', on = 'product_id')
    #     df_opname_product_variant = pd.merge(df_to_join_product_variant_master, df_stock_opname, how = 'inner', on = 'product_variant_id')
    #     df_opname_product_only = pd.merge(df_to_join_product_master, df_stock_opname, how = 'inner', on = 'product_id')

    #     dfs_11 = []
    #     dfs_11.append(df_opname_product_variant)

    #     if dfs_11 and not dfs_11[0].empty:
    #         df_opname_product_variant = pd.concat(dfs_11)
    #         df_opname_product_variant['pv_materials_buy_price'] = df_opname_product_variant.apply(
    #             lambda row: row['buy_price'] * row['qty_product_variant_materials'] * row['uom_conversion'] if row['uom_conversion'] > 0 else row['buy_price'] * row['qty_product_variant_materials']
    #         )
    #         df_opname_product_variant = df_opname_product_variant.groupby(["date"], as_index = False)["pv_materials_buy_price"].sum() # jika ada filter, column filternya akan masuk ke groupby
    #         df_opname_product_variant['date'] = pd.to_datetime(df_opname_product_variant['date'])
    #         df_opname_product_variant['pv_materials_buy_price'] = df_opname_product_variant['pv_materials_buy_price'].astype(float)
    #         df_complete_8 = pd.DataFrame(training_date_range, columns = ['date'])
    #         df_opname_product_variant_complete = pd.merge(df_complete_8, df_opname_product_variant, on = 'date', how = 'left')
    #         df_opname_product_variant_complete['pv_materials_buy_price'] = df_opname_product_variant_complete['pv_materials_buy_price'].fillna(0)
    #         df_opname_product_variant_complete = df_opname_product_variant_complete[(df_opname_product_variant_complete["date"] >= start_date_training) & (df_opname_product_variant_complete["date"] <= end_date_training)]
    #         df_opname_product_variant_complete = df_opname_product_variant_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_opname_product_variant_complete, on = 'date_trx_days')
    #     else:
    #         df_opname_product_variant = pd.DataFrame(columns = ['date', 'pv_materials_buy_price'])
    #         df_complete_8 = pd.DataFrame(training_date_range, columns = ['date'])
    #         if 'date' not in df_opname_product_variant.columns:
    #             df_opname_product_variant['date'] = pd.NaT
    #         df_opname_product_variant_complete = pd.merge(df_complete_8, df_opname_product_variant, on = 'date', how = 'left')
    #         df_opname_product_variant_complete['pv_materials_buy_price'] = df_opname_product_variant_complete['pv_materials_buy_price'].fillna(0)
    #         df_opname_product_variant_complete = df_opname_product_variant_complete[(df_opname_product_variant_complete["date"] >= start_date_training) & (df_opname_product_variant_complete["date"] <= end_date_training)]
    #         df_opname_product_variant_complete = df_opname_product_variant_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_opname_product_variant_complete, on = 'date_trx_days')

    #     dfs_12 = []
    #     dfs_12.append(df_opname_product_only)

    #     if dfs_12 and not dfs_12[0].empty:
    #         df_opname_product_only = pd.concat(dfs_12)
    #         df_opname_product_only['p_materials_buy_price'] = df_opname_product_only.apply(
    #             lambda row: row['buy_price'] * row['qty_product_materials'] * row['uom_conversion'] if row['uom_conversion'] > 0 else row['buy_price'] * row['qty_product_materials']
    #         )
    #         df_opname_product_only = df_opname_product_only.groupby(["date"], as_index = False)["p_materials_buy_price"].sum() # jika ada filter, column filternya akan masuk ke groupby
    #         df_opname_product_only['p_materials_buy_price'] = df_opname_product_only['p_materials_buy_price'].astype(float)
    #         df_complete_9 = pd.DataFrame(training_date_range, columns = ['date'])
    #         df_opname_product_only_complete = pd.merge(df_complete_9, df_opname_product_only, on ='date', how = 'left')
    #         df_opname_product_only_complete['p_materials_buy_price'] = df_opname_product_only_complete['p_materials_buy_price'].fillna(0)
    #         df_opname_product_only_complete = df_opname_product_only_complete[(df_opname_product_only_complete["date"] >= start_date_training) & (df_opname_product_only_complete["date"] <= end_date_training)]
    #         df_opname_product_only_complete = df_opname_product_only_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_opname_product_only_complete, on = 'date_trx_days')
    #     else:
    #         df_opname_product_only = pd.DataFrame(columns = ['date', 'p_materials_buy_price'])
    #         df_complete_9 = pd.DataFrame(training_date_range, columns = ['date'])
    #         if 'date' not in df_opname_product_only.columns:
    #             df_opname_product_only['date'] = pd.NaT
    #         df_opname_product_only_complete = pd.merge(df_complete_9, df_opname_product_only, on ='date', how = 'left')
    #         df_opname_product_only_complete['p_materials_buy_price'] = df_opname_product_only_complete['p_materials_buy_price'].fillna(0)
    #         df_opname_product_only_complete = df_opname_product_only_complete[(df_opname_product_only_complete["date"] >= start_date_training) & (df_opname_product_only_complete["date"] <= end_date_training)]
    #         df_opname_product_only_complete = df_opname_product_only_complete.rename(columns = {
    #             'date': 'date_trx_days'
    #         })
    #         df_complete = pd.merge(df_complete, df_opname_product_only_complete, on = 'date_trx_days')

    #     df_complete['pv_materials_buy_price'] = df_complete['pv_materials_buy_price'].astype(float)
    #     df_complete['p_materials_buy_price'] = df_complete['p_materials_buy_price'].astype(float)

    #     def calculate_nett_profit(row):
    #         if row['pv_materials_buy_price'] != 0:
    #             return row['nett_profit_total_3'] + row['pv_materials_buy_price']
    #         else:
    #             return row['nett_profit_total_3'] + row['p_materials_buy_price']
        
    #     df_complete['nett_profit_total'] = df_complete.apply(calculate_nett_profit, axis=1)
        
    #     train = df_complete[['date_trx_days', 'nett_profit_total']].reset_index()
    #     train.set_index("date_trx_days", inplace = True)
    #     train.index.name = "date_trx_days"

    #     # # Calculate mean and standard deviation
    #     # mean = train["total_amount"].mean()
    #     # std = train["total_amount"].std()

    #     # # Identify outliers using Z-scores threshold
    #     # outliers_mask = ((train["total_amount"] - mean).abs() / std) <= 3

    #     # # Filter out outliers
    #     # train_filtered = train[outliers_mask]
    #     train_filtered = train # comment this line jika jadi menggunakan outliers dan uncomment line code di atas

    #     # Create lag features
    #     train_filtered['lag_30'] = train_filtered["nett_profit_total"].shift(30)
    #     train_filtered.dropna(inplace=True)

    #     # Features and target
    #     X_train = train_filtered[['lag_30']]
    #     y_train = train_filtered["nett_profit_total"]

    #     # Initialize Ridge with a pipeline
    #     pipeline = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    #     pipeline.fit(X_train, y_train)

    #     future_df = pd.DataFrame({'date_trx_days': future_dates})
    #     future_df['lag_30'] = 0

    #     # Iteratively update the lag feature
    #     last_total_amount = train_filtered["nett_profit_total"].iloc[-30:]
    #     for i in range(len(future_df)):
    #         if i < 30:
    #             future_df.at[i, 'lag_30'] = last_total_amount.values[i]
    #         else:
    #             future_df.at[i, 'lag_30'] = y_pred_projection[i - 30]

    #         X_future = future_df[['lag_30']].iloc[:i+1]
    #         y_pred_projection = pipeline.predict(X_future)
    #     y_pred_1 = pd.DataFrame({
    #         'date_trx_days': future_dates,
    #         'nett_profit_projection': y_pred_projection
    #     })

    #     # Create future dataframe to predict until two month ahead
    #     p = 5
    #     d = 1
    #     q = 0

    #     model_2 = ARIMA(train_filtered["nett_profit_total"], order = (p, d, q))

    #     model_2_fit = model_2.fit()

    #     # Forecast on the future set
    #     forecast_output = model_2_fit.forecast(steps = 93)
    #     y_pred_2 = pd.DataFrame({
    #         'date_trx_days': future_dates,
    #         'nett_profit_forecasting': forecast_output
    #     })
        
    #     train_filtered = train_filtered.reset_index()
    #     train_filtered['date_trx_days'] = pd.to_datetime(train_filtered['date_trx_days'])
    #     y_pred_1['date_trx_days'] = pd.to_datetime(y_pred_1['date_trx_days'])
    #     y_pred_2['date_trx_days'] = pd.to_datetime(y_pred_2['date_trx_days'])
    #     # Concatenate the training and prediction data
    #     combined_df_1 = pd.merge(train_filtered, y_pred_1, how = "outer", on = "date_trx_days")
    #     combined_df = pd.merge(combined_df_1, y_pred_2, how = "outer", on = "date_trx_days")

    #     combined_df = combined_df.rename(columns = {
    #         'date_trx_days': 'transaction_date',
    #         'nett_profit_total': 'laba',
    #         'nett_profit_projection': 'laba_projection',
    #         'nett_profit_forecasting': 'laba_forecasting'
    #     })

    #     df_final = combined_df[['laba', 'laba_projection', 'laba_forecasting', 'transaction_date']]

    #     df_final_to_list = list(df_final.itertuples(index = False, name = None)) # ubah DataFrame menjadi tuple di dalam list

    #     return df_final_to_list
        
    #     # except Exception as e:
    #     #     raise e
    
    # # Bikin function baru panggil GetSalesProjectionForecasting
    # def CreateFileFeatherProfitProjectionForecasting():
    #     # try:
    #     store = StoreModel.Store
    #     dataStore = store.GetIndexData()
    #     classSalesRecapService = SalesRecapService
    #     bucket_name= os.getenv('AWS_BUCKET')

    #     for row in dataStore:
    #         store_id = row[0]
    #         # profit projection forecasting
    #         profitProjectionForecasting = classSalesRecapService.GetProfitProjectionForecasting(store_id)
    #         file_name = f"{store_id}_profit_projection_and_forecasting.feather"
    #         file_path = f"{SalesRecapService.directory}/ml/laba"
    #         WrapperCreateFile(sourceData = profitProjectionForecasting,
    #                         fileName = file_name,
    #                         columns = SetColumnForFeatherFile(10),
    #                         aws_bucket = bucket_name,
    #                         path = file_path)

    #     # except Exception as error:
    #     #     raise error

    @staticmethod
    def GetSalesS3Data(store_id):
        try:
            current_date = datetime.now()
            now_wib = current_date + timedelta(hours=7)
            start_date_training = (now_wib - timedelta(days=181)).date() # karena ada lag 1 hari data rekap
            end_date_training = (now_wib - timedelta(days=1)).date() # karena ada lag 1 hari data rekap
            start_year_penarikan = start_date_training.year
            end_year_penarikan = end_date_training.year
            years_raw = [start_year_penarikan, end_year_penarikan]

            seen = set()
            years = []

            for item in years_raw:
                if item not in seen:
                    seen.add(item)
                    years.append(item)
            
            # Download Data Sales
            dfs = []
            for year in years:
                try:
                    path_sales = f"{SalesRecapService.directory}/sales-tracking-vision/{store_id}_{year}_sales_tracking_vision.feather"
                    df_sales = download_file_from_s3(path_sales)
                    dfs.append(df_sales)
                    # print(dfs.columns.tolist())
                    # print(path_sales)
                except:
                    continue
            
            if not dfs:
                df_sales_complete = pd.DataFrame(columns=['date_trx_days', 'total_amount'])
                return df_sales_complete

            df_sales = pd.concat(dfs)
            sales = df_sales.loc[df_sales['flag'] == 0, :].rename(columns={'total_amount': 'total_sales'})
            refunds = df_sales.loc[df_sales['flag'] == 1, ["total_amount", "date_trx_days"]].rename(columns={'total_amount': 'total_refund'})
            if refunds.empty:
                merged = sales.copy()
                merged['total'] = merged['total_sales']
            else:
                merged = pd.merge(sales, refunds, on=['date_trx_days'], how='outer')
                merged['total_sales'].fillna(0, inplace=True)
                merged['total_refund'].fillna(0, inplace=True)
                merged["total"] = merged["total_sales"] - merged["total_refund"]
            df_sales = merged.groupby(["date_trx_days"], as_index=False)["total"].sum()
            df_sales['date_trx_days'] = pd.to_datetime(df_sales['date_trx_days'])
            df_sales['total'] = df_sales['total'].astype(float)
            start_date_training = pd.to_datetime(start_date_training)
            end_date_training = pd.to_datetime(end_date_training)
            training_date_range = pd.date_range(start=start_date_training, end=end_date_training)
            df_complete = pd.DataFrame(training_date_range, columns=['date_trx_days'])
            df_sales_complete = pd.merge(df_complete, df_sales, on='date_trx_days', how='left')
            df_sales_complete['total'] = df_sales_complete['total'].fillna(0)
            df_sales_complete = df_sales_complete[(df_sales_complete["date_trx_days"] >= start_date_training) & (df_sales_complete["date_trx_days"] <= end_date_training)]
            df_sales_complete = df_sales_complete.rename(columns={
                'total': 'total_amount'
            })

            return df_sales_complete
        
        except Exception as e:
            raise e
    
    @staticmethod
    def TrainMachineLearningSales(df_sales_complete):
        try:
            current_date = datetime.now()
            now_wib = current_date + timedelta(hours=7)
            this_month_beginning = now_wib.replace(day = 1)
            future_dates = pd.date_range(start=this_month_beginning, periods=93, freq='D').date

            if df_sales_complete.empty:
                start_date = pd.to_datetime(future_dates[0]) - pd.Timedelta(days=181)
                end_date = pd.to_datetime(future_dates[-1])
                all_dates = pd.date_range(start=start_date, end=end_date)
                df_sales = pd.DataFrame({
                    'transaction_date': all_dates,
                    'total_sales': 0,
                    'sales_projection': 0,
                    'sales_forecasting': 0
                })
                return df_sales
            
            train = df_sales_complete.reset_index()
            train.set_index("date_trx_days", inplace = True)
            train.index.name = "date_trx_days"

            # # Calculate mean and standard deviation
            # mean = train["total_amount"].mean()
            # std = train["total_amount"].std()

            # # Identify outliers using Z-scores threshold
            # outliers_mask = ((train["total_amount"] - mean).abs() / std) <= 3

            # # Filter out outliers
            # train_filtered = train[outliers_mask]
            train_filtered = train # comment this line jika jadi menggunakan outliers dan uncomment line code di atas

            # Create lag features
            train_filtered['lag_30'] = train_filtered["total_amount"].shift(30)
            train_filtered.dropna(inplace=True)

            # Features and target
            X_train = train_filtered[['lag_30']]
            y_train = train_filtered["total_amount"]

            # Initialize Ridge with a pipeline
            pipeline = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
            pipeline.fit(X_train, y_train)

            future_df = pd.DataFrame({'date_trx_days': future_dates})
            future_df['lag_30'] = 0

            # Iteratively update the lag feature
            last_total_amount = train_filtered["total_amount"].iloc[-30:]
            for i in range(len(future_df)):
                if i < 30:
                    future_df.at[i, 'lag_30'] = last_total_amount.values[i]
                else:
                    future_df.at[i, 'lag_30'] = y_pred_projection[i - 30]

                X_future = future_df[['lag_30']].iloc[:i+1]
                y_pred_projection = pipeline.predict(X_future)
            y_pred_1 = pd.DataFrame({
                'date_trx_days': future_dates,
                'sales_projection': y_pred_projection
            })

            # Create future dataframe to predict until two month ahead
            p = 5
            d = 1
            q = 0

            model_2 = ARIMA(train_filtered["total_amount"], order = (p, d, q))

            model_2_fit = model_2.fit()

            # Forecast on the future set
            forecast_output = model_2_fit.forecast(steps = 93)
            y_pred_2 = pd.DataFrame({
                'date_trx_days': future_dates,
                'sales_forecasting': forecast_output
            })
            train_filtered = train_filtered.reset_index()
            train_filtered['date_trx_days'] = pd.to_datetime(train_filtered['date_trx_days'])
            y_pred_1['date_trx_days'] = pd.to_datetime(y_pred_1['date_trx_days'])
            y_pred_2['date_trx_days'] = pd.to_datetime(y_pred_2['date_trx_days'])
            # Concatenate the training and prediction data
            combined_df_1 = pd.merge(train_filtered, y_pred_1, how = "outer", on = "date_trx_days")
            combined_df = pd.merge(combined_df_1, y_pred_2, how = "outer", on = "date_trx_days")

            combined_df = combined_df.rename(columns = {
                'total_amount': 'total_sales',
                'date_trx_days': 'transaction_date'
            })

            df_sales = combined_df[['total_sales', 'sales_projection', 'sales_forecasting', 'transaction_date']]

            return df_sales
        
        except Exception as e:
            raise e
    
    @staticmethod
    def InsertSalesProjection(df_final_to_list):
        current_date = datetime.now()
        now_wib_h_min_2 = current_date + timedelta(hours = 7) - timedelta(days = 2)
        fnow_wib_h_min_2 = now_wib_h_min_2.strftime('%Y-%m-%d')
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_projection_forecasting" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_projection_forecasting"
        delete_query = f"DELETE FROM {table_name} WHERE store_id = %s"
        insert_query = f"""
        INSERT INTO {table_name} (
            store_id,
            total_sales,
            sales_projection,
            sales_forecasting,
            transaction_date
        ) VALUES (
        %s, %s, %s, %s, %s
        )
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                # Get the single store_id
                store_id = df_final_to_list[0][0]
                
                # Delete existing records for the store_id
                cursor.execute(delete_query, (store_id,))
                
                # Convert Timestamps to string format
                for order in df_final_to_list:
                    cursor.execute(insert_query, (
                        order[0], # store_id
                        order[1], # total_sales
                        order[2], # sales_projection
                        order[3], # sales_forecasting
                        order[4].strftime('%Y-%m-%d')  # transaction_date
                    ))
                
                # Commit perubahan ke database
                conn.commit()
                
                # Update last sync for the store
                classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                classProjectionForecastingCheckService.UpdateLastSyncForSalesProjectionForecasting(fnow_wib_h_min_2, store_id)

        except Exception as e:
            conn.rollback()
            print(f"Error inserting profit projection forecasting: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()

    @staticmethod
    def UpdateSalesDaily(df_final_to_list, store_id, fstart_date_update):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_projection_forecasting" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_projection_forecasting"
        
        # insert_query = f"""
        # INSERT INTO {table_name} (store_id, laba, transaction_date)
        # VALUES (%s, %s, %s)
        # """
        
        update_query = f"""
        UPDATE {table_name}
        SET total_sales = %s
        WHERE store_id = %s AND transaction_date = %s
        """

        check_existing_query = f"""
        SELECT id
        FROM {table_name}
        WHERE store_id = %s AND transaction_date = %s
        """

        flast_non_zero_sales_date = None

        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                for row in df_final_to_list:
                    store_id_value = row[0]  # store_id from the current row
                    total_sales_value = row[1]  # total_sales from the current row
                    transaction_date = row[2]  # transaction_date from the current row
                    ftransaction_date = transaction_date.strftime('%Y-%m-%d')

                    # Update last_non_zero_sales_date if sales is non-zero
                    if total_sales_value != 0:
                        flast_non_zero_sales_date = ftransaction_date
                    
                    # Check if the row exists
                    cursor.execute(check_existing_query, (store_id_value, ftransaction_date))
                    result = cursor.fetchone()

                    if result:
                    # If the row exists, perform an update
                        cursor.execute(update_query, (total_sales_value, store_id_value, ftransaction_date))
                    # else:
                    #     # If the row doesn't exist, perform an insert
                    #     cursor.execute(insert_query, (store_id_value, total_sales_value, ftransaction_date))
                
                # Commit changes to the database
                conn.commit()

                # Call the function to update last sync after processing all rows
                classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                if flast_non_zero_sales_date and flast_non_zero_sales_date != fstart_date_update:
                    classProjectionForecastingCheckService.UpdateLastSyncForSalesProjectionForecasting(
                        flast_non_zero_sales_date, store_id
                    )
                else:
                    print(f"No non-zero sales found for store_id {store_id}")
                    return True

        except Exception as e:
            conn.rollback()
            print(f"Error updating sales projection forecasting: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def StoringSalesToDBAnalytic(payload = None):
        try:
            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()
            
            classSalesRecapService = SalesRecapService
            for row in dataStore:
                try:
                    store_id = row[0]
                    current_date = datetime.now()
                    now_wib = current_date + timedelta(hours=7)
                    start_date_training = (now_wib - timedelta(days=181)).date() # karena ada lag 1 hari data rekap
                    end_date_training = (now_wib - timedelta(days=1)).date() # karena ada lag 1 hari data rekap
                    
                    this_month_beginning = now_wib.replace(day = 1)
                    future_dates = pd.date_range(start=this_month_beginning, periods=93, freq='D').date

                    start_date_training = pd.to_datetime(start_date_training)
                    end_date_training = pd.to_datetime(end_date_training)
                    training_date_range = pd.date_range(start = start_date_training, end = end_date_training)

                    date_sql = start_date_training

                    data_for_check_activation = StoreExtSettingsModel.StoreExtSettings.checkActivation(store_id)
                    check_activation_date = data_for_check_activation[2] # activation_date ada di kolom ketiga hasil fetch SQL
                    test_1_boolean = (check_activation_date != now_wib.date())
                    if test_1_boolean:
                    # data_for_flag_sync = SalesRecapService.ActivationSyncChecker(store_id)
                    # flag_sync = data_for_flag_sync[0][0]
                    # flag_sync = 1 # comment line ini dan uncomment line diatas jika sudah ada data flag nya
                    # if flag_sync == 1:
                        today_date = now_wib.day
                        df_sales_complete = classSalesRecapService.GetSalesS3Data(store_id)
                        test_boolean = (today_date == 17)
                        # if today_date == 29: # customize disini untuk staging dan prod sesuai tanggal hari ini, di cron default today_date == 3
                        if test_boolean:
                            df_final = classSalesRecapService.TrainMachineLearningSales(df_sales_complete)
                            df_final['store_id'] = store_id
                            df_final = df_final[['store_id', 'total_sales', 'sales_projection', 'sales_forecasting', 'transaction_date']]
                            df_final = df_final.fillna(0)
                            df_final_to_list = list(df_final.itertuples(index = False, name = None))
                            classSalesRecapService.InsertSalesProjection(df_final_to_list)
                        else:
                            df_final = df_sales_complete
                            df_final = df_final.rename(columns = {
                                'date_trx_days': 'transaction_date',
                                'total_amount': 'total_sales'
                            })
                            classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                            fetch_start_date_update = classProjectionForecastingCheckService.GetSyncedStoreSalesProjectionForecasting(store_id)
                            start_date_update = fetch_start_date_update[1]
                            fstart_date_update = start_date_update.strftime("%Y-%m-%d")
                            start_date_update = pd.to_datetime(start_date_update)
                            df_final = df_final.loc[df_final['transaction_date'] >= start_date_update, :]
                            if not df_final.empty:
                                df_final['store_id'] = store_id
                                df_final = df_final[['store_id', 'total_sales', 'transaction_date']]
                                df_final_to_list = list(df_final.itertuples(index = False, name = None))
                                classSalesRecapService.UpdateSalesDaily(df_final_to_list, store_id, fstart_date_update)
                    else:
                        df_sales_complete = classSalesRecapService.GetSalesS3Data(store_id)
                        df_final = classSalesRecapService.TrainMachineLearningSales(df_sales_complete)
                        df_final['store_id'] = store_id
                        df_final = df_final[['store_id', 'total_sales', 'sales_projection', 'sales_forecasting', 'transaction_date']]
                        df_final = df_final.fillna(0)
                        df_final_to_list = list(df_final.itertuples(index = False, name = None))
                        classSalesRecapService.InsertSalesProjection(df_final_to_list)
                        
                except Exception as error:
                    raise error
                
        except Exception as error:
            raise error

    @staticmethod
    def GetSalesOrderDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order"
        query = f"""
        SELECT
            sales_order_id,
            store_id,
            total_amount,
            total_cost_amount,
            customer_id,
            order_date,
            order_time,
            shipping_cost,
            service_charge_amount,
            tax_amount,
            exchange_rate,
            credit_payment_id,
            payment_amount_credit_payment,
            payment_date_credit_payment,
            sales_return_id,
            total_amount_return,
            total_cost_amount_return,
            exchange_rate_return,
            return_date,
            status,
            is_paid,
            credit_payment_status,
            sales_return_status,
            payment_type_id,
            order_source
        FROM
            {table_name}
        WHERE
            store_id = %s
        AND (order_date BETWEEN %s AND %s)
        AND (payment_date_credit_payment IS NULL OR payment_date_credit_payment BETWEEN %s AND %s)
        AND (return_date IS NULL OR return_date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib, fstart_date_range, fnow_wib, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def GetPurchaseOrderDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_purchase_order" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_purchase_order"
        query = f"""
        SELECT
            id,
            store_id,
            purchase_date,
            purchase_time,
            discount_amount,
            shipping_cost,
            tax_amount,
            exchange_rate,
            status,
            is_paid
        FROM {table_name}
        WHERE
            store_id = %s
        AND (purchase_date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def GetInexTransDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_inex_trans" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_inex_trans"
        query = f"""
        SELECT
            id,
            trans_no,
            amount,
            store_id,
            trans_date,
            status,
            status_trans_type
        FROM {table_name}
        WHERE
            store_id = %s
        AND (trans_date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def GetSalesOrderItemDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order_item" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order_item"
        query = f"""
        SELECT
            id,
            store_id,
            product_id,
            product_variant_id,
            product_combo_id,
            sales_order_id,
            qty,
            order_time,
            status
        FROM {table_name}
        FORCE INDEX (store_order_time)
        WHERE
            store_id = %s
        AND (order_time BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def GetStockInOutItemsDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_stock_in_out_items" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_stock_in_out_items"
        query = f"""
        SELECT
            store_id,
            in_out_id,
            product_id,
            product_variant_id,
            qty,
            new_buy_price,
            last_buy_price,
            last_qty,
            avg_buy_price,
            date
        FROM {table_name}
        WHERE
            store_id = %s
        AND (date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def GetStockOpnameItemsDBAnalyticData(fstart_date_range, store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        # start_date_range = now_wib - timedelta(days = 180)
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_stock_opname_items" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_stock_opname_items"
        query = f"""
        SELECT
            store_id,
            opname_id,
            product_id,
            product_variant_id,
            qty,
            qty_sys,
            date
        FROM {table_name}
        WHERE
            store_id = %s
        AND (date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()

    @staticmethod
    def NettProfit1(store_id: int, date_range, start_date_tarik_data, end_date_tarik_data):
        try:
            fstart_date = start_date_tarik_data.strftime('%Y-%m-%d')
            tuple_within_list_1 = SalesRecapService.GetSalesOrderDBAnalyticData(fstart_date, store_id)
            columns_1 = ['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'customer_id', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'credit_payment_id', 'payment_amount_credit_payment', 'payment_date_credit_payment', 'sales_return_id', 'total_amount_return', 'total_cost_amount_return', 'exchange_rate_return', 'return_date', 'order_status', 'is_paid', 'credit_payment_status', 'sales_return_status', 'payment_type_id', 'order_source']
            df_sales = pd.DataFrame(tuple_within_list_1, columns = columns_1)
            
            # df_sales['order_date'] = pd.to_datetime(df_sales['order_date'])
            df_sales['payment_date_credit_payment'] = pd.to_datetime(df_sales['payment_date_credit_payment'])
            # df_sales['return_date'] = pd.to_datetime(df_sales['return_date'])
            if not df_sales.empty:
                # df_sales difilter menjadi column-column yang dibutuhkan saja
                # hilangkan row duplicate df_sales
                df_for_revenue_non_shipping = df_sales.copy()
                df_for_revenue_non_shipping = df_for_revenue_non_shipping[['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'order_status', 'is_paid', 'payment_type_id', 'order_source']]
                df_for_revenue_non_shipping = df_for_revenue_non_shipping.drop_duplicates()
                df_for_revenue_shipping = df_sales.copy()
                df_for_revenue_shipping = df_for_revenue_shipping[['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'order_status', 'is_paid', 'payment_type_id', 'order_source']]
                df_for_revenue_shipping = df_for_revenue_shipping.drop_duplicates()
                df_for_revenue_service_charge = df_sales.copy()
                df_for_revenue_service_charge = df_for_revenue_service_charge[['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'order_status', 'is_paid', 'payment_type_id', 'order_source']]
                df_for_revenue_service_charge = df_for_revenue_service_charge.drop_duplicates()
                df_for_revenue_tax = df_sales.copy()
                df_for_revenue_tax = df_for_revenue_tax[['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'order_status', 'is_paid', 'payment_type_id', 'order_source']]
                df_for_revenue_tax = df_for_revenue_tax.drop_duplicates()
                df_for_goods_cost = df_sales.copy()
                df_for_goods_cost = df_for_goods_cost[['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'order_status', 'is_paid', 'payment_type_id', 'order_source']]
                df_for_goods_cost = df_for_goods_cost.drop_duplicates()
                df_sales_credit_payment = df_sales.copy()
                df_sales_return = df_sales.copy()
                # filter1_1 = (~df_for_revenue_non_shipping['order_status'].isin(['X', 'D']) & (df_for_revenue_non_shipping['is_paid'] == 1) & (df_for_revenue_non_shipping['payment_type_id'] != 'CT')) # Digunakan untuk mode 'Pembayaran'
                filter1_1 = (~df_for_revenue_non_shipping['order_status'].isin(['X', 'D'])) # Digunakan untuk mode 'Penjualan'
                df_for_revenue_non_shipping = df_for_revenue_non_shipping.loc[filter1_1, ['order_date', 'total_amount', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate']]
                # df_for_revenue_non_shipping = df_for_revenue_non_shipping[~df_for_revenue_non_shipping['order_status'].isin(['X', 'D'])]
                # df_for_revenue_non_shipping = df_for_revenue_non_shipping[df_for_revenue_non_shipping['is_paid'] == 1]
                # df_for_revenue_non_shipping = df_for_revenue_non_shipping[df_for_revenue_non_shipping['payment_type_id'] != 'CT']
                # df_for_revenue_non_shipping = df_for_revenue_non_shipping[['order_date', 'total_amount', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate']]
                df_for_revenue_non_shipping['revenue_non_shipping'] = (df_for_revenue_non_shipping['total_amount'] - df_for_revenue_non_shipping['shipping_cost'] - df_for_revenue_non_shipping['service_charge_amount'] - df_for_revenue_non_shipping['tax_amount'])/df_for_revenue_non_shipping['exchange_rate']
                # filter1_2 = (~df_for_revenue_shipping['order_status'].isin(['X', 'D']) & (df_for_revenue_shipping['is_paid'] == 1)) # Digunakan untuk mode 'Pembayaran'
                filter1_2 = (~df_for_revenue_shipping['order_status'].isin(['X', 'D'])) # Digunakan untuk mode 'Penjualan'
                df_for_revenue_shipping = df_for_revenue_shipping.loc[filter1_2, ['order_date', 'shipping_cost', 'exchange_rate']]
                # df_for_revenue_shipping = df_for_revenue_shipping[~df_for_revenue_shipping['order_status'].isin(['X', 'D'])]
                # df_for_revenue_shipping = df_for_revenue_shipping[df_for_revenue_shipping['is_paid'] == 1]
                # df_for_revenue_shipping = df_for_revenue_shipping[['order_date', 'shipping_cost', 'exchange_rate']]
                df_for_revenue_shipping['revenue_shipping'] = df_for_revenue_shipping['shipping_cost']/df_for_revenue_shipping['exchange_rate']
                # filter1_3 = (~df_for_revenue_service_charge['order_status'].isin(['X', 'D']) & (df_for_revenue_service_charge['is_paid'] == 1) & (df_for_revenue_service_charge['payment_type_id'] != 'CT'))  # Digunakan untuk mode 'Pembayaran'
                filter1_3 = (~df_for_revenue_service_charge['order_status'].isin(['X', 'D']) & ((df_for_revenue_service_charge['is_paid'] == 1) | (df_for_revenue_service_charge['payment_type_id'].notnull()))) # Digunakan untuk mode 'Penjualan'
                df_for_revenue_service_charge = df_for_revenue_service_charge.loc[filter1_3, ['order_date', 'service_charge_amount', 'exchange_rate']]
                # df_for_revenue_service_charge = df_for_revenue_service_charge[~df_for_revenue_service_charge['order_status'].isin(['X', 'D'])]
                # df_for_revenue_service_charge = df_for_revenue_service_charge[df_for_revenue_service_charge['is_paid'] == 1]
                # df_for_revenue_service_charge = df_for_revenue_service_charge[df_for_revenue_service_charge['payment_type_id'] != 'CT']
                # df_for_revenue_service_charge = df_for_revenue_service_charge[['order_date', 'service_charge_amount', 'exchange_rate']]
                df_for_revenue_service_charge['revenue_service_charge'] = df_for_revenue_service_charge['service_charge_amount']/df_for_revenue_service_charge['exchange_rate']
                # filter1_4 = (~df_for_revenue_tax['order_status'].isin(['X', 'D']) & (df_for_revenue_tax['is_paid'] == 1) & (df_for_revenue_tax['payment_type_id'] != 'CT')) # Digunakan untuk mode 'Pembayaran'
                filter1_4 = (~df_for_revenue_tax['order_status'].isin(['X', 'D']) & ((df_for_revenue_tax['is_paid'] == 1) | (df_for_revenue_tax['payment_type_id'].notnull()))) # Digunakan untuk mode 'Penjualan'
                df_for_revenue_tax = df_for_revenue_tax.loc[filter1_4, ['order_date', 'tax_amount', 'exchange_rate']]
                # df_for_revenue_tax = df_for_revenue_tax[~df_for_revenue_tax['order_status'].isin(['X', 'D'])]
                # df_for_revenue_tax = df_for_revenue_tax[df_for_revenue_tax['is_paid'] == 1]
                # df_for_revenue_tax = df_for_revenue_tax[df_for_revenue_tax['payment_type_id'] != 'CT']
                # df_for_revenue_tax = df_for_revenue_tax[['order_date', 'tax_amount', 'exchange_rate']]
                df_for_revenue_tax['revenue_tax'] = df_for_revenue_tax['tax_amount']/df_for_revenue_tax['exchange_rate']
                # filter1_5 = (~df_for_goods_cost['order_status'].isin(['X', 'D']) & (df_for_goods_cost['is_paid'] == 1)) # Digunakan untuk mode 'Pembayaran'
                filter1_5 = (~df_for_goods_cost['order_status'].isin(['X', 'D'])) # Digunakan untuk mode 'Penjualan'
                df_for_goods_cost = df_for_goods_cost.loc[filter1_5, ['order_date', 'total_cost_amount', 'exchange_rate']]
                # df_for_goods_cost = df_for_goods_cost[~df_for_goods_cost['order_status'].isin(['X', 'D'])]
                # df_for_goods_cost = df_for_goods_cost[df_for_goods_cost['is_paid'] == 1]
                # df_for_goods_cost = df_for_goods_cost[['order_date', 'total_cost_amount', 'exchange_rate']]
                df_for_goods_cost['goods_cost'] = df_for_goods_cost['total_cost_amount']/df_for_goods_cost['exchange_rate']

                df_for_revenue_non_shipping = df_for_revenue_non_shipping.groupby(["order_date"], as_index=False)["revenue_non_shipping"].sum()
                df_for_revenue_shipping = df_for_revenue_shipping.groupby(["order_date"], as_index=False)["revenue_shipping"].sum()
                df_for_revenue_service_charge = df_for_revenue_service_charge.groupby(["order_date"], as_index = False)["revenue_service_charge"].sum()
                df_for_revenue_tax = df_for_revenue_tax.groupby(["order_date"], as_index = False)["revenue_tax"].sum()
                df_for_goods_cost = df_for_goods_cost.groupby(["order_date"], as_index = False)["goods_cost"].sum()

                df_for_revenue_non_shipping['order_date'] = pd.to_datetime(df_for_revenue_non_shipping['order_date'])
                df_for_revenue_shipping['order_date'] = pd.to_datetime(df_for_revenue_shipping['order_date'])
                df_for_revenue_service_charge['order_date'] = pd.to_datetime(df_for_revenue_service_charge['order_date'])
                df_for_revenue_tax['order_date'] = pd.to_datetime(df_for_revenue_tax['order_date'])
                df_for_goods_cost['order_date'] = pd.to_datetime(df_for_goods_cost['order_date'])

                df_for_revenue_non_shipping["revenue_non_shipping"] = df_for_revenue_non_shipping["revenue_non_shipping"].astype(float)
                df_for_revenue_shipping["revenue_shipping"] = df_for_revenue_shipping["revenue_shipping"].astype(float)
                df_for_revenue_service_charge["revenue_service_charge"] = df_for_revenue_service_charge["revenue_service_charge"].astype(float)
                df_for_revenue_tax["revenue_tax"] = df_for_revenue_tax["revenue_tax"].astype(float)
                df_for_goods_cost["goods_cost"] = df_for_goods_cost["goods_cost"].astype(float)
                
                df_complete_1 = pd.DataFrame(date_range, columns=['order_date'])

                df_join_1_1 = pd.merge(df_for_revenue_non_shipping, df_for_revenue_shipping, on = 'order_date', how = 'left')
                df_join_1_2 = pd.merge(df_join_1_1, df_for_revenue_service_charge, on = 'order_date', how = 'left')
                df_join_1_3 = pd.merge(df_join_1_2, df_for_revenue_tax, on = 'order_date', how = 'left')
                df_join_1_4 = pd.merge(df_join_1_3, df_for_goods_cost, on = 'order_date', how = 'left')

                df_sales_complete = pd.merge(df_complete_1, df_join_1_4, on = 'order_date', how = 'left')
                df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']] = df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']].fillna(0)
                df_sales_complete = df_sales_complete[(df_sales_complete["order_date"] >= start_date_tarik_data) & (df_sales_complete["order_date"] <= end_date_tarik_data)]
                df_sales_complete = df_sales_complete.rename(columns = {
                    'order_date': 'date_trx_days'
                })
            else:
                df_sales = pd.DataFrame(columns=['order_date', 'revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost'])
                # Uncomment line ini jika mode yang digunakan adalah 'Pembayaran'
                # df_sales_credit_payment = pd.DataFrame(columns=[
                #     'payment_date_credit_payment', 
                #     'payment_amount_credit_payment',
                #     'order_status',
                #     'credit_payment_status',
                #     'credit_payment_id',
                #     'store_id'
                #     ])
                df_sales_return = pd.DataFrame(columns=[
                    'return_date',
                    'total_amount_return',
                    'total_cost_amount_return',
                    'exchange_rate',
                    'sales_return_status',
                    'sales_return_id',
                    'store_id'
                    ])
                df_complete_1 = pd.DataFrame(date_range, columns=['order_date'])
                if 'order_date' not in df_sales.columns:
                    df_sales['order_date'] = pd.NaT
                df_sales_complete = pd.merge(df_complete_1, df_sales, on='order_date', how='left')
                df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']] = df_sales_complete[['revenue_non_shipping', 'revenue_shipping', 'revenue_service_charge', 'revenue_tax', 'goods_cost']].fillna(0)
                df_sales_complete = df_sales_complete[(df_sales_complete["order_date"] >= start_date_tarik_data) & (df_sales_complete["order_date"] <= end_date_tarik_data)]
                df_sales_complete = df_sales_complete.rename(columns = {
                    'order_date': 'date_trx_days'
                })
            
            tuple_within_list_2 = SalesRecapService.GetPurchaseOrderDBAnalyticData(fstart_date, store_id)
            columns_2 = ["id", "store_id", "purchase_date", "purchase_time", "discount_amount", "shipping_cost", "tax_amount", "exchange_rate", "status", "is_paid"]
            df_purchase_order = pd.DataFrame(tuple_within_list_2, columns = columns_2)
            if not df_purchase_order.empty:
                df_for_purchases_discount = df_purchase_order.copy()
                df_for_purchases_shipping = df_purchase_order.copy()
                df_for_purchases_tax = df_purchase_order.copy()

                df_for_purchases_discount = df_for_purchases_discount[~df_for_purchases_discount['status'].isin(['X', 'D'])]
                df_for_purchases_discount = df_for_purchases_discount[['purchase_date', 'discount_amount']]
                df_for_purchases_discount['purchases_discount'] = df_for_purchases_discount['discount_amount']
                df_for_purchases_shipping = df_for_purchases_shipping[~df_for_purchases_shipping['status'].isin(['X', 'D'])]
                df_for_purchases_shipping = df_for_purchases_shipping[df_for_purchases_shipping['is_paid'] == 1]
                df_for_purchases_shipping = df_for_purchases_shipping[['purchase_date', 'shipping_cost', 'exchange_rate']]
                df_for_purchases_shipping['purchases_shipping'] = df_for_purchases_shipping['shipping_cost']/df_for_purchases_shipping['exchange_rate']
                df_for_purchases_tax = df_for_purchases_tax[~df_for_purchases_tax['status'].isin(['X', 'D'])]
                df_for_purchases_tax = df_for_purchases_tax[df_for_purchases_tax['is_paid'] == 1]
                df_for_purchases_tax = df_for_purchases_tax[['purchase_date', 'tax_amount', 'exchange_rate']]
                df_for_purchases_tax['purchases_tax'] = df_for_purchases_tax['tax_amount']/df_for_purchases_tax['exchange_rate']

                df_for_purchases_discount = df_for_purchases_discount.groupby(["purchase_date"], as_index = False)['purchases_discount'].sum()
                df_for_purchases_shipping = df_for_purchases_shipping.groupby(["purchase_date"], as_index = False)['purchases_shipping'].sum()
                df_for_purchases_tax = df_for_purchases_tax.groupby(["purchase_date"], as_index = False)['purchases_tax'].sum()

                df_for_purchases_discount['purchase_date'] = pd.to_datetime(df_for_purchases_discount['purchase_date'])
                df_for_purchases_shipping['purchase_date'] = pd.to_datetime(df_for_purchases_shipping['purchase_date'])
                df_for_purchases_tax['purchase_date'] = pd.to_datetime(df_for_purchases_tax['purchase_date'])

                df_for_purchases_discount['purchases_discount'] = df_for_purchases_discount['purchases_discount'].astype(float)
                df_for_purchases_shipping['purchases_shipping'] = df_for_purchases_shipping['purchases_shipping'].astype(float)
                df_for_purchases_tax['purchases_tax'] = df_for_purchases_tax['purchases_tax'].astype(float)

                df_complete_2 = pd.DataFrame(date_range, columns=['purchase_date'])

                df_join_2_1 = pd.merge(df_for_purchases_discount, df_for_purchases_shipping, on ='purchase_date', how = 'left')
                df_join_2_2 = pd.merge(df_join_2_1, df_for_purchases_tax, on = 'purchase_date', how = 'left')

                df_purchase_order_complete = pd.merge(df_complete_2, df_join_2_2, on = 'purchase_date', how = 'left')
                df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']] = df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']].fillna(0)
                df_purchase_order_complete = df_purchase_order_complete[(df_purchase_order_complete["purchase_date"] >= start_date_tarik_data) & (df_purchase_order_complete["purchase_date"] <= end_date_tarik_data)]
                df_purchase_order_complete = df_purchase_order_complete.rename(columns = {
                    'purchase_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_sales_complete, df_purchase_order_complete, on = 'date_trx_days')
            else:
                df_purchase_order = pd.DataFrame(columns=['purchase_date', 'purchases_discount', 'purchases_shipping', 'purchases_tax'])
                df_complete_2 = pd.DataFrame(date_range, columns=['purchase_date'])
                if 'purchase_date' not in df_purchase_order.columns:
                    df_purchase_order['purchase_date'] = pd.NaT
                df_purchase_order_complete = pd.merge(df_complete_2, df_purchase_order, on='purchase_date', how='left')
                df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']] = df_purchase_order_complete[['purchases_discount', 'purchases_shipping', 'purchases_tax']].fillna(0)
                df_purchase_order_complete = df_purchase_order_complete[(df_purchase_order_complete["purchase_date"] >= start_date_tarik_data) & (df_purchase_order_complete["purchase_date"] <= end_date_tarik_data)]
                df_purchase_order_complete = df_purchase_order_complete.rename(columns = {
                    'purchase_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_sales_complete, df_purchase_order_complete, on = 'date_trx_days')
            
            """
            Catatan untuk perhitungan credit_payment:
            Jika mode yang digunakan adalah 'Pembayaran', maka line
            `df_sales_credit_payment`
            sampai
            `df_complete`
            perlu di-uncomment
            Untuk mode 'Penjualan', sudah dicover di penghapusan filter payment_type_id <> 'CT' sehingga perhitungan
            `df_sales_credit_payment` dapat di-comment
            """
            
            # # df_sales_credit_payment difilter menjadi column-column yang dibutuhkan saja
            # # hilangkan row duplicate df_sales_credit_payment
            # df_sales_credit_payment = df_sales_credit_payment[['credit_payment_id', 'store_id', 'payment_amount_credit_payment', 'payment_date_credit_payment', 'order_status', 'credit_payment_status']]
            # df_sales_credit_payment = df_sales_credit_payment.drop_duplicates()
            # df_sales_credit_payment = df_sales_credit_payment[~df_sales_credit_payment['order_status'].isin(['X', 'D'])]
            # df_sales_credit_payment = df_sales_credit_payment[df_sales_credit_payment['credit_payment_status'] != 'X']
            # df_sales_credit_payment['revenue_credit_payment'] = df_sales_credit_payment['payment_amount_credit_payment']
            # df_sales_credit_payment = df_sales_credit_payment.groupby(["payment_date_credit_payment"], as_index=False)["revenue_credit_payment"].sum()
            
            # if not df_sales_credit_payment.empty:
            #     df_sales_credit_payment['payment_date_credit_payment'] = pd.to_datetime(df_sales_credit_payment['payment_date_credit_payment'])
            #     df_sales_credit_payment['revenue_credit_payment'] = df_sales_credit_payment['revenue_credit_payment'].astype(float)
            #     df_complete_3 = pd.DataFrame(training_date_range, columns=['payment_date_credit_payment'])
            #     df_sales_credit_payment_complete = pd.merge(df_complete_3, df_sales_credit_payment, on='payment_date_credit_payment', how='left')
            #     df_sales_credit_payment_complete['revenue_credit_payment'] = df_sales_credit_payment_complete['revenue_credit_payment'].fillna(0)
            #     df_sales_credit_payment_complete = df_sales_credit_payment_complete[(df_sales_credit_payment_complete["payment_date_credit_payment"] >= start_date_training) & (df_sales_credit_payment_complete["payment_date_credit_payment"] <= end_date_training)]
            #     df_sales_credit_payment_complete = df_sales_credit_payment_complete.rename(columns = {
            #         'payment_date_credit_payment': 'date_trx_days'
            #     })
            #     df_complete = pd.merge(df_complete, df_sales_credit_payment_complete, on = 'date_trx_days')
            # else:
            #     df_sales_credit_payment = pd.DataFrame(columns=['payment_date_credit_payment', 'revenue_credit_payment'])
            #     df_complete_3 = pd.DataFrame(training_date_range, columns=['payment_date_credit_payment'])
            #     if 'payment_date_credit_payment' not in df_sales_credit_payment.columns:
            #         df_sales_credit_payment['payment_date_credit_payment'] = pd.NaT
            #     df_sales_credit_payment_complete = pd.merge(df_complete_3, df_sales_credit_payment, on='payment_date_credit_payment', how='left')
            #     df_sales_credit_payment_complete['revenue_credit_payment'] = df_sales_credit_payment_complete['revenue_credit_payment'].fillna(0)
            #     df_sales_credit_payment_complete = df_sales_credit_payment_complete[(df_sales_credit_payment_complete["payment_date_credit_payment"] >= start_date_training) & (df_sales_credit_payment_complete["payment_date_credit_payment"] <= end_date_training)]
            #     df_sales_credit_payment_complete = df_sales_credit_payment_complete.rename(columns = {
            #         'payment_date_credit_payment': 'date_trx_days'
            #     })
            #     df_complete = pd.merge(df_complete, df_sales_credit_payment_complete, on = 'date_trx_days')
            
            # df_sales_return difilter menjadi column-column yang dibutuhkan saja
            # hilangkan row duplicate df_sales_return
            df_sales_return = df_sales_return[['sales_return_id', 'store_id', 'total_amount_return', 'total_cost_amount_return', 'return_date', 'sales_return_status', 'exchange_rate']]
            df_sales_return = df_sales_return.drop_duplicates()
            df_sales_return = df_sales_return[df_sales_return['sales_return_status'] != 'X']
            df_sales_return['return'] = df_sales_return['total_amount_return']/df_sales_return['exchange_rate']
            df_sales_return['return_cost'] = df_sales_return['total_cost_amount_return']/df_sales_return['exchange_rate']
            df_sales_return = df_sales_return.groupby(["return_date"], as_index=False).agg({
                "return": "sum",
                "return_cost": "sum"
            })

            # Check if the result of the groupby is not empty
            if not df_sales_return.empty:
                df_sales_return['return_date'] = pd.to_datetime(df_sales_return['return_date'])
                df_sales_return[['return', 'return_cost']] = df_sales_return[['return', 'return_cost']].astype(float)
                df_complete_4 = pd.DataFrame(date_range, columns=['return_date'])
                df_sales_return_complete = pd.merge(df_complete_4, df_sales_return, on='return_date', how='left')
                df_sales_return_complete[['return', 'return_cost']] = df_sales_return_complete[['return', 'return_cost']].fillna(0)
                df_sales_return_complete = df_sales_return_complete[(df_sales_return_complete["return_date"] >= start_date_tarik_data) & (df_sales_return_complete["return_date"] <= end_date_tarik_data)]
                df_sales_return_complete = df_sales_return_complete.rename(columns = {
                    'return_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_sales_return_complete, on = 'date_trx_days')
            else:
                df_sales_return = pd.DataFrame(columns=['return_date', 'return', 'return_cost'])
                df_complete_4 = pd.DataFrame(date_range, columns=['return_date'])
                if 'return_date' not in df_sales_return.columns:
                    df_sales_return['return_date'] = pd.NaT
                df_sales_return_complete = pd.merge(df_complete_4, df_sales_return, on='return_date', how='left')
                df_sales_return_complete[['return', 'return_cost']] = df_sales_return_complete[['return', 'return_cost']].fillna(0)
                df_sales_return_complete = df_sales_return_complete[(df_sales_return_complete["return_date"] >= start_date_tarik_data) & (df_sales_return_complete["return_date"] <= end_date_tarik_data)]
                df_sales_return_complete = df_sales_return_complete.rename(columns = {
                    'return_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_sales_return_complete, on = 'date_trx_days')
            
            tuple_within_list_3 = SalesRecapService.GetInexTransDBAnalyticData(fstart_date, store_id)
            columns_3 = ["id", "trans_no", "amount", "store_id", "trans_date", "status", "status_trans_type"]
            df_inex_trans = pd.DataFrame(tuple_within_list_3, columns = columns_3)
            if not df_inex_trans.empty:
                df_inex_trans = df_inex_trans[df_inex_trans['status'] != 'X']
                df_inex_trans = df_inex_trans[df_inex_trans['status_trans_type'] == 'E']
                df_inex_trans['expense_amount'] = df_inex_trans['amount']
                df_inex_trans = df_inex_trans.groupby(["trans_date"], as_index=False)["expense_amount"].sum()
                df_inex_trans['trans_date'] = pd.to_datetime(df_inex_trans['trans_date'])
                df_inex_trans['expense_amount'] = df_inex_trans['expense_amount'].astype(float)
                df_complete_5 = pd.DataFrame(date_range, columns=['trans_date'])
                df_inex_trans_complete = pd.merge(df_complete_5, df_inex_trans, on='trans_date', how='left')
                df_inex_trans_complete['expense_amount'] = df_inex_trans_complete['expense_amount'].fillna(0)
                df_inex_trans_complete = df_inex_trans_complete[(df_inex_trans_complete["trans_date"] >= start_date_tarik_data) & (df_inex_trans_complete["trans_date"] <= end_date_tarik_data)]
                df_inex_trans_complete = df_inex_trans_complete.rename(columns = {
                    'trans_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_inex_trans_complete, on = 'date_trx_days')
            else:
                df_inex_trans = pd.DataFrame(columns=['trans_date', 'expense_amount'])
                df_complete_5 = pd.DataFrame(date_range, columns=['trans_date'])
                if 'trans_date' not in df_inex_trans.columns:
                    df_inex_trans['trans_date'] = pd.NaT
                df_inex_trans_complete = pd.merge(df_complete_5, df_inex_trans, on='trans_date', how='left')
                df_inex_trans_complete['expense_amount'] = df_inex_trans_complete['expense_amount'].fillna(0)
                df_inex_trans_complete = df_inex_trans_complete[(df_inex_trans_complete["trans_date"] >= start_date_tarik_data) & (df_inex_trans_complete["trans_date"] <= end_date_tarik_data)]
                df_inex_trans_complete = df_inex_trans_complete.rename(columns = {
                    'trans_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_inex_trans_complete, on = 'date_trx_days')
            
            df_complete['revenue_credit_payment'] = 0 # Comment line ini jika mode yang digunakan adalah 'Pembayaran'
            df_complete['nett_profit_total_1'] = df_complete['revenue_non_shipping'] + df_complete['revenue_service_charge'] + df_complete['revenue_credit_payment'] + df_complete['return_cost'] + df_complete['purchases_discount'] - df_complete['purchases_shipping'] - df_complete['purchases_tax'] - df_complete['return'] - df_complete['goods_cost'] - df_complete['expense_amount']
            return df_complete
        except Exception as e:
            raise e

    @staticmethod
    def NettProfit2(df_complete_from_nett_profit_1, store_id: int, date_range, start_date_tarik_data, end_date_tarik_data):
       try:
            # panggil tuple dalam list yang sudah dibuat mas fadil
            # ubah tuple dalam list menjadi dataframe bernama df_sales dan df_sales_order_item_for_food_delivery
            df_complete = df_complete_from_nett_profit_1
            fstart_date = start_date_tarik_data.strftime('%Y-%m-%d')
            tuple_within_list_4 = SalesRecapService.GetSalesOrderDBAnalyticData(fstart_date, store_id)
            columns_4 = ['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'customer_id', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'credit_payment_id', 'payment_amount_credit_payment', 'payment_date_credit_payment', 'sales_return_id', 'total_amount_return', 'total_cost_amount_return', 'exchange_rate_return', 'return_date', 'order_status', 'is_paid', 'credit_payment_status', 'sales_return_status', 'payment_type_id', 'order_source']
            df_sales_for_food_delivery = pd.DataFrame(tuple_within_list_4, columns = columns_4)
            tuple_within_list_5 = SalesRecapService.GetSalesOrderItemDBAnalyticData(fstart_date, store_id)
            columns_5 = ["id", "store_id", "product_id", "product_variant_id", "product_combo_id", "sales_order_id", "qty", "order_time", "status"]
            df_sales_order_item_for_food_delivery = pd.DataFrame(tuple_within_list_5, columns = columns_5)
            df_sales_order_item_for_food_delivery['product_variant_id'] = df_sales_order_item_for_food_delivery['product_variant_id'].fillna(0)
            df_sales_order_item_for_food_delivery['product_variant_id'] = df_sales_order_item_for_food_delivery['product_variant_id'].astype(int)
            df_sales_for_food_delivery = pd.merge(df_sales_for_food_delivery, df_sales_order_item_for_food_delivery, on = 'sales_order_id', how = 'left')
            df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['order_source'].isin(['X', 'Y', 'Z', 'V'])]
            df_sales_for_food_delivery = df_sales_for_food_delivery[~df_sales_for_food_delivery['order_status'].isin(['X', 'D'])]
            df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['status'] == 'A']
            df_sales_for_food_delivery = df_sales_for_food_delivery[
                np.where(df_sales_for_food_delivery['product_combo_id'] == 0, 
                        df_sales_for_food_delivery['product_id'], 
                        999) > 1
            ]
            df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['is_paid'] == 1]
            df_sales_for_food_delivery = df_sales_for_food_delivery[df_sales_for_food_delivery['payment_type_id'] != 'CT']

            if not df_sales_for_food_delivery.empty:
                df_sales_for_food_delivery['food_delivery_amount'] = df_sales_for_food_delivery['total_amount']
                df_sales_for_food_delivery = df_sales_for_food_delivery.groupby(["order_date"], as_index = False)["food_delivery_amount"].sum()
                df_sales_for_food_delivery['order_date'] = pd.to_datetime(df_sales_for_food_delivery['order_date'])
                df_sales_for_food_delivery['food_delivery_amount'] = df_sales_for_food_delivery['food_delivery_amount'].astype(float)
                df_complete_6 = pd.DataFrame(date_range, columns=['order_date'])
                df_sales_for_food_delivery_complete = pd.merge(df_complete_6, df_sales_for_food_delivery, on = 'order_date', how = 'left')
                df_sales_for_food_delivery_complete['food_delivery_amount'] = df_sales_for_food_delivery_complete['food_delivery_amount'].fillna(0)
                df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete[(df_sales_for_food_delivery_complete["order_date"] >= start_date_tarik_data) & (df_sales_for_food_delivery_complete["order_date"] <= end_date_tarik_data)]
                df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete.rename(columns = {
                    'order_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_sales_for_food_delivery_complete, on = 'date_trx_days')
            else:
                df_sales_for_food_delivery = pd.DataFrame(columns = ['order_date', 'food_delivery_amount'])
                df_complete_6 = pd.DataFrame(date_range, columns=['order_date'])
                if 'order_date' not in df_sales_for_food_delivery.columns:
                    df_sales_for_food_delivery['order_date'] = pd.NaT
                df_sales_for_food_delivery_complete = pd.merge(df_complete_6, df_sales_for_food_delivery, on = 'order_date', how = 'left')
                df_sales_for_food_delivery_complete['food_delivery_amount'] = df_sales_for_food_delivery_complete['food_delivery_amount'].fillna(0)
                df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete[(df_sales_for_food_delivery_complete["order_date"] >= start_date_tarik_data) & (df_sales_for_food_delivery_complete["order_date"] <= end_date_tarik_data)]
                df_sales_for_food_delivery_complete = df_sales_for_food_delivery_complete.rename(columns = {
                    'order_date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_sales_for_food_delivery_complete, on = 'date_trx_days')
            
            df_complete['food_delivery_amount'] = abs(df_complete['food_delivery_amount'])
            df_complete['nett_profit_total_2'] = df_complete['nett_profit_total_1'] - df_complete['food_delivery_amount']

            return df_complete
       except Exception as e:
           raise e
    
    @staticmethod
    def NettProfit3(df_complete_from_nett_profit_2, store_id: int, date_range, start_date_tarik_data, end_date_tarik_data):
        try:
            df_complete = df_complete_from_nett_profit_2
            fstart_date = start_date_tarik_data.strftime('%Y-%m-%d')
            tuple_within_list_6 = SalesRecapService.GetStockInOutItemsDBAnalyticData(fstart_date, store_id)
            columns_6 = ["store_id", "in_out_id", "product_id", "product_variant_id", "qty", "new_buy_price", "last_buy_price", "last_qty", "avg_buy_price", "date"]
            df_stock_out_record = pd.DataFrame(tuple_within_list_6, columns = columns_6)
            # Inisialisasi df_complete_9 di sini untuk menghindari kesalahan UnboundLocalError
            df_complete_9 = pd.DataFrame(date_range, columns=['date'])
            if not df_stock_out_record.empty:
                df_stock_out_record['stock_out_record_amount'] = df_stock_out_record['new_buy_price'] * df_stock_out_record['qty']
                df_stock_out_record = df_stock_out_record.groupby(["date"], as_index = False)["stock_out_record_amount"].sum()
                df_stock_out_record['date'] = pd.to_datetime(df_stock_out_record['date'])
                df_stock_out_record['stock_out_record_amount'] = df_stock_out_record['stock_out_record_amount'].astype(float)
                df_complete_7 = pd.DataFrame(date_range, columns=['date'])
                df_stock_out_record_complete = pd.merge(df_complete_7, df_stock_out_record, on = 'date', how = 'left')
                df_stock_out_record_complete['stock_out_record_amount'] = df_stock_out_record_complete['stock_out_record_amount'].fillna(0)
                df_stock_out_record_complete = df_stock_out_record_complete[(df_stock_out_record_complete["date"] >= start_date_tarik_data) & (df_stock_out_record_complete["date"] <= end_date_tarik_data)]
                df_stock_out_record_complete = df_stock_out_record_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_stock_out_record_complete, on = 'date_trx_days')
            else:
                df_stock_out_record = pd.DataFrame(columns = ['date', 'stock_out_record_amount'])
                df_complete_7 = pd.DataFrame(date_range, columns=['date'])
                if 'date' not in df_stock_out_record.columns:
                    df_stock_out_record['date'] = pd.NaT
                df_stock_out_record_complete = pd.merge(df_complete_7, df_stock_out_record, on = 'date', how = 'left')
                df_stock_out_record_complete['stock_out_record_amount'] = df_stock_out_record_complete['stock_out_record_amount'].fillna(0)
                df_stock_out_record_complete = df_stock_out_record_complete[(df_stock_out_record_complete["date"] >= start_date_tarik_data) & (df_stock_out_record_complete["date"] <= end_date_tarik_data)]
                df_stock_out_record_complete = df_stock_out_record_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_stock_out_record_complete, on = 'date_trx_days')
            
            df_complete['stock_out_record_amount'] = abs(df_complete['stock_out_record_amount'])
            df_complete['nett_profit_total_3'] = df_complete['nett_profit_total_2'] - df_complete['stock_out_record_amount']

            tuple_within_list_7 = SalesRecapService.GetStockOpnameItemsDBAnalyticData(fstart_date, store_id)
            columns_7 = ["store_id", "opname_id", "product_id", "product_variant_id", "qty", "qty_sys", "date"]
            df_stock_opname = pd.DataFrame(tuple_within_list_7, columns = columns_7)
            if not df_stock_opname.empty:
                pass
            else:
                df_stock_opname = pd.DataFrame(columns = ['opname_id', 'product_id', 'product_variant_id', 'qty', 'qty_sys', 'date'])
            
            tuple_within_list_8 = ProductMaterialsModel.ProductMaterials.GetDataProductMaterials(store_id)
            columns_8 = ["id", "product_id", "product_variant_id", "material_product_id", "material_product_variant_id", "qty", "uom", "uom_conversion"]
            df_product_materials = pd.DataFrame(tuple_within_list_8, columns = columns_8)
            if not df_product_materials.empty:
                df_product_materials = df_product_materials.rename(columns = {
                    'qty': 'qty_product_materials'
                })
            else:
                df_product_materials = pd.DataFrame(columns = ['id', 'product_id', 'product_variant_id', 'material_product_id', 'material_product_variant_id', 'qty_product_materials', 'uom', 'uom_conversion'])
            
            tuple_within_list_9 = ProductVariantMaterialsModel.ProductVariantMaterials.GetDataProductVariantMaterials(store_id)
            columns_9 = ["id", "product_id", "product_variant_id", "material_product_id", "material_product_variant_id", "qty", "uom", "uom_conversion"]
            df_product_variant_materials = pd.DataFrame(tuple_within_list_9, columns = columns_9)
            if not df_product_variant_materials.empty:
                df_product_variant_materials = df_product_variant_materials.rename(columns = {
                    'qty': 'qty_product_variant_materials'
                })
            else:
                df_product_variant_materials = pd.DataFrame(columns = ['id', 'product_id', 'product_variant_id', 'material_product_id', 'material_product_variant_id', 'qty_product_variant_materials', 'uom', 'uom_conversion'])
            
            tuple_within_list_10 = ProductModel.Product.ProductMaster(store_id)
            columns_10 = ["id", "store_id", "name", "category_id", "status", "buy_price"]
            df_product_master = pd.DataFrame(tuple_within_list_10, columns = columns_10)

            if not df_product_master.empty:
                df_product_master = df_product_master.rename(columns = {
                    'id': 'product_id'
                })
            else:
                df_product_master = pd.DataFrame(columns = ['product_id', 'store_id', 'name', 'category_id', 'status', 'buy_price'])
            
            tuple_within_list_11 = ProductModel.Product.ProductVariant(store_id)
            columns_11 = ["id", "store_id", "name", "category_id", "status", "buy_price"]
            df_product_variant_master = pd.DataFrame(tuple_within_list_11, columns = columns_11)

            if not df_product_variant_master.empty:
                df_product_variant_master = df_product_variant_master.rename(columns = {
                    'id': 'product_variant_id'
                })
            else:
                df_product_variant_master = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'name', 'category_id', 'status', 'buy_price'])
            
            # df_to_join_product_variant_master = pd.merge(df_product_variant_materials, df_product_variant_master, how = 'inner', on = 'product_variant_id')
            # df_to_join_product_master = pd.merge(df_product_materials, df_product_master, how = 'inner', on = 'product_id')
            # df_opname_product_variant = pd.merge(df_to_join_product_variant_master, df_stock_opname, how = 'inner', on = 'product_variant_id')
            # df_opname_product_only = pd.merge(df_to_join_product_master, df_stock_opname, how = 'inner', on = 'product_id')
            df_opname_product_variant = pd.merge(df_product_variant_master, df_stock_opname, how = 'inner', on = 'product_variant_id')
            df_opname_product_variant = df_opname_product_variant.drop_duplicates()
            df_opname_product_only = pd.merge(df_product_master, df_stock_opname, how = 'inner', on = 'product_id')
            df_opname_product_only = df_opname_product_only.drop_duplicates()

            if not df_opname_product_variant.empty:
                # df_opname_product_variant['pv_materials_buy_price'] = df_opname_product_variant.apply(
                #     lambda row: row['buy_price'] * row['qty_product_variant_materials'] * row['uom_conversion'] if row['uom_conversion'] > 0 else row['buy_price'] * row['qty_product_variant_materials']
                # )
                df_opname_product_variant['pv_materials_buy_price'] = df_opname_product_variant['buy_price'] * (df_opname_product_variant['qty'] - df_opname_product_variant['qty_sys'])
                df_opname_product_variant = df_opname_product_variant.groupby(["date"], as_index = False)["pv_materials_buy_price"].sum() # jika ada filter, column filternya akan masuk ke groupby
                df_opname_product_variant['date'] = pd.to_datetime(df_opname_product_variant['date'])
                df_opname_product_variant['pv_materials_buy_price'] = df_opname_product_variant['pv_materials_buy_price'].astype(float)
                df_complete_8 = pd.DataFrame(date_range, columns = ['date'])
                df_opname_product_variant_complete = pd.merge(df_complete_8, df_opname_product_variant, on = 'date', how = 'left')
                df_opname_product_variant_complete['pv_materials_buy_price'] = df_opname_product_variant_complete['pv_materials_buy_price'].fillna(0)
                df_opname_product_variant_complete = df_opname_product_variant_complete[(df_opname_product_variant_complete["date"] >= start_date_tarik_data) & (df_opname_product_variant_complete["date"] <= end_date_tarik_data)]
                df_opname_product_variant_complete = df_opname_product_variant_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_opname_product_variant_complete, on = 'date_trx_days')
            else:
                df_opname_product_variant = pd.DataFrame(columns = ['date', 'pv_materials_buy_price'])
                df_complete_8 = pd.DataFrame(date_range, columns = ['date'])
                if 'date' not in df_opname_product_variant.columns:
                    df_opname_product_variant['date'] = pd.NaT
                df_opname_product_variant_complete = pd.merge(df_complete_8, df_opname_product_variant, on = 'date', how = 'left')
                df_opname_product_variant_complete['pv_materials_buy_price'] = df_opname_product_variant_complete['pv_materials_buy_price'].fillna(0)
                df_opname_product_variant_complete = df_opname_product_variant_complete[(df_opname_product_variant_complete["date"] >= start_date_tarik_data) & (df_opname_product_variant_complete["date"] <= end_date_tarik_data)]
                df_opname_product_variant_complete = df_opname_product_variant_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_opname_product_variant_complete, on = 'date_trx_days')
            
            if not df_opname_product_only.empty:
                pd.set_option('future.no_silent_downcasting', True)
                df_opname_product_only['date'] = pd.to_datetime(df_opname_product_only['date'])  # Pastikan kolom date bertipe datetime
                df_complete_9['date'] = pd.to_datetime(df_complete_9['date'])  # Pastikan kolom date bertipe datetime

                # df_opname_product_only['p_materials_buy_price'] = df_opname_product_only.apply(
                #     lambda row: row['buy_price'] * row['qty_product_materials'] * row['uom_conversion'] if row['uom_conversion'] > 0 else row['buy_price'] * row['qty_product_materials']
                # )
                # df_opname_product_only['p_materials_buy_price'] = df_opname_product_only.apply(
                #     lambda row: row['buy_price'] * row['qty_product_materials'] * row['uom_conversion'] 
                #     if pd.notnull(row['uom_conversion']) and row['uom_conversion'] > 0 
                #     else row['buy_price'] * row['qty_product_materials'],
                #     axis=1
                # )
                df_opname_product_only['p_materials_buy_price'] = df_opname_product_only['buy_price'] * (df_opname_product_only['qty'] - df_opname_product_only['qty_sys'])
                df_opname_product_only = df_opname_product_only.groupby(["date"], as_index = False)["p_materials_buy_price"].sum() # jika ada filter, column filternya akan masuk ke groupby
                df_opname_product_only['p_materials_buy_price'] = df_opname_product_only['p_materials_buy_price'].astype(float)
                df_complete_9 = pd.DataFrame(date_range, columns = ['date'])
                df_opname_product_only_complete = pd.merge(df_complete_9, df_opname_product_only, on ='date', how = 'left')
                # df_opname_product_only_complete['p_materials_buy_price'] = df_opname_product_only_complete['p_materials_buy_price'].fillna(0)
                df_opname_product_only_complete['p_materials_buy_price'] = (
                    df_opname_product_only_complete['p_materials_buy_price']
                    .fillna(0)
                    .infer_objects(copy=False)
                )
                df_opname_product_only_complete = df_opname_product_only_complete[(df_opname_product_only_complete["date"] >= start_date_tarik_data) & (df_opname_product_only_complete["date"] <= end_date_tarik_data)]
                df_opname_product_only_complete = df_opname_product_only_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_opname_product_only_complete, on = 'date_trx_days')
            else:
                df_opname_product_only = pd.DataFrame(columns = ['date', 'p_materials_buy_price'])
                df_complete_9 = pd.DataFrame(date_range, columns = ['date'])
                if 'date' not in df_opname_product_only.columns:
                    df_opname_product_only['date'] = pd.NaT
                df_opname_product_only_complete = pd.merge(df_complete_9, df_opname_product_only, on ='date', how = 'left')
                df_opname_product_only_complete['p_materials_buy_price'] = df_opname_product_only_complete['p_materials_buy_price'].fillna(0)
                df_opname_product_only_complete = df_opname_product_only_complete[(df_opname_product_only_complete["date"] >= start_date_tarik_data) & (df_opname_product_only_complete["date"] <= end_date_tarik_data)]
                df_opname_product_only_complete = df_opname_product_only_complete.rename(columns = {
                    'date': 'date_trx_days'
                })
                df_complete = pd.merge(df_complete, df_opname_product_only_complete, on = 'date_trx_days')
            
            df_complete['pv_materials_buy_price'] = df_complete['pv_materials_buy_price'].astype(float)
            df_complete['p_materials_buy_price'] = df_complete['p_materials_buy_price'].astype(float)

            def calculate_nett_profit(row):
                if row['pv_materials_buy_price'] != 0:
                    return row['nett_profit_total_3'] + row['pv_materials_buy_price']
                else:
                    return row['nett_profit_total_3'] + row['p_materials_buy_price']
            
            df_complete['nett_profit_total'] = df_complete.apply(calculate_nett_profit, axis=1)

            return df_complete
        except Exception as e:
            raise e
    
    @staticmethod
    def TrainMachineLearningLaba(df_complete, future_dates):
        try:
            train = df_complete[['date_trx_days', 'nett_profit_total']].reset_index()
            train.set_index("date_trx_days", inplace = True)
            train.index.name = "date_trx_days"

            # # Calculate mean and standard deviation
            # mean = train["total_amount"].mean()
            # std = train["total_amount"].std()

            # # Identify outliers using Z-scores threshold
            # outliers_mask = ((train["total_amount"] - mean).abs() / std) <= 3

            # # Filter out outliers
            # train_filtered = train[outliers_mask]
            train_filtered = train # comment this line jika jadi menggunakan outliers dan uncomment line code di atas

            # Create lag features
            train_filtered['lag_30'] = train_filtered["nett_profit_total"].shift(30)
            train_filtered.dropna(inplace=True)

            # Features and target
            X_train = train_filtered[['lag_30']]
            y_train = train_filtered["nett_profit_total"]

            # Initialize Ridge with a pipeline
            pipeline = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
            pipeline.fit(X_train, y_train)

            future_df = pd.DataFrame({'date_trx_days': future_dates})
            future_df['lag_30'] = 0

            # Iteratively update the lag feature
            last_total_amount = train_filtered["nett_profit_total"].iloc[-30:]
            for i in range(len(future_df)):
                if i < 30:
                    future_df.at[i, 'lag_30'] = last_total_amount.values[i]
                else:
                    future_df.at[i, 'lag_30'] = y_pred_projection[i - 30]

                X_future = future_df[['lag_30']].iloc[:i+1]
                y_pred_projection = pipeline.predict(X_future)
            y_pred_1 = pd.DataFrame({
                'date_trx_days': future_dates,
                'nett_profit_projection': y_pred_projection
            })

            # Create future dataframe to predict until two month ahead
            p = 5
            d = 1
            q = 0

            model_2 = ARIMA(train_filtered["nett_profit_total"], order = (p, d, q))

            model_2_fit = model_2.fit()

            # Forecast on the future set
            forecast_output = model_2_fit.forecast(steps = 93)
            y_pred_2 = pd.DataFrame({
                'date_trx_days': future_dates,
                'nett_profit_forecasting': forecast_output
            })
            
            train_filtered = train_filtered.reset_index()
            train_filtered['date_trx_days'] = pd.to_datetime(train_filtered['date_trx_days'])
            y_pred_1['date_trx_days'] = pd.to_datetime(y_pred_1['date_trx_days'])
            y_pred_2['date_trx_days'] = pd.to_datetime(y_pred_2['date_trx_days'])
            # Concatenate the training and prediction data
            combined_df_1 = pd.merge(train_filtered, y_pred_1, how = "outer", on = "date_trx_days")
            combined_df = pd.merge(combined_df_1, y_pred_2, how = "outer", on = "date_trx_days")

            combined_df = combined_df.rename(columns = {
                'date_trx_days': 'transaction_date',
                'nett_profit_total': 'laba',
                'nett_profit_projection': 'laba_projection',
                'nett_profit_forecasting': 'laba_forecasting'
            })

            df_final = combined_df[['laba', 'laba_projection', 'laba_forecasting', 'transaction_date']]

            return df_final
        
        except Exception as e:
            raise e
    
    @staticmethod
    def ActivationSyncChecker(store_id: int):
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = """
        SELECT flag_first_sync_datainsight
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = %s
        """
        params = (store_id,)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil satu baris hasil query
                return data
        except Exception as e:
            print(f"Error checking activation: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def InsertProfitProjectionForecasting(df_final_to_list):
        current_date = datetime.now()
        now_wib_h_min_1 = current_date + timedelta(hours = 7) - timedelta(days = 1)
        fnow_wib_h_min_1 = now_wib_h_min_1.strftime('%Y-%m-%d')
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_profit_projection_forecasting" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_profit_projection_forecasting"
        delete_query = f"DELETE FROM {table_name} WHERE store_id = %s"
        insert_query = f"""
        INSERT INTO {table_name} (
            store_id,
            laba,
            laba_projection,
            laba_forecasting,
            transaction_date
        ) VALUES (
        %s, %s, %s, %s, %s
        )
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam salesOrder
                store_ids = list(set(order[0] for order in df_final_to_list))
                
                # Delete all existing records for each store_id
                for store_id in store_ids:
                    cursor.execute(delete_query, (store_id,))
                
                # Insert all new records
                cursor.executemany(insert_query, df_final_to_list)
                
                # Commit perubahan ke database
                conn.commit()
                
                # Call the function to update last sync after processing all rows
                classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                classProjectionForecastingCheckService.UpdateLastSyncForProfitProjectionForecasting(fnow_wib_h_min_1, store_id)

        except Exception as e:
            conn.rollback()
            print(f"Error inserting profit projection forecasting: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
    
    # def UpdateProfitDaily(df_final_to_list):
    #     conn_generator = get_db_connection_third()
    #     conn = next(conn_generator)
    #     table_name = "staging_olsera_datainsight_profit_projection_forecasting" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_profit_projection_forecasting"
    #     query = f"""
    #     UPDATE {table_name}
    #     SET laba = %s
    #     WHERE store_id = %s
    #     AND transaction_date = %s
    #     """
    #     try:
    #         # Memulai transaksi
    #         # conn.begin()
    #         with conn.cursor() as cursor:
    #             # Looping untuk setiap baris data dalam salesOrder
    #             for order in df_final_to_list:
    #                 cursor.execute(query, (
    #                     order[1], # laba
    #                     order[0], # store_id
    #                     order[2] # transaction_date
    #                 ))
                
    #             # Commit perubahan ke database
    #             conn.commit()
    #             print(f"Data successfully updated into {table_name}.")
    #     except Exception as e:
    #         conn.rollback()
    #         print(f"Error updating profit projection forecasting: {e}")
    #         return e
    #     finally:
    #         # Tutup koneksi
    #         if conn and conn.is_connected():
    #             conn.close()

    @staticmethod
    def UpdateProfitDaily(df_final_to_list, store_id, fstart_date_update):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_profit_projection_forecasting" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_profit_projection_forecasting"
        
        # insert_query = f"""
        # INSERT INTO {table_name} (store_id, laba, transaction_date)
        # VALUES (%s, %s, %s)
        # """
        
        update_query = f"""
        UPDATE {table_name}
        SET laba = %s
        WHERE store_id = %s AND transaction_date = %s
        """

        check_existing_query = f"""
        SELECT id
        FROM {table_name}
        WHERE store_id = %s AND transaction_date = %s
        """

        flast_non_zero_profit_date = None

        try:
            with conn.cursor() as cursor:
                for row in df_final_to_list:
                    store_id_value = row[0] # store_id from the current row
                    laba_value = row[1] # laba from the current row
                    transaction_date = row[2]  # transaction_date from the current row
                    ftransaction_date = transaction_date.strftime('%Y-%m-%d')

                    # Update last_non_zero_profit_date if profit is non-zero
                    if laba_value != 0:
                        flast_non_zero_profit_date = ftransaction_date
                    
                    # Check if the row exists
                    cursor.execute(check_existing_query, (store_id_value, ftransaction_date))
                    result = cursor.fetchone()

                    if result:
                        # If the row exists, perform an update
                        cursor.execute(update_query, (laba_value, store_id_value, ftransaction_date))
                    # else:
                    #     # If the row doesn't exist, perform an insert
                    #     cursor.execute(insert_query, (store_id_value, laba_value, ftransaction_date))

                # Commit changes to the database
                conn.commit()

                # Call the function to update last sync after processing all rows
                classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                if flast_non_zero_profit_date and flast_non_zero_profit_date != fstart_date_update:
                    classProjectionForecastingCheckService.UpdateLastSyncForProfitProjectionForecasting(
                        flast_non_zero_profit_date, store_id
                    )
                else:
                    print(f"No non-zero profit found for store_id {store_id}")
                    return True

        except Exception as e:
            conn.rollback()
            print(f"Error updating profit projection forecasting for store_id {store_id}: {e}")
            return e
        finally:
            # Close the connection
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def StoringProfitToDBAnalytic(payload = None):
        try:
            print("storing profit is running")
            if payload is not None:
                store_id = payload.store_id
                # Cek apakah store_id adalah array (list)
                if isinstance(store_id, list):
                    # Jika store_id adalah array, gunakan langsung sebagai dataStore
                    dataStore = [store_id]
                else:
                    # Jika store_id bukan array, ubah ke format yang diinginkan
                    dataStore = [[store_id]]
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()
                
            classSalesRecapService = SalesRecapService
            for row in dataStore:
                store_id = row[0]
                current_date = datetime.now()
                now_wib = current_date + timedelta(hours=7)
                
                this_month_beginning = now_wib.replace(day = 1)
                future_dates = pd.date_range(start=this_month_beginning, periods=93, freq='D').date

                data_for_check_activation = StoreExtSettingsModel.StoreExtSettings.checkActivation(store_id)
                check_activation_date = data_for_check_activation[2] # activation_date ada di kolom ketiga hasil fetch SQL
                test_1_boolean = (check_activation_date != now_wib.date())
                if test_1_boolean:
                # data_for_flag_sync = SalesRecapService.ActivationSyncChecker(store_id)
                # flag_sync = data_for_flag_sync[0][0]
                # flag_sync = 1 # comment line ini dan uncomment line diatas jika sudah ada data flag nya
                # if flag_sync == 1:
                    today_date = now_wib.day
                    test_boolean = (today_date == 28)
                    # if today_date == 29: # customize disini untuk staging dan prod sesuai tanggal hari ini, di cron default today_date == 2
                    if test_boolean:
                        start_date_training = (now_wib - timedelta(days=180)).date()
                        end_date_training = now_wib.date()
                        start_date_training = pd.to_datetime(start_date_training)
                        end_date_training = pd.to_datetime(end_date_training)
                        training_date_range = pd.date_range(start = start_date_training, end = end_date_training)
                        df_complete_from_nett_profit_1 = classSalesRecapService.NettProfit1(store_id, training_date_range, start_date_training, end_date_training)
                        df_complete_from_nett_profit_2 = classSalesRecapService.NettProfit2(df_complete_from_nett_profit_1, store_id, training_date_range, start_date_training, end_date_training)
                        df_complete = classSalesRecapService.NettProfit3(df_complete_from_nett_profit_2, store_id, training_date_range, start_date_training, end_date_training)
                        df_final = classSalesRecapService.TrainMachineLearningLaba(df_complete, future_dates)
                        df_final['store_id'] = store_id
                        df_final = df_final[['store_id', 'laba', 'laba_projection', 'laba_forecasting', 'transaction_date']]
                        df_final = df_final.fillna(0)
                        df_final_to_list = list(df_final.itertuples(index = False, name = None))
                        classSalesRecapService.InsertProfitProjectionForecasting(df_final_to_list)
                    else:
                        try:
                            classProjectionForecastingCheckService = ProjectionForecastingCheckService.ProjectionForecastingCheckService
                            fetch_start_date_update = classProjectionForecastingCheckService.GetSyncedStoreProfitProjectionForecasting(store_id)
                            start_date_update = fetch_start_date_update[1]
                            fstart_date_update = start_date_update.strftime("%Y-%m-%d")
                            end_date_update = now_wib.date()
                            start_date_update = pd.to_datetime(start_date_update)
                            end_date_update = pd.to_datetime(end_date_update)
                            update_date_range = pd.date_range(start = start_date_update, end = end_date_update)
                            df_complete_from_nett_profit_1 = classSalesRecapService.NettProfit1(store_id, update_date_range, start_date_update, end_date_update)
                            df_complete_from_nett_profit_2 = classSalesRecapService.NettProfit2(df_complete_from_nett_profit_1, store_id, update_date_range, start_date_update, end_date_update)
                            df_complete = classSalesRecapService.NettProfit3(df_complete_from_nett_profit_2, store_id, update_date_range, start_date_update, end_date_update)
                            df_final = df_complete
                            df_final = df_final.rename(columns = {
                                'date_trx_days': 'transaction_date',
                                'nett_profit_total': 'laba'
                            })
                            # now_wib_date_to_update = now_wib.strftime("%Y-%m-%d")
                            # df_final = df_final.loc[df_final['transaction_date'] == now_wib_date_to_update, :]
                            if not df_final.empty:
                                df_final['store_id'] = store_id
                                df_final = df_final[['store_id', 'laba', 'transaction_date']]
                                df_final_to_list = list(df_final.itertuples(index = False, name = None))
                                classSalesRecapService.UpdateProfitDaily(df_final_to_list, store_id, fstart_date_update)
                        except Exception as update_error:
                            print(f"Error processing update for store_id {store_id}: {update_error}")
                            continue
                else:
                    today_date = now_wib.day
                    start_date_training = (now_wib - timedelta(days=180)).date()
                    end_date_training = now_wib.date()
                    start_date_training = pd.to_datetime(start_date_training)
                    end_date_training = pd.to_datetime(end_date_training)
                    training_date_range = pd.date_range(start = start_date_training, end = end_date_training)
                    df_complete_from_nett_profit_1 = classSalesRecapService.NettProfit1(store_id, training_date_range, start_date_training, end_date_training)
                    df_complete_from_nett_profit_2 = classSalesRecapService.NettProfit2(df_complete_from_nett_profit_1, store_id, training_date_range, start_date_training, end_date_training)
                    df_complete = classSalesRecapService.NettProfit3(df_complete_from_nett_profit_2, store_id, training_date_range, start_date_training, end_date_training)
                    df_final = classSalesRecapService.TrainMachineLearningLaba(df_complete, future_dates)
                    df_final['store_id'] = store_id
                    df_final = df_final[['store_id', 'laba', 'laba_projection', 'laba_forecasting', 'transaction_date']]
                    df_final = df_final.fillna(0)
                    df_final_to_list = list(df_final.itertuples(index = False, name = None))
                    classSalesRecapService.InsertProfitProjectionForecasting(df_final_to_list)
            print("storing profit is finished")
        except Exception as error:
            raise error