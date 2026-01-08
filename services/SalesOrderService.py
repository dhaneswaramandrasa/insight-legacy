from config.database import get_db_connection
import os
from models import StoreModel, SalesOrderModel, PurchaseOrderModel, InexTransModel, StockInOutItemsModel, StockOpnameItemsModel
# from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from config.bugsnag import get_logger
import os
from dotenv import load_dotenv
# from services import SalesRecapService
from datetime import datetime, timedelta
import json

load_dotenv('.env')
logger = get_logger()
conn_generator = get_db_connection()
conn = next(conn_generator)

# from olsera_db_connection import get_connection
# conn = get_connection()

class SalesOrderService():
    directory = "data-lake-python-feather"
        
    @staticmethod
    def CreateFileFeatherOrder():
        try:
            log_file = "sales-order-log.json"
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classSalesOrder = SalesOrderModel.SalesOrder
            classPurchaseOrder = PurchaseOrderModel.PurchaseOrder
            classInexTrans = InexTransModel.InexTrans
            classStockInoutItems = StockInOutItemsModel.StockInOutItems
            classStockOpnameItems = StockOpnameItemsModel.StockOpnameItems
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')

            # Tanggal hari ini
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
         
            for row in dataStore:
                store_id = row[0]

                # Cek apakah store_id ada di log untuk hari ini
                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                   salesOrder = classSalesOrder.GetDataSalesOrder(store_id, date)
                   WrapperCreateFile(salesOrder, f"{store_id}_sales_order_6_months_ago.feather", SetColumnForFeatherFile(0), aws_bucket, f"{SalesOrderService.directory}/sales-order")
                   log_data[store_id] = today
                   write_log(log_data, log_file)

                # # # #salesorderitem
                salesOrderItem = classSalesOrder.GetDataSalesOrderItem(store_id,date)
                WrapperCreateFile(salesOrderItem,f"{store_id}_sales_order_item_6_months_ago.feather",SetColumnForFeatherFile(1),aws_bucket, f"{SalesOrderService.directory}/sales-order-item")
                # # # # endsalesorderitem

                # # # #purchaseorder
                purchaseOrder = classPurchaseOrder.GetDataPurchaseOrder(store_id,date)
                WrapperCreateFile(purchaseOrder, f"{store_id}_purchase_order_6_months_ago.feather",SetColumnForFeatherFile(4),aws_bucket, f"{SalesOrderService.directory}/purchase-order")

                # # # #inextrans
                inexTrans = classInexTrans.GetDataInexTrans(store_id, date)
                WrapperCreateFile(inexTrans, f"{store_id}_inex_trans_6_months_ago.feather",SetColumnForFeatherFile(5),aws_bucket, f"{SalesOrderService.directory}/inex-trans")

                # # #StockInOutItems
                stockInOutItems = classStockInoutItems.GetDataStockInOutItems(store_id, date)
                WrapperCreateFile(stockInOutItems, f"{store_id}_stock_in_out_items_6_months_ago.feather",SetColumnForFeatherFile(7),aws_bucket, f"{SalesOrderService.directory}/stock-opname-items")

                # # #stockOpnameItems
                stockOpnameItems = classStockOpnameItems.GetDataStockOpnameItems(store_id, date)
                WrapperCreateFile(stockOpnameItems, f"{store_id}_stock_opname_items_6_months_ago.feather",SetColumnForFeatherFile(8),aws_bucket, f"{SalesOrderService.directory}/stock-in-out-items")
        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFileSalesOrderItem():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classSalesOrder = SalesOrderModel.SalesOrder
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')
            log_file = "sales-order-item-log.json"
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
            
            for row in dataStore:
                store_id = row[0]

                 # Cek apakah store_id ada di log untuk hari ini
                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                    salesOrderItem = classSalesOrder.GetDataSalesOrderItem(store_id,date)
                    WrapperCreateFile(salesOrderItem,f"{store_id}_sales_order_item_6_months_ago.feather",SetColumnForFeatherFile(1),aws_bucket, f"{SalesOrderService.directory}/sales-order-item")
                    log_data[store_id] = today
                    write_log(log_data, log_file)
                # # endsalesorderitem
        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFilePurchaseOrder():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classPurchaseOrder = PurchaseOrderModel.PurchaseOrder
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')

            log_file = "purchase-order-log.json"
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
            
            for row in dataStore:
                store_id = row[0]
                # #purchaseorder
                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                    purchaseOrder = classPurchaseOrder.GetDataPurchaseOrder(store_id,date)
                    WrapperCreateFile(purchaseOrder, f"{store_id}_purchase_order_6_months_ago.feather",SetColumnForFeatherFile(4),aws_bucket, f"{SalesOrderService.directory}/purchase-order")
                    log_data[store_id] = today
                    write_log(log_data, log_file)

        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFileInexTrans():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classInexTrans = InexTransModel.InexTrans
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')

            log_file = "inex-trans-log.json"
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
            
            for row in dataStore:
                store_id = row[0]
                #inextrans

                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                    inexTrans = classInexTrans.GetDataInexTrans(store_id, date)
                    WrapperCreateFile(inexTrans, f"{store_id}_inex_trans_6_months_ago.feather",SetColumnForFeatherFile(5),aws_bucket, f"{SalesOrderService.directory}/inex-trans")
                    log_data[store_id] = today
                    write_log(log_data, log_file)
        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFileInoutItems():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classStockInoutItems = StockInOutItemsModel.StockInOutItems
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')

            log_file = "inout-items-log.json"
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
            
            for row in dataStore:
                store_id = row[0]
                #StockInOutItems

                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                    stockInOutItems = classStockInoutItems.GetDataStockInOutItems(store_id, date)
                    WrapperCreateFile(stockInOutItems, f"{store_id}_stock_in_out_items_6_months_ago.feather",SetColumnForFeatherFile(7),aws_bucket, f"{SalesOrderService.directory}/stock-opname-items")
                    log_data[store_id] = today
                    write_log(log_data, log_file)
        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFileOpnameItems():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classStockOpnameItems = StockOpnameItemsModel.StockOpnameItems
            aws_bucket= os.getenv('AWS_BUCKET')
    
            # Hitung tanggal 6 bulan yang lalu
            dateAgo = datetime.now() - timedelta(days=180) #set six month ago
            date = dateAgo.strftime('%Y-%m-%d')

            log_file = "opname-items-log.json"
            today = datetime.now().strftime('%Y-%m-%d')
            # Baca log yang sudah ada
            log_data = read_log(log_file)
            
            for row in dataStore:
                store_id = row[0]
                #stockOpnameItems

                store_id_str = str(store_id)
                if log_data.get(store_id_str) == today:
                    print(f"Store {store_id} already processed today.")
                else:
                    stockOpnameItems = classStockOpnameItems.GetDataStockOpnameItems(store_id, date)
                    WrapperCreateFile(stockOpnameItems, f"{store_id}_stock_opname_items_6_months_ago.feather",SetColumnForFeatherFile(8),aws_bucket, f"{SalesOrderService.directory}/stock-in-out-items")
                    log_data[store_id] = today
                    write_log(log_data, log_file)
        except Exception as error:
            raise error


