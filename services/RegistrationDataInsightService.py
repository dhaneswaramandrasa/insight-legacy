# services/RegistrationDataInsightService.py
from models import SalesOrderModel, StoreExtSettingsModel, PurchaseOrderModel, InexTransModel,StockInOutItemsModel, StockOpnameItemsModel
from datetime import datetime, timedelta
from utilities.utils import *

logger = LogginWithTimeZoneJakarta('registration-datainsight.log', 'registration_logger')

def Submit(store_id):
    try:
        logger.info(f"Submit task started for store_id: {store_id}")
    
        activation_date = datetime.now().strftime('%Y-%m-%d')
        # Mengambil tanggal hari ini
        today = datetime.today()
        # Menghitung tanggal 180 hari yang lalu
        last_date_sync = today - timedelta(days=180)

        # Format tanggal ke y-m-d
        # formatted_today = today.strftime('%Y-%m-%d')
        formatted_last_date_sync = last_date_sync.strftime('%Y-%m-%d')
        classStoreExtSettings = StoreExtSettingsModel.StoreExtSettings
        
        classStoreExtSettings.activation(store_id, activation_date, formatted_last_date_sync) #activation data insight
        SyncDataInsight(store_id, formatted_last_date_sync)

        logger.info(f"Submit task completed for store_id: {store_id}")
        return True
    except Exception as e:
        print(e)


def SyncSalesOrder(store_id: int, date):
    classSalesOrder = SalesOrderModel.SalesOrder
    salesOrder = classSalesOrder.GetDataSalesOrder(store_id, date)
    if not salesOrder:
        print("data salesOrder not found")
        return
    # Insert data to olsera_datainsight_sales_order
    classSalesOrder.InsertDataSalesOrder(salesOrder)

def SyncSalesOrderItem(store_id, date):
    classSalesOrderItem = SalesOrderModel.SalesOrder
    salesOrderItem = classSalesOrderItem.GetDataSalesOrderItem(store_id, date)
    if not salesOrderItem:
        print("Data salesOrderItem not found")
        return
    classSalesOrderItem.InsertDataSalesOrderItem(salesOrderItem)

def SyncPurchaseOrder(store_id,date):
    classPurchaseOrder = PurchaseOrderModel.PurchaseOrder
    purchaseOrder = classPurchaseOrder.GetDataPurchaseOrder(store_id, date)
    if not purchaseOrder:
        print("data purchaseOrder not found")
        return
    classPurchaseOrder.InsertDataPurchaseOrder(purchaseOrder)

def SyncInexTrans(store_id,date):
    classInexTrans = InexTransModel.InexTrans
    inexTrans = classInexTrans.GetDataInexTrans(store_id, date)
    if not inexTrans:
        print("data inexTrans not found")
        return
    classInexTrans.InsertDataInexTrans(inexTrans)

def SyncStockInOutItems(store_id, date):
    classStockInOutItems = StockInOutItemsModel.StockInOutItems
    stockInOutItems = classStockInOutItems.GetDataStockInOutItems(store_id, date)
    if not stockInOutItems:
        print("data stockInOutItems not found")
        return
    classStockInOutItems.InsertDataStockInOutItems(stockInOutItems)

def SycnStockOpnameItems(store_id, date):
    classStockOpnameItems = StockOpnameItemsModel.StockOpnameItems
    stockOpnameItems = classStockOpnameItems.GetDataStockOpnameItems(store_id, date)
    if not stockOpnameItems:
        print("data stockOpnameItems not found")
        return
    classStockOpnameItems.InsertDataStockOpnameItems(stockOpnameItems)

def SyncPerDayDataInsight():
    classStoreDataInsight = StoreExtSettingsModel.StoreExtSettings
    getStore = classStoreDataInsight.GetStoreDataInsight()
    if not getStore:
        print("store not found")
        return
    timezone = pytz.timezone('Asia/Jakarta')
    currentDate = datetime.now(timezone)
    previousDate = currentDate - timedelta(days=1)
    fPreviousDate = previousDate.strftime('%Y-%m-%d')
    # print(fPreviousDate)
    for row in getStore:
        store_id = row[0]
        SyncDataInsight(store_id,fPreviousDate)
        classStoreDataInsight.UpdateLastSync(fPreviousDate, store_id)
    
def SyncDataInsight(store_id, last_date_sync):
    SyncSalesOrder(store_id,last_date_sync) #sync/download data sales order ke db analytics
    SyncSalesOrderItem(store_id, last_date_sync) #sync data sales order item
    SyncPurchaseOrder(store_id, last_date_sync) #sync purchase order
    SyncInexTrans(store_id,last_date_sync)
    SyncStockInOutItems(store_id, last_date_sync)
    SycnStockOpnameItems(store_id, last_date_sync)

def SyncDataSalesOrder():
    classStoreDataInsight = StoreExtSettingsModel.StoreExtSettings
    getStore = classStoreDataInsight.GetStoreDataInsight()
    if not getStore:
        print("store not found")
        return
    timezone = pytz.timezone('Asia/Jakarta')
    currentDate = datetime.now(timezone)
    previousDate = currentDate - timedelta(days=10)
    fPreviousDate = previousDate.strftime('%Y-%m-%d')
    # print(fPreviousDate)
    for row in getStore:
        store_id = row[0]
        SyncSalesOrder(store_id,fPreviousDate) #sync/download data sales order ke db analytics
        # classStoreDataInsight.UpdateLastSync(fPreviousDate, store_id)