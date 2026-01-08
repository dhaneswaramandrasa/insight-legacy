from apscheduler.schedulers.background import BackgroundScheduler
from services import ProductService, SalesOrderService, SalesRecapService, ProductRecommendationService, ProductTrendService, RegistrationDataInsightService, CustomerService, PromotionService
import logging
from apscheduler.triggers.cron import CronTrigger
import pytz
from datetime import datetime
import bugsnag
import os

# Inisialisasi timezone
jakarta_timezone = pytz.timezone('Asia/Jakarta')

# Formatter kustom untuk logging
class AsiaJakartaFormatter(logging.Formatter):
    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, jakarta_timezone)
        return dt.astimezone(jakarta_timezone)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec='milliseconds')
            except TypeError:
                s = dt.isoformat()
        return s

# Konfigurasi logging
formatter = AsiaJakartaFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler = logging.FileHandler('scheduler_utilities.log')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

# # Fungsi umum untuk menurunkan prioritas CPU dan menjalankan job
# def run_job(job_function):
#     os.nice(19)  # Menurunkan prioritas CPU
#     try:
#         job_function()
#     except Exception as e:
#         bugsnag.notify(e)

def JobFunctionProductFileFeather():
    try:
        print("JobFunctionProductFileFeather is running ...")
        # Gantikan dengan logika pekerjaan sebenarnya
        productService = ProductService.ProductService
        productService.CreateFileFeather()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionCategoryProductFileFeather():
    try:
        print("JobFunctionCategoryProductFileFeather is running ...")
        productService = ProductService.ProductService
        productService.CreateFileFeatherProductCategory()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionOrderFileFeather():
    try:
        print("JobFunctionSalesOrderFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFileFeatherOrder()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSalesOrderItemFileFeather():
    try:
        print("JobFunctionSalesOrderItemFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFileSalesOrderItem()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionPurchaseOrderFileFeather():
    try:
        print("JobFunctionPurchaseOrderFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFilePurchaseOrder()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionInexTransFileFeather():
    try:
        print("JobFunctionInexTransFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFileInexTrans()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionCreateFileInoutItemsFileFeather():
    try:
        print("JobFunctionCreateFileInoutItemsFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFileInoutItems()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionCreateFileOpnameItemsFileFeather():
    try:
        print("JobFunctionCreateFileInoutItemsFileFeather is running ...")
        salesOrder = SalesOrderService.SalesOrderService
        salesOrder.CreateFileOpnameItems()
    except Exception as e:
        bugsnag.notify(e)
        
        

def JobFunctionSalesProjectionForecastingFileFeather():
    try:
        print("JobFunctionSalesProjectionForecastingFileFeather is running ...")
        salesRecap = SalesRecapService.SalesRecapService
        salesRecap.CreateFileFeatherSalesProjectionForecasting()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionProfitProjectionForecastingFileFeather():
    try:
        print("JobFunctionProfitProjectionForecastingFileFeather is running ...")
        salesRecap = SalesRecapService.SalesRecapService
        salesRecap.CreateFileFeatherProfitProjectionForecasting()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionProductRecommendationFileFeather():
    try:
        print("JobFunctionProductRecommendationFileFeather is running ...")
        productRecommendationService = ProductRecommendationService.ProductRecommendationService
        productRecommendationService.CreateFileFeatherProductRecommendation()
    except Exception as e:
        bugsnag.notify(e)

# def JobFunctionProductTrendByDayFileFeather():
#     try:
#         print("JobFunctionProductTrendByDayFileFeather is running ...")
#         productTrendByDayService = ProductTrendService.ProductTrendService
#         productTrendByDayService.CreateFileFe atherProductTrendByDay()
#     except Exception as e:
#         bugsnag.notify(e)

# def JobFunctionProductTrendByHourFileFeather():
#     try:
#         print("JobFunctionProductTrendByHourFileFeather is running ...")
#         productTrendByHourService = ProductTrendService.ProductTrendService
#         productTrendByHourService.CreateFileFeatherProductTrendByHour()
#     except Exception as e:
#         bugsnag.notify(e)

def JobFunctionProductTrendSummaryJSON():
    try:
        print("JobFunctionProductTrendSummaryJSON is running ...")
        productTrendSummary = ProductTrendService.ProductTrendService
        productTrendSummary.ProductTrendSummary()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSyncPerDayDataInsight():
    try:
        print("JobFunctionSyncPerDayDataInsight is running ...")
        RegistrationDataInsightService.SyncPerDayDataInsight()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSyncDataSalesOrder():
    try:
        print("JobFunctionSyncPerDayDataInsight is running ...")
        RegistrationDataInsightService.SyncDataSalesOrder()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSalesProjectionForecastingDBAnalytic(): # Warning: Perhatikan conditional date nya!
    try:
        print("JobFunctionSalesProjectionForecastingFileFeather is running ...")
        salesRecap = SalesRecapService.SalesRecapService
        salesRecap.StoringSalesToDBAnalytic()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionProfitProjectionForecastingDBAnalytic(): # Warning: Perhatikan conditional date nya!
    try:
        print("JobFunctionProfitProjectionForecastingDBAnalytic is running ...")
        salesRecap = SalesRecapService.SalesRecapService
        salesRecap.StoringProfitToDBAnalytic()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionListBuyer():
    try:
        print("JobFunctionListBuyer")
        CustomerService.DataBuyer()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionListOrderPromo():
    try:
        print("JobFunctionListOrderPromo")
        PromotionService.DataPromotion()
    except Exception as e:
        bugsnag.notify(e)



scheduler_utilities = BackgroundScheduler(daemon=False)

def start_scheduler_utilities():
    # scheduler_utilities.add_job(JobFunctionOrderFileFeather, CronTrigger(hour=18, minute=20, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionSalesOrderItemFileFeather, CronTrigger(hour=18, minute=25, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionPurchaseOrderFileFeather, CronTrigger(hour=22, minute=0, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionInexTransFileFeather, CronTrigger(hour=23, minute=0, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionCreateFileInoutItemsFileFeather, CronTrigger(hour=23, minute=30, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionCreateFileOpnameItemsFileFeather, CronTrigger(hour=23, minute=50, timezone=jakarta_timezone))
    
    # scheduler_utilities.add_job(JobFunctionProductFileFeather, CronTrigger(hour=1, minute=0, timezone=jakarta_timezone)) # Sesuaikan penjadwalan
    # scheduler_utilities.add_job(JobFunctionCategoryProductFileFeather, CronTrigger(hour=1, minute=30, timezone=jakarta_timezone)) 
    # scheduler_utilities.add_job(JobFunctionSalesProjectionForecastingFileFeather, CronTrigger(hour=1, minute=50, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionProfitProjectionForecastingFileFeather, CronTrigger(hour=13, minute=42, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionProductRecommendationFileFeather, CronTrigger(hour=2, minute=30, timezone=jakarta_timezone))
    # customize these scheduler interval:
    # scheduler_utilities.add_job(JobFunctionProductTrendByDayFileFeather, 'interval', minutes=20)
    # scheduler_utilities.add_job(JobFunctionProductTrendByHourFileFeather,'interval', minutes=25)
    scheduler_utilities.add_job(JobFunctionProductTrendSummaryJSON, CronTrigger(hour=2, timezone=jakarta_timezone))
    scheduler_utilities.add_job(JobFunctionSyncPerDayDataInsight, CronTrigger(hour=2, timezone=jakarta_timezone))
    scheduler_utilities.add_job(JobFunctionSyncDataSalesOrder, CronTrigger(hour=1, timezone=jakarta_timezone))
    
    scheduler_utilities.add_job(JobFunctionSalesProjectionForecastingDBAnalytic,CronTrigger(hour=3, minute=30, timezone=jakarta_timezone))
    scheduler_utilities.add_job(JobFunctionProfitProjectionForecastingDBAnalytic, CronTrigger(hour=4, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionListBuyer,CronTrigger(hour=23, minute=50, timezone=jakarta_timezone))
    # scheduler_utilities.add_job(JobFunctionListOrderPromo,CronTrigger(hour=2, minute=40, timezone=jakarta_timezone))
    scheduler_utilities.start()


def stop_scheduler():
    if scheduler_utilities.running:
        scheduler_utilities.shutdown(wait=False)

def start_scheduller():
    start_scheduler_utilities()

# Ekspor scheduler dari modul
__all__ = ['scheduler_utilities', 'start_scheduler_utilities']
