from apscheduler.schedulers.background import BackgroundScheduler
from services.SalesRecapService import SalesRecapService
import logging
from apscheduler.triggers.cron import CronTrigger
import pytz
from datetime import datetime
import bugsnag

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
handler = logging.FileHandler('scheduler.log')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

def JobFunctionSalesVision():
    try:
        print("Job sales vision is running ...")
        salesRecapService = SalesRecapService
        salesRecapService.GetSalesDataVision()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSalesDepositVision():
    try:
        print("Job sales deposit vision is running ...")
        salesRecapService = SalesRecapService
        salesRecapService.GetDepositVision()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionSummarySalesVision():
    try:
        print("Job summary sales vision is running ...")
        salesRecapService = SalesRecapService
        salesRecapService.GetAllSummarySales()
    except Exception as e:
        bugsnag.notify(e)

def JobFunctionProductVision():
    try:
        print("job product vision is running ...")
        salesRecapService = SalesRecapService
        salesRecapService.GetProductVision()
    except Exception as e:
        # logging.error(f"Error in JobFunctionProductVision: {e}")
        bugsnag.notify(e)

def JobFunctionSummarySalesProductVision():
    try:
        print("job summary sales product vision is running ...")
        salesRecapService = SalesRecapService
        salesRecapService.GetAllSummarySalesProduct()
    except Exception as e:
        # logging.error(f"Error in JobFunctionSummarySalesProductVision: {e}")
        bugsnag.notify(e)
                          
# def JobFunctionCustomerVision():
#     try:
#         print("job customer vision is running ...")
#         salesRecapService = SalesRecapService
#         salesRecapService.GetCustomerVision()
#     except Exception as e:
#         logging.error(f"Error in JobFunctionCustomerVision: {e}")
#         bugsnag.notify(e)

scheduler = BackgroundScheduler()

def start_scheduler():
    # scheduler.add_job(JobFunctionSalesVision, 'interval', minutes=1) #setiap 1 menit
    scheduler.add_job(JobFunctionSalesDepositVision, CronTrigger(hour=1, timezone=jakarta_timezone)) #setiap 1 menit
    scheduler.add_job(JobFunctionSalesVision, CronTrigger(hour=1, minute=10, timezone=jakarta_timezone)) #dijalankan setiap jam 1 dinihari (01:00)
    scheduler.add_job(JobFunctionProductVision, CronTrigger(hour=1, minute=30, timezone=jakarta_timezone)) #dijalankan setiap jam 2 dinihari (02:00)
    # scheduler.add_job(JobFunctionCustomerVision, 'interval', seconds=10) #dijalankan setiap jam 1:30 dinihari (01:30)
    scheduler.add_job(JobFunctionSummarySalesVision,CronTrigger(hour=1, minute=50, timezone=jakarta_timezone)) #dijalankan setiap jam 1:30 dinihari (01:30)
    scheduler.add_job(JobFunctionSummarySalesProductVision, CronTrigger(hour=2, minute=20, timezone=jakarta_timezone))
    scheduler.start()

# Ekspor scheduler dari modul
__all__ = ['scheduler', 'start_scheduler']
