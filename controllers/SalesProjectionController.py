# controllers/SalesProjectionController.py
from schemas.RegistrationRequest import RegistrationRequest
from schemas.StoringRequest import StoringRequest
from services.SalesRecapService import SalesRecapService
from services.ProductTrendService import ProductTrendService
from fastapi import BackgroundTasks

class SalesProjection:
    @staticmethod
    async def sales(request: RegistrationRequest, background_tasks: BackgroundTasks):
        # background_tasks.add_task(SalesRecapService.StoringSalesToDBAnalytic, request)
        background_tasks.add_task(SalesRecapService.GetProductVision, request) #file s3 product populer
        background_tasks.add_task(SalesRecapService.GetAllSummarySales, request) # file s3 summary sales vision
        background_tasks.add_task(SalesRecapService.GetAllSummarySalesProduct, request) # file s3 summary sales vision
        background_tasks.add_task(SalesRecapService.GetSalesDataVision, request)
        # background_tasks.add_task(SalesRecapService.StoringProfitToDBAnalytic, request)
        # background_tasks.add_task(ProductTrendService.ProductTrendSummary, request)
         # Contoh hardcode JSON response
        response_data = {
            "status": "success",
            "message": "sync data has been processed.",
        }
        return response_data  # Mengemba
        # return result
    
    @staticmethod
    async def sales_profit_product_trend(request: StoringRequest, background_tasks: BackgroundTasks):
        flag = request.flag
        if flag == 1:
            background_tasks.add_task(SalesRecapService.StoringSalesToDBAnalytic, request)
            response_data = {
            "status": "success",
            "message": "Sales data processed successfully.",
            }
        elif flag == 2:
            background_tasks.add_task(SalesRecapService.StoringProfitToDBAnalytic, request)
            response_data = {
            "status": "success",
            "message": "Profit data processed successfully.",
            }
        else:
            background_tasks.add_task(ProductTrendService.ProductTrendSummary, request)
            response_data = {
            "status": "success",
            "message": "Product trend data processed successfully.",
            }
        
        return response_data  # Mengemba
