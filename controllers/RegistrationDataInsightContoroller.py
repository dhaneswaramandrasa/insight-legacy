from fastapi import BackgroundTasks
from models import StoreExtSettingsModel
from fastapi.responses import JSONResponse
from services.RegistrationDataInsightService import *
from schemas.RegistrationRequest import RegistrationRequest, RegistrationRequestList
from resources.RegistrationDataInsightResource import RegistrationDataInsightResource
from services.SalesRecapService import SalesRecapService
import asyncio
class RegistrationDataInsight:

    @staticmethod
    async def submit(request: RegistrationRequest, background_tasks: BackgroundTasks):
        store_id = request.store_id
        classStoreExtSettings = StoreExtSettingsModel.StoreExtSettings
        result = classStoreExtSettings.checkActivation(store_id)
        if result[1] == 0:
            # Pindahkan submit(body) ke background task
            background_tasks.add_task(Submit, store_id)
            background_tasks.add_task(SalesRecapService.GetProductVision, request) #file s3 product populer
            background_tasks.add_task(SalesRecapService.GetAllSummarySales, request) # file s3 summary sales vision
            background_tasks.add_task(SalesRecapService.GetAllSummarySalesProduct, request) # file s3 summary sales vision
            background_tasks.add_task(SalesRecapService.GetSalesDataVision, request)

            background_tasks.add_task(RegistrationDataInsight.delayed_task, SalesRecapService.StoringSalesToDBAnalytic, request, delay=120)
            background_tasks.add_task(RegistrationDataInsight.delayed_task, SalesRecapService.StoringProfitToDBAnalytic, request, delay=150)
        response = RegistrationDataInsightResource.response(result)
        return response
    
    @staticmethod
    async def createdfiles3(request: RegistrationRequestList, background_tasks: BackgroundTasks):
        # store_id = request.store_id
        # classStoreExtSettings = StoreExtSettingsModel.StoreExtSettings
        # result = classStoreExtSettings.checkActivation(store_id)
        # if result[1] == 0:
        #     print(result[1])
        # Pindahkan submit(body) ke background task
        # background_tasks.add_task(Submit, store_id)
        background_tasks.add_task(SalesRecapService.GetProductVision, request) #file s3 product populer
        background_tasks.add_task(SalesRecapService.GetAllSummarySales, request) # file s3 summary sales vision
        background_tasks.add_task(SalesRecapService.GetAllSummarySalesProduct, request) # file s3 summary sales vision
        background_tasks.add_task(SalesRecapService.GetSalesDataVision, request)

        background_tasks.add_task(RegistrationDataInsight.delayed_task, SalesRecapService.StoringSalesToDBAnalytic, request, delay=120)
        background_tasks.add_task(RegistrationDataInsight.delayed_task, SalesRecapService.StoringProfitToDBAnalytic, request, delay=150)
        # response = RegistrationDataInsightResource.response(result)
        # return response
        return True
    
    
    @staticmethod
    async def delayed_task(func, *args, delay: int = 0):
        await asyncio.sleep(delay)
        func(*args)