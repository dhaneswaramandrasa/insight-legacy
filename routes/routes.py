#routes/routes.py

from fastapi import APIRouter, Request, HTTPException, BackgroundTasks,Body
from controllers.VisionController import VisionController
from controllers.S3StorageAwsController import S3StorageAwsController
# from config.database import get_db_connection
from schemas.schemas import ModulChoices
from schemas.RegistrationRequest import RegistrationRequest, RegistrationRequestList
from schemas.StoringRequest import StoringRequest
# from middleware.HMACMiddleware import HMACMiddleware
from utilities.jobs.SchedulerUtilities import stop_scheduler, start_scheduller
from controllers.simple_controller import send_message_to_sqs
from pydantic import BaseModel
from controllers.RegistrationDataInsightContoroller import RegistrationDataInsight
from controllers.SalesProjectionController import SalesProjection

router = APIRouter()
# @router.post("/api/user")
# async def post_employees(request: Request, connection = Depends(get_db_connection)):
#     return await UserController.GetUserData(request, connection)
@router.get("/health")
async def health_check():
    # Logic to check the health of the application
    # Here you can add more detailed checks if needed (e.g., database connection, external service availability, etc.)
    return {"status": "OK", "details": "Application is healthy"}
@router.get("/")
async def health_check():
    # Logic to check the health of the application
    # Here you can add more detailed checks if needed (e.g., database connection, external service availability, etc.)
    return {"status": "OK", "details": "Application is healthy"}

@router.post("/api/readfiles3")
async def post_readfiles3(request: Request):
    return await S3StorageAwsController.ReadFileFromS3(request)

@router.post('/api/createfile')
async def create_file(request: Request):
    return await S3StorageAwsController.CreateFile(request)

@router.get("/")
async def baseUrl():
    return {"message": "welcome to fastapi python"}

@router.get("/api/data-insight")
async def get_insight(store_id: int, start_date: str, end_date:str, modul: ModulChoices = None):
    return await VisionController.DataInsight(store_id, start_date, end_date, modul)

@router.post("/stop-scheduler")
async def restart_scheduler_endpoint(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(stop_scheduler)  # Memanggil fungsi untuk restart scheduler
        return {"message": "Scheduler stopped successfully"}
    except Exception as e:
        # Log error jika perlu
        return HTTPException(status_code=500, detail=f"Failed to restart scheduler: {e}")

@router.post("/start-scheduler")
async def restart_scheduler_endpoint(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(start_scheduller)  # Memanggil fungsi untuk restart scheduler
        return {"message": "Scheduler started successfully"}
    except Exception as e:
        # Log error jika perlu
        return HTTPException(status_code=500, detail=f"Failed to restart scheduler: {e}")
    
@router.post("/api/data-insight/registration")
async def submit(request: RegistrationRequest, background_tasks: BackgroundTasks):
    # result = await RegistrationDataInsight.submit(request)
    return await RegistrationDataInsight.submit(request, background_tasks)

@router.post("/api/data-insight/createdfiles3")
async def createdfiles3(request: RegistrationRequestList, background_tasks: BackgroundTasks):
    # result = await RegistrationDataInsight.submit(request)
    return await RegistrationDataInsight.createdfiles3(request, background_tasks)

@router.post("/api/data-insight/sales-projection")
async def submit_sales_projection(request: RegistrationRequest, background_tasks: BackgroundTasks):
    # result = await RegistrationDataInsight.submit(request)
    return await SalesProjection.sales(request, background_tasks)
    
@router.post("/api/data-insight/sales-profit-product-trend")
async def submit_profit_product_trend(request: StoringRequest, background_tasks: BackgroundTasks):
    return await SalesProjection.sales_profit_product_trend(request, background_tasks)

class Message(BaseModel):
    message: str

@router.post("/send-message/")
async def send_message(msg: Message):
    result = await send_message_to_sqs(msg.message)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result
