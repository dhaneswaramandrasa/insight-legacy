from fastapi import FastAPI, Request
import uvicorn
from routes.routes import router
from utilities.jobs.SalesVision import scheduler, start_scheduler
from utilities.jobs.SchedulerUtilities import start_scheduler_utilities
from starlette.middleware.gzip import GZipMiddleware
from middleware.HMACMiddleware import HMACMiddleware
from config.bugsnag import configure_bugsnag, get_bugsnag_client, get_logger
from middleware.BugsnagMiddleware import BugsnagMiddleware
# import logging

# Configure Bugsnag
configure_bugsnag()
bugsnag_client = get_bugsnag_client()

# Initialize logging
# logger = logging.getLogger("bugsnag")
logger = get_logger()  # Define global logger

app = FastAPI()

# Menambahkan middleware GZip dengan ukuran minimum 1000 byte untuk kompresi
app.add_middleware(GZipMiddleware, minimum_size=10000000)
# Add Bugsnag middleware
app.add_middleware(BugsnagMiddleware, bugsnag_client=bugsnag_client)

@app.on_event("startup")
async def startup_event():
    start_scheduler()
    start_scheduler_utilities()

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()  # Menghentikan scheduler saat aplikasi dihentikan

# Add HMAC middleware
app.add_middleware(HMACMiddleware)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
