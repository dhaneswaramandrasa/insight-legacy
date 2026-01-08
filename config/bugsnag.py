import bugsnag
from dotenv import load_dotenv
import os
import logging
load_dotenv('.env')

def configure_bugsnag():
    bugsnag.configure(
        api_key=os.getenv('BUGSNAG_KEY'),
        release_stage=os.getenv('RELEASE_STAGE'),
        # Optional: you can add more configuration options here
    )
    handler = bugsnag.handlers.BugsnagHandler()
    handler.setLevel(logging.ERROR)
    logging.getLogger("bugsnag").addHandler(handler)

def get_bugsnag_client():
    return bugsnag

def get_logger():
    logger = logging.getLogger("bugsnag")
    logger.setLevel(logging.ERROR)
    return logger
