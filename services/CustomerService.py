from config.database import get_db_connection
from datetime import datetime
import pytz
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *

from models import CustomerModel,StoreExtSettingsModel 

def DataBuyer():
    try:
        directory = "data-lake-python-feather"
        aws_bucket= os.getenv('AWS_BUCKET')
        classStoreDataInsight = StoreExtSettingsModel.StoreExtSettings
        getStore = classStoreDataInsight.GetStoreDataInsight()
        classBuyer = CustomerModel.Customer

        if not getStore:
            print("store not found")
            return
        
        timezone = pytz.timezone('Asia/Jakarta')
        currentDate = datetime.now(timezone)
        currentYear = currentDate.strftime('%Y') 
        for row in getStore:
            store_id = row[0]
            sourceDataBuyer = classBuyer.GetListBuyer(store_id, currentYear)
            fileName = f"{store_id}_{currentYear}_listbuyer.feather"
            path = f"{directory}/buyer/{currentYear}"

            #check directory, if empty created dir, if not empty continue
            checkDir =  S3_utils.ensure_directory_exists(aws_bucket, path) 
           
            if checkDir:
                WrapperCreateFile(sourceDataBuyer, fileName, SetColumnForFeatherFile(14), aws_bucket, path)
    except Exception as error:
        raise error