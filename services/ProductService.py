from config.database import get_db_connection
from models import ProductCategoryModel, ProductModel, ProductVariantMaterialsModel, StoreModel, ProductMaterialsModel
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from config.bugsnag import get_logger
import os
from dotenv import load_dotenv
from services import SalesRecapService

load_dotenv('.env')
logger = get_logger()
conn_generator = get_db_connection()
conn = next(conn_generator)

# from olsera_db_connection import get_connection
# conn = get_connection()

class ProductService():
    directory = "data-lake-python-feather"

    @staticmethod
    def CreateFileFeather():
        try:
            #store
            store = StoreModel.Store
            dataStore = store.GetIndexData()

            #product
            classProduct = ProductModel.Product

            s3Utils = S3_utils
            aws_bucket= os.getenv('AWS_BUCKET')
            pathProductMaster = f"{ProductService.directory}/product-master"
            pathProductVariant = f"{ProductService.directory}/product-variant"
            pathProductMaterials = f"{ProductService.directory}/product-materials"
            pathProductVariantMaterials = f"{ProductService.directory}/product-variant-materials"
            salesRecap = SalesRecapService.SalesRecapService
            classProductMaterials = ProductMaterialsModel.ProductMaterials
            classProductVariantMaterials = ProductVariantMaterialsModel.ProductVariantMaterials
            
            #looping data
            for row in dataStore:
                store_id = row[0] #penulisannya variable inline store_id milik row[0], created_date milik row[1]

                productMaster = classProduct.ProductMaster(store_id)  # Menunggu hingga GetIndexData selesai
                productVariant = classProduct.ProductVariant(store_id)
                productMasterFileName = f"{store_id}_product_master.feather"
                productVariantFileName = f"{store_id}_product_variant.feather"

                salesRecap.WriteFeatherFile(productMaster, productMasterFileName, SetColumnForFeatherFile(2))  # Menunggu hingga WriteFeatherFile selesai
                s3Utils.uploadToS3(productMasterFileName,aws_bucket, pathProductMaster) #call utilities s3 dan passing, if jika berhasil upload

                salesRecap.WriteFeatherFile(productVariant, productVariantFileName, SetColumnForFeatherFile(2))
                s3Utils.uploadToS3(productVariantFileName,aws_bucket, pathProductVariant)

                #productMaterials
                productMaterials = classProductMaterials.GetDataProductMaterials(store_id)
                WrapperCreateFile(productMaterials, f"{store_id}_product_materials.feather",SetColumnForFeatherFile(9),aws_bucket, pathProductMaterials)

                #productVariantMaterials
                productVariantMaterials = classProductVariantMaterials.GetDataProductVariantMaterials(store_id)
                WrapperCreateFile(productVariantMaterials, f"{store_id}_product_variant_materials.feather",SetColumnForFeatherFile(9),aws_bucket, pathProductVariantMaterials)
                
        except Exception as error:
            raise error
        
    @staticmethod
    def CreateFileFeatherProductCategory():
        try:
            s3Utils = S3_utils
            salesRecap = SalesRecapService.SalesRecapService
            classProductCategory = ProductCategoryModel.ProductCategory
            aws_bucket= os.getenv('AWS_BUCKET')
            path = f"{ProductService.directory}/category-product"
            fileName = f"category_product.feather"
            masterCategory = classProductCategory.GetDataIndex()
            salesRecap.WriteFeatherFile(masterCategory, fileName, SetColumnForFeatherFile(3))
            s3Utils.uploadToS3(fileName,aws_bucket, path)
        except Exception as error:
            raise error