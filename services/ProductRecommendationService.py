from config.database import get_db_connection
import os
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from models import StoreModel
from config.bugsnag import get_logger
from dotenv import load_dotenv
from typing import Any
from config.s3 import download_file_from_s3

load_dotenv('.env')
logger = get_logger()
conn_generator = get_db_connection()
conn = next(conn_generator)

class ProductRecommendationService():
    directory = "data-lake-python-feather"

    def ProductRecommendation(store_id: int):
        # Download data Product & Variant
        dfs_13 = []
        path_sales_order_item = f"{ProductRecommendationService.directory}/sales-order-item/{store_id}_sales_order_item_6_months_ago.feather"
        df_sales_order_item = download_file_from_s3(path_sales_order_item)

        dfs_13.append(df_sales_order_item)

        if dfs_13 and not dfs_13[0].empty:
            df_sales_order_item = pd.concat(dfs_13)
        else:
            df_sales_order_item = pd.DataFrame(columns = ['id', 'store_id', 'product_id', 'product_variant_id', 'product_combo_id', 'sales_order_id', 'qty'])
        
        dfs_14 = []
        path_sales = f"{ProductRecommendationService.directory}/sales-order/{store_id}_sales_order_6_months_ago.feather"
        df_sales_2 = download_file_from_s3(path_sales)

        dfs_14.append(df_sales_2)

        if dfs_14 and not dfs_14[0].empty:
            df_sales_2 = pd.concat(dfs_14)
            df_sales_2 = df_sales_2.rename(columns = {
                'id': 'sales_order_id'
            })
            df_sales_2 = df_sales_2[['sales_order_id', 'order_date', 'total_amount', 'total_cost_amount']]
        else:
            df_sales_2 = pd.DataFrame(columns = ['sales_order_id', 'order_date', 'total_amount', 'total_cost_amount'])
        
        dfs_15 = []
        path_product_master = f"{ProductRecommendationService.directory}/product-master/{store_id}_product_master.feather"
        df_product_master_2 = download_file_from_s3(path_product_master)

        dfs_15.append(df_product_master_2)

        if dfs_15 and not dfs_15[0].empty:
            df_product_master_2 = pd.concat(dfs_15)
            df_product_master_2 = df_product_master_2.rename(columns = {
                'id': 'product_id',
                'name': 'product_name'
            })
        else:
            df_product_master_2 = pd.DataFrame(columns = ['product_id', 'store_id', 'product_name', 'category_id', 'status', 'buy_price'])
        
        dfs_16 = []
        path_variant_master = f"{ProductRecommendationService.directory}/product-variant/{store_id}_product_variant.feather"
        df_product_variant_master_2 = download_file_from_s3(path_variant_master)

        dfs_16.append(df_product_variant_master_2)

        if dfs_16 and not dfs_16[0].empty:
            df_product_variant_master_2 = pd.concat(dfs_16)
            df_product_variant_master_2 = df_product_variant_master_2.rename(columns = {
                'id': 'product_variant_id',
                'name': 'product_variant_name'
            })
        else:
            df_product_variant_master_2 = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'product_variant_name', 'category_id', 'status', 'buy_price'])
        
        path_category = f"{ProductRecommendationService.directory}/category-product/category_product.feather"
        df_category = download_file_from_s3(path_category)
        df_category = df_category.rename(columns = {
            'id': 'category_id',
            'name': 'product_category'
        })

        df_to_join_product_master_2 = pd.merge(df_category, df_product_master_2, how = 'inner', on = 'category_id') # apakah inner atau left? karena ada yg pas dijoin category nya null
        df_to_join_sales_order_item = pd.merge(df_sales_order_item, df_to_join_product_master_2, how = 'left', on = 'product_id')
        df_to_join_sales_order_item = pd.merge(df_to_join_sales_order_item, df_product_variant_master_2, how = 'left', on = 'product_variant_id')
        df_to_join_sales_order_item = pd.merge(df_to_join_sales_order_item, df_sales_2, how = 'left', on = 'sales_order_id')

        df_items = df_to_join_sales_order_item.pivot_table(index = 'sales_order_id', columns = ['product_name'], values = 'qty').fillna(0)
        item_names = df_items.columns.values.tolist()

        recommendations = {}
        for item in item_names:
            item_corr = df_items.corrwith(df_items[item])
            item_corr = item_corr.dropna()
            item_corr = item_corr.sort_values(ascending = False)
            item_corr = item_corr[item_corr < 1.0] # Remove self-correlation

            # Keep only the top 10 correlated products
            top_10_corr = item_corr.head(10)

            # Get product_id and product_category from the original DataFrame for each correlated item
            correlated_items = df_to_join_sales_order_item[df_to_join_sales_order_item['product_name'].isin(top_10_corr.index)][['product_name', 'product_id', 'product_category']].drop_duplicates()

            # Format the recommendation as "product_id |-| product_category"
            correlated_items['product_name'] = correlated_items['product_name'].astype(str)
            correlated_items['recommendation'] = correlated_items['product_name'].astype(str) + " |-| " + correlated_items['product_category']

            # Add to the dictionary
            recommendations[item] = correlated_items['recommendation'].tolist()
        
        # Convert the dictionary to a DataFrame
        recommendations_df = pd.DataFrame([
            {'product_name': key, 'recommendation': value} for key, value in recommendations.items()
        ])

        # Merge the recommendations with the original product_id and product_category
        result = df_to_join_sales_order_item[['product_name', 'product_id', 'product_category']].drop_duplicates().merge(recommendations_df, on='product_name')

        # Final result columns: product_id, product_category, recommendation
        final_df = result[['product_name', 'product_category', 'recommendation']]

        final_df_to_list = list(final_df.itertuples(index = False, name = None)) # ubah DataFrame menjadi tuple di dalam list

        return final_df_to_list
    
    def CreateFileFeatherProductRecommendation():
        try:
            store = StoreModel.Store
            dataStore = store.GetIndexData()
            classProductRecommendation = ProductRecommendationService
            bucket_name = os.getenv('AWS_BUCKET')
            for row in dataStore:
                store_id = row[0]
                # product recommendation
                productRecommendation = classProductRecommendation.ProductRecommendation(store_id)
                file_name = f"{store_id}_product_recommendation.feather"
                file_path = f"{ProductRecommendationService.directory}/ml/product-recommendation"
                WrapperCreateFile(sourceData = productRecommendation,
                                  fileName = file_name,
                                  columns = SetColumnForFeatherFile(11),
                                  aws_bucket = bucket_name,
                                  path = file_path)

        except Exception as error:
            raise error