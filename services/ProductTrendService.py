from config.database import get_db_connection, get_db_connection_second, get_db_connection_third
import os
import datetime as dt
from datetime import datetime, timedelta
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from models import StoreModel
from config.bugsnag import get_logger
from dotenv import load_dotenv
from typing import Any
from config.s3 import download_file_from_s3, upload_file_to_s3
from decimal import Decimal
import json

load_dotenv('.env')
logger = get_logger()
conn_generator = get_db_connection()
conn = next(conn_generator)

class ProductTrendService():
    directory = "data-lake-python-feather"

    def ProductTrendByDay(store_id: int):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours=7)
        start_date_historical_data = (now_wib - timedelta(days=181)).date() # karena ada lag 1 hari data rekap
        end_date_historical_data = (now_wib - timedelta(days=1)).date() # karena ada lag 1 hari data rekap
        start_year_penarikan = start_date_historical_data.year
        end_year_penarikan = end_date_historical_data.year
        years_raw = [start_year_penarikan, end_year_penarikan]

        seen = set()
        years = []

        for item in years_raw:
            if item not in seen:
                seen.add(item)
                years.append(item)
        
        # Download Data Sales Order Item, Product, and Variant
        dfs_21 = []
        for year in years:
            try:
                path_sales_order_item_recap = f"{ProductTrendService.directory}/product-populer-vision/{store_id}_{year}_product_populer_vision.feather"
                df_sales_order_item_recap = download_file_from_s3(path_sales_order_item_recap)
                dfs_21.append(df_sales_order_item_recap)
            except:
                continue
        
        if dfs_21 and not dfs_21[0].empty:
            df_sales_order_item_recap = pd.concat(dfs_21)
            # dfs_22 = []
            # path_product_master = f"{ProductTrendService.directory}/product-master/{store_id}_product_master.feather"
            # df_product_master_4 = download_file_from_s3(path_product_master)

            # dfs_22.append(df_product_master_4)

            # if dfs_22 and not dfs_22[0].empty:
            #     df_product_master_4 = pd.concat(dfs_22)
            #     df_product_master_4 = df_product_master_4.rename(columns = {
            #         'id': 'product_id',
            #         'name': 'product_name'
            #     })
            # else:
            #     df_product_master_4 = pd.DataFrame(columns = ['product_id', 'store_id', 'product_name', 'category_id', 'status', 'buy_price'])
            
            tuple_within_list_5 = ProductTrendService.GetProductMasterDBReplica(store_id)
            columns_5 = ['id', 'store_id', 'name', 'category_id', 'status', 'buy_price']
            df_product_master_4 = pd.DataFrame(tuple_within_list_5, columns = columns_5)
            if not df_product_master_4.empty:
                df_product_master_4 = df_product_master_4.rename(columns = {
                    'id': 'product_id',
                    'name': 'product_name'
                })
            else:
                df_product_master_4 = pd.DataFrame(columns = ['product_id', 'store_id', 'product_name', 'category_id', 'status', 'buy_price'])
            
            # dfs_23 = []
            # path_variant_master = f"{ProductTrendService.directory}/product-variant/{store_id}_product_variant.feather"
            # df_product_variant_master_4 = download_file_from_s3(path_variant_master)

            # dfs_23.append(df_product_variant_master_4)

            # if dfs_23 and not dfs_23[0].empty:
            #     df_product_variant_master_4 = pd.concat(dfs_23)
            #     df_product_variant_master_4 = df_product_variant_master_4.rename(columns = {
            #         'id': 'product_variant_id',
            #         'name': 'product_variant_name'
            #     })
            # else:
            #     df_product_variant_master_4 = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'product_variant_name', 'category_id', 'status', 'buy_price'])
            
            tuple_within_list_6 = ProductTrendService.GetProductVariantDBReplica(store_id)
            columns_6 = ['id', 'store_id', 'name', 'category_id', 'status', 'buy_price']
            df_product_variant_master_4 = pd.DataFrame(tuple_within_list_6, columns = columns_6)
            if not df_product_variant_master_4.empty:
                df_product_variant_master_4 = df_product_variant_master_4.rename(columns = {
                    'id': 'product_variant_id',
                    'name': 'product_variant_name'
                })
            else:
                df_product_variant_master_4 = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'product_variant_name', 'category_id', 'status', 'buy_price'])
            
            df_sales_order_item_recap['product_id'] = df_sales_order_item_recap['product_id'].fillna(0).astype(int)
            df_product_master_4['product_id'] = df_product_master_4['product_id'].astype(int)
            df_sales_order_item_recap['product_variant_id'] = df_sales_order_item_recap['product_variant_id'].fillna(0).astype(int)
            df_product_variant_master_4['product_variant_id'] = df_product_variant_master_4['product_variant_id'].astype(int)
            df_to_join_sales_order_item_recap = pd.merge(df_sales_order_item_recap, df_product_master_4, how = 'left', on = 'product_id')
            df_to_join_sales_order_item_recap = pd.merge(df_to_join_sales_order_item_recap, df_product_variant_master_4, how = 'left', on = 'product_variant_id')

            df_to_join_sales_order_item_recap['date_trx_days'] = pd.to_datetime(df_to_join_sales_order_item_recap['date_trx_days'])
            df_to_join_sales_order_item_recap['day_index'] = df_to_join_sales_order_item_recap['date_trx_days'].dt.dayofweek + 1
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap[df_to_join_sales_order_item_recap['product_id'].notnull()]
            df_to_join_sales_order_item_recap['product_variant_concat'] = df_to_join_sales_order_item_recap.apply(
                lambda row: f"{row['product_name']} |-| {row['product_variant_name']}" if pd.notna(row['product_variant_name']) else f"{row['product_name']} |-| 0", axis = 1
            ) # confirm apakah ada kasus dimana product_variant_id nya NOT NULL tapi product_variant_name nya NULL dan/atau product_name nya NULL
            df_to_join_sales_order_item_recap['gross_revenue'] = df_to_join_sales_order_item_recap['total_amount'] - df_to_join_sales_order_item_recap['total_cost_amount']
            df_to_join_sales_order_item_recap['total_qty'] = df_to_join_sales_order_item_recap['total_qty'] - df_to_join_sales_order_item_recap['total_qty_return'] - df_to_join_sales_order_item_recap['total_qty_unpaid']
            index_filter_1 = ((df_to_join_sales_order_item_recap['total_qty'] != 0) | (df_to_join_sales_order_item_recap['total_qty_from_item_combo'] == 0))
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap.loc[index_filter_1, :]
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap[df_to_join_sales_order_item_recap['total_qty'] > 0]
            df_to_join_sales_order_item_recap.rename(columns = {
                'total_amount': 'total_revenue'
            }, inplace = True)
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap[['product_variant_concat', 'total_revenue', 'gross_revenue', 'total_qty', 'day_index']]
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap.groupby(['product_variant_concat', 'day_index']).agg({
                'total_revenue': 'sum',
                'gross_revenue': 'sum',
                'total_qty': 'sum'
            })
            df_to_join_sales_order_item_recap['total_qty'] = pd.to_numeric(df_to_join_sales_order_item_recap['total_qty'], errors='coerce').astype(float)
            df_to_join_sales_order_item_recap = df_to_join_sales_order_item_recap.reset_index()
        else:
            df_to_join_sales_order_item_recap = pd.DataFrame(columns = ["product_variant_concat", "day_index", "total_revenue", "gross_revenue", "total_qty"])

        # df_to_join_sales_order_item_recap_to_list = list(df_to_join_sales_order_item_recap.itertuples(index = False, name = None)) # ubah DataFrame menjadi tuple di dalam list

        # return df_to_join_sales_order_item_recap_to_list
        return df_to_join_sales_order_item_recap
    
    # def CreateFileFeatherProductTrendByDay():
    #     try:
    #         store = StoreModel.Store
    #         dataStore = store.GetIndexData()
    #         classProductTrendByDay = ProductTrendService
    #         bucket_name = os.getenv('AWS_BUCKET')
    #         for row in dataStore:
    #             store_id = row[0]
    #             # product trend by day
    #             productTrendByDay = classProductTrendByDay.ProductTrendByDay(store_id)
    #             file_name = f"{store_id}_product_trend_by_day.feather"
    #             file_path = f"{ProductTrendService.directory}/ml/product-trend"
    #             WrapperCreateFile(sourceData = productTrendByDay,
    #                               fileName = file_name,
    #                               columns = SetColumnForFeatherFile(12),
    #                               aws_bucket = bucket_name,
    #                               path = file_path)
    #     except Exception as error:
    #         raise error
    
    # def ProductTrendByHour(store_id: int):
    #     dfs_17 = []
    #     path_sales_order_item = f"{ProductTrendService.directory}/sales-order-item/{store_id}_sales_order_item_6_months_ago.feather"
    #     df_sales_order_item_2 = download_file_from_s3(path_sales_order_item)

    #     dfs_17.append(df_sales_order_item_2)

    #     if dfs_17 and not dfs_17[0].empty:
    #         df_sales_order_item_2 = pd.concat(dfs_17)
    #         df_sales_order_item_2.rename(columns = {
    #             'qty': 'total_qty'
    #         }, inplace = True)

    #         dfs_18 = []
    #         path_sales = f"{ProductTrendService.directory}/sales-order/{store_id}_sales_order_6_months_ago.feather"
    #         df_sales_3 = download_file_from_s3(path_sales)

    #         dfs_18.append(df_sales_3)

    #         if dfs_18 and not dfs_18[0].empty:
    #             df_sales_3 = pd.concat(dfs_18)
    #             df_sales_3 = df_sales_3.rename(columns = {
    #                 'id': 'sales_order_id'
    #             })
    #             df_sales_3 = df_sales_3[['sales_order_id', 'total_amount', 'total_cost_amount']]
    #         else:
    #             df_sales_3 = pd.DataFrame(columns = ['sales_order_id', 'total_amount', 'total_cost_amount'])
            
    #         dfs_19 = []
    #         path_product_master = f"{ProductTrendService.directory}/product-master/{store_id}_product_master.feather"
    #         df_product_master_3 = download_file_from_s3(path_product_master)

    #         dfs_19.append(df_product_master_3)

    #         if dfs_19 and not dfs_19[0].empty:
    #             df_product_master_3 = pd.concat(dfs_19)
    #             df_product_master_3 = df_product_master_3.rename(columns = {
    #                 'id': 'product_id',
    #                 'name': 'product_name'
    #             })
    #         else:
    #             df_product_master_3 = pd.DataFrame(columns = ['product_id', 'store_id', 'product_name', 'category_id', 'status', 'buy_price'])
            
    #         dfs_20 = []
    #         path_variant_master = f"{ProductTrendService.directory}/product-variant/{store_id}_product_variant.feather"
    #         df_product_variant_master_3 = download_file_from_s3(path_variant_master)

    #         dfs_20.append(df_product_variant_master_3)

    #         if dfs_20 and not dfs_20[0].empty:
    #             df_product_variant_master_3 = pd.concat(dfs_20)
    #             df_product_variant_master_3 = df_product_variant_master_3.rename(columns = {
    #                 'id': 'product_variant_id',
    #                 'name': 'product_variant_name'
    #             })
    #         else:
    #             df_product_variant_master_3 = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'product_variant_name', 'category_id', 'status', 'buy_price'])
            
    #         df_sales_order_item_2['product_variant_id'] = df_sales_order_item_2['product_variant_id'].fillna(0).astype(int)
    #         df_product_variant_master_3['product_variant_id'] = df_product_variant_master_3['product_variant_id'].astype(int)
    #         df_to_join_product_variant_master = pd.merge(df_sales_order_item_2, df_product_variant_master_3, how = 'left', on = 'product_variant_id')
    #         df_to_join_product_variant_master = pd.merge(df_to_join_product_variant_master, df_product_master_3, how = 'left', on = 'product_id')
    #         df_to_join_sales_order_item = pd.merge(df_to_join_product_variant_master, df_sales_3, how = 'inner', on = 'sales_order_id')

    #         df_to_join_sales_order_item['order_time'] = pd.to_datetime(df_to_join_sales_order_item['order_time'])
    #         df_to_join_sales_order_item['hour_index'] = df_to_join_sales_order_item['order_time'].dt.hour
    #         custom_order = list(range(0, 24))
    #         df_to_join_sales_order_item['hour_index'] = pd.Categorical(df_to_join_sales_order_item['hour_index'], categories = custom_order, ordered = True)
    #         df_to_join_sales_order_item = df_to_join_sales_order_item[df_to_join_sales_order_item['product_id'].notnull()]
    #         df_to_join_sales_order_item['product_variant_concat'] = df_to_join_sales_order_item.apply(
    #             lambda row: f"{row['product_name']} |-| {row['product_variant_name']}" if pd.notna(row['product_variant_name']) else f"{row['product_name']} |-| 0", axis = 1
    #         )
    #         df_to_join_sales_order_item['gross_revenue'] = df_to_join_sales_order_item['total_amount'] - df_to_join_sales_order_item['total_cost_amount']
    #         df_to_join_sales_order_item.rename(columns = {
    #             'total_amount': 'total_revenue'
    #         }, inplace = True)
    #         df_to_join_sales_order_item = df_to_join_sales_order_item[['product_variant_concat', 'total_revenue', 'gross_revenue', 'total_qty', 'hour_index']]
    #         df_to_join_sales_order_item = df_to_join_sales_order_item.groupby(['product_variant_concat', 'hour_index']).agg({
    #             'total_revenue': 'sum',
    #             'gross_revenue': 'sum',
    #             'total_qty': 'sum'
    #         })
    #         df_to_join_sales_order_item['total_qty'] = pd.to_numeric(df_to_join_sales_order_item['total_qty'], errors = 'coerce').astype(float)
    #         df_to_join_sales_order_item = df_to_join_sales_order_item.reset_index()
    #     else:
    #         df_to_join_sales_order_item = pd.DataFrame(columns = ['product_variant_concat', 'hour_index', 'total_revenue', 'gross_revenue', 'total_qty'])

    #     # df_to_join_sales_order_item_to_list = list(df_to_join_sales_order_item.itertuples(index = False, name = None)) # ubah DataFrame menjadi tuple di dalam list

    #     # return df_to_join_sales_order_item_to_list
    #     return df_to_join_sales_order_item
    
    def GetSalesOrderItemDBAnalyticData(store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        start_date_range = now_wib - timedelta(days = 180)
        fstart_date_range = start_date_range.strftime('%Y-%m-%d')
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order_item" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order_item"
        query = f"""
        SELECT
            id,
            store_id,
            product_id,
            product_variant_id,
            product_combo_id,
            sales_order_id,
            qty,
            order_time,
            status
        FROM {table_name}
        WHERE
            store_id = %s
        AND (order_time BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def GetSalesOrderDBAnalyticData(store_id):
        current_date = datetime.now()
        now_wib = current_date + timedelta(hours = 7)
        start_date_range = now_wib - timedelta(days = 180)
        fstart_date_range = start_date_range.strftime('%Y-%m-%d')
        fnow_wib = now_wib.strftime('%Y-%m-%d')
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order"
        query = f"""
        SELECT
            sales_order_id,
            store_id,
            total_amount,
            total_cost_amount,
            customer_id,
            order_date,
            order_time,
            shipping_cost,
            service_charge_amount,
            tax_amount,
            exchange_rate,
            credit_payment_id,
            payment_amount_credit_payment,
            payment_date_credit_payment,
            sales_return_id,
            total_amount_return,
            total_cost_amount_return,
            exchange_rate_return,
            return_date,
            status,
            is_paid,
            credit_payment_status,
            sales_return_status,
            payment_type_id,
            order_source
        FROM
            {table_name}
        WHERE
            store_id = %s
        AND (order_date BETWEEN %s AND %s)
        AND (payment_date_credit_payment IS NULL OR payment_date_credit_payment BETWEEN %s AND %s)
        AND (return_date IS NULL OR return_date BETWEEN %s AND %s);
        """
        params = (store_id, fstart_date_range, fnow_wib, fstart_date_range, fnow_wib, fstart_date_range, fnow_wib)
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall() # Mengambil semua baris hasil query
                return data
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def GetProductMasterDBReplica(store_id: Any = 0):
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
      
        query = """
            SELECT
                id,
                store_id,
                name,
                category_id,
                status,
                buy_price
            FROM
                ci_store_product 
            WHERE
                store_id = %s
            AND status = "A"
            ORDER BY name
        """
        # params = (store_id,date, store_id, date)
        params = (store_id,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
                # logging.debug(f"Data fetched: {data}")
        except Exception as e:
            # logging.error(f"Error executing query: {e}")
            raise e
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                # print("Koneksi database ditutup.")
    
    def GetProductVariantDBReplica(store_id):
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
      
        query = f"""
            SELECT
                pv.id,
                product.store_id,
                pv.name,
                product.category_id,
                pv.status,
                pv.buy_price
            FROM
                ci_store_product_variants as pv
            INNER JOIN ci_store_product as product on product.id = pv.product_id
            WHERE
                product.store_id = %s
            and product.status = "A"
            and pv.status = "A"
            ORDER BY pv.name
        """
        # params = (store_id,date, store_id, date)
        params = (store_id,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
                # logging.debug(f"Data fetched: {data}")
        except Exception as e:
            # logging.error(f"Error executing query: {e}")
            raise e
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
    
    def ProductTrendByHour(store_id: int):
        tuple_within_list_1 = ProductTrendService.GetSalesOrderItemDBAnalyticData(store_id)
        columns_1 = ['id', 'store_id', 'product_id', 'product_variant_id', 'product_combo_id', 'sales_order_id', 'qty', 'order_time', 'status']
        df_sales_order_item_2 = pd.DataFrame(tuple_within_list_1, columns = columns_1)

        if not df_sales_order_item_2.empty:
            df_sales_order_item_2 = df_sales_order_item_2.rename(columns = {
                'qty': 'total_qty',
                'status': 'order_item_status'
            })
            tuple_within_list_2 = ProductTrendService.GetSalesOrderDBAnalyticData(store_id)
            columns_2 = ['sales_order_id', 'store_id', 'total_amount', 'total_cost_amount', 'customer_id', 'order_date', 'order_time', 'shipping_cost', 'service_charge_amount', 'tax_amount', 'exchange_rate', 'credit_payment_id', 'payment_amount_credit_payment', 'payment_date_credit_payment', 'sales_return_id', 'total_amount_return', 'total_cost_amount_return', 'exchange_rate_return', 'return_date', 'order_status', 'is_paid', 'credit_payment_status', 'sales_return_status', 'payment_type_id', 'order_source']
            df_sales_3 = pd.DataFrame(tuple_within_list_2, columns = columns_2)
            if not df_sales_3.empty:
                df_sales_3 = df_sales_3[['sales_order_id', 'total_amount', 'total_cost_amount', 'order_status', 'is_paid']]
            else:
                df_sales_3 = pd.DataFrame(columns = ['sales_order_id', 'total_amount', 'total_cost_amount', 'order_status', 'is_paid'])

            tuple_within_list_3 = ProductTrendService.GetProductMasterDBReplica(store_id)
            columns_3 = ['id', 'store_id', 'name', 'category_id', 'status', 'buy_price']
            df_product_master_3 = pd.DataFrame(tuple_within_list_3, columns = columns_3)
            if not df_product_master_3.empty:
                df_product_master_3 = df_product_master_3.rename(columns = {
                    'id': 'product_id',
                    'name': 'product_name',
                    'status': 'product_status'
                })
            else:
                df_product_master_3 = pd.DataFrame(columns = ['product_id', 'store_id', 'product_name', 'category_id', 'product_status', 'buy_price'])
            
            tuple_within_list_4 = ProductTrendService.GetProductVariantDBReplica(store_id)
            columns_4 = ['id', 'store_id', 'name', 'category_id', 'status', 'buy_price']
            df_product_variant_master_3 = pd.DataFrame(tuple_within_list_4, columns = columns_4)
            if not df_product_variant_master_3.empty:
                df_product_variant_master_3 = df_product_variant_master_3.rename(columns = {
                    'id': 'product_variant_id',
                    'name': 'product_variant_name',
                    'status': 'product_variant_status'
                })
            else:
                df_product_variant_master_3 = pd.DataFrame(columns = ['product_variant_id', 'store_id', 'product_variant_name', 'category_id', 'product_variant_status', 'buy_price'])

            df_sales_order_item_2['product_variant_id'] = df_sales_order_item_2['product_variant_id'].fillna(0).astype(int)
            df_product_variant_master_3['product_variant_id'] = df_product_variant_master_3['product_variant_id'].astype(int)
            df_to_join_product_variant_master = pd.merge(df_sales_order_item_2, df_product_variant_master_3, how = 'left', on = 'product_variant_id')
            df_to_join_product_variant_master = pd.merge(df_to_join_product_variant_master, df_product_master_3, how = 'left', on = 'product_id')
            df_to_join_sales_order_item = pd.merge(df_to_join_product_variant_master, df_sales_3, how = 'inner', on = 'sales_order_id')

            df_to_join_sales_order_item = df_to_join_sales_order_item[~df_to_join_sales_order_item['order_status'].isin(['X', 'D'])]
            df_to_join_sales_order_item = df_to_join_sales_order_item[df_to_join_sales_order_item['order_item_status'] == 'A']
            df_to_join_sales_order_item = df_to_join_sales_order_item[df_to_join_sales_order_item['is_paid'] == 1]
            df_to_join_sales_order_item = df_to_join_sales_order_item.drop_duplicates()
            df_to_join_sales_order_item['order_time'] = pd.to_datetime(df_to_join_sales_order_item['order_time'])
            df_to_join_sales_order_item['hour_index'] = df_to_join_sales_order_item['order_time'].dt.hour
            custom_order = list(range(0, 24))
            df_to_join_sales_order_item['hour_index'] = pd.Categorical(df_to_join_sales_order_item['hour_index'], categories = custom_order, ordered = True)
            df_to_join_sales_order_item = df_to_join_sales_order_item[df_to_join_sales_order_item['product_id'].notnull()]
            df_to_join_sales_order_item['product_variant_concat'] = df_to_join_sales_order_item.apply(
                lambda row: f"{row['product_name']} |-| {row['product_variant_name']}" if pd.notna(row['product_variant_name']) else f"{row['product_name']} |-| 0", axis = 1
            )
            df_to_join_sales_order_item['gross_revenue'] = df_to_join_sales_order_item['total_amount'] - df_to_join_sales_order_item['total_cost_amount']
            df_to_join_sales_order_item.rename(columns = {
                'total_amount': 'total_revenue'
            }, inplace = True)
            df_to_join_sales_order_item = df_to_join_sales_order_item[['product_variant_concat', 'total_revenue', 'gross_revenue', 'total_qty', 'hour_index']]
            df_to_join_sales_order_item = df_to_join_sales_order_item.groupby(['product_variant_concat', 'hour_index']).agg({
                'total_revenue': 'sum',
                'gross_revenue': 'sum',
                'total_qty': 'sum'
            })
            df_to_join_sales_order_item['total_qty'] = pd.to_numeric(df_to_join_sales_order_item['total_qty'], errors = 'coerce').astype(float)
            df_to_join_sales_order_item = df_to_join_sales_order_item.reset_index()
        else:
            df_to_join_sales_order_item = pd.DataFrame(columns = ['product_variant_concat', 'hour_index', 'total_revenue', 'gross_revenue', 'total_qty'])
        
        return df_to_join_sales_order_item
    
    def convert_decimals(obj):
        if isinstance(obj, dict):
            return {k: ProductTrendService.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ProductTrendService.convert_decimals(i) for i in obj]
        elif isinstance(obj, Decimal):
            return float(obj)  # or use str(obj) if you prefer string representation
        return obj

    def ProductTrendSummary(payload):
        try:
            if payload is not None:
                # Ambil store_id dari payload
                store_id = payload.store_id
                dataStore = [[store_id]]  # Mengubah store_id menjadi format yang diinginkan
            else:
                # Ambil dataStore dari method default
                store = StoreModel.Store
                dataStore = store.GetIndexData()

            bucket_name = os.getenv('AWS_BUCKET')
            for row in dataStore:
                store_id = row[0]
                df_product_trend_daily = ProductTrendService.ProductTrendByDay(store_id)
                df_product_trend_hourly = ProductTrendService.ProductTrendByHour(store_id)

                df_product_trend_daily =  df_product_trend_daily.groupby('day_index', group_keys=False).apply(
                    lambda x: x.nlargest(20, 'total_qty')
                )

                df_product_trend_hourly =  df_product_trend_hourly.groupby('hour_index', group_keys=False).apply(
                    lambda x: x.nlargest(20, 'total_qty')
                )

                if df_product_trend_daily.empty and df_product_trend_hourly.empty:
                    response = {
                        "data": {
                            "product_trend_daily": [],
                            "product_trend_hourly": [],
                        },
                        "status": 200,
                        "error": 0
                    }
                elif not df_product_trend_daily.empty and df_product_trend_hourly.empty:
                    response = {
                        "data": {
                            "product_trend_daily": df_product_trend_daily.to_dict(orient="records"),
                            "product_trend_hourly": []
                        },
                        "status": 200,
                        "error": 0
                    }
                elif df_product_trend_daily.empty and not df_product_trend_hourly.empty:
                    response = {
                        "data": {
                            "product_trend_daily": [],
                            "product_trend_hourly": df_product_trend_hourly.to_dict(orient="records"),
                        },
                        "status": 200,
                        "error": 0
                    }
                elif not df_product_trend_daily.empty and not df_product_trend_hourly.empty:
                    response = {
                        "data": {
                            "product_trend_daily": df_product_trend_daily.to_dict(orient="records"),
                            "product_trend_hourly": df_product_trend_hourly.to_dict(orient="records"),
                        },
                        "status": 200,
                        "error": 0
                    }
                # Convert Decimals in the response to a serializable type
                response = ProductTrendService.convert_decimals(response)
                json_object= json.dumps(response, indent=4)
                file_name = f"{store_id}_product_trend.json"
                with open(f"{store_id}_product_trend.json", "w") as outfile:
                    outfile.write(json_object)
                upload_file_to_s3(file_name, bucket_name, f"data-lake-python-feather/ml/product-trend/{store_id}_product_trend.json")
                try:
                    os.remove(f"{store_id}_product_trend.json")
                except Exception as e:
                    print(f"File {file_name} {bucket_name}: {e}")
        except Exception as error:
            raise error

    # def CreateFileFeatherProductTrendByHour():
    #     try:
    #         store = StoreModel.Store
    #         dataStore = store.GetIndexData()
    #         classProductTrendByHour = ProductTrendService
    #         bucket_name = os.getenv('AWS_BUCKET')
    #         for row in dataStore:
    #             store_id = row[0]
    #             # product trend by hour
    #             productTrendByHour = classProductTrendByHour.ProductTrendByHour(store_id)
    #             file_name = f"{store_id}_product_trend_by_hour.feather"
    #             file_path = f"{ProductTrendService.directory}/ml/product-trend"
    #             WrapperCreateFile(sourceData = productTrendByHour,
    #                               fileName = file_name,
    #                               columns = SetColumnForFeatherFile(13),
    #                               aws_bucket = bucket_name,
    #                               path = file_path)
    #     except Exception as error:
    #         raise error