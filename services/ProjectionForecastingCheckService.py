from config.database import get_db_connection, get_db_connection_second, get_db_connection_third
import os
import datetime as dt
from datetime import datetime, timedelta
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from config.bugsnag import get_logger
from dotenv import load_dotenv
from typing import Any
from decimal import Decimal
import json

load_dotenv('.env')
logger = get_logger()
conn_generator = get_db_connection()
conn = next(conn_generator)

class ProjectionForecastingCheckService():
    directory = "data-lake-python-feather"
    
    def UpdateLastSyncForSalesProjectionForecasting(last_date_sync, store_id):
        # Jalankan query UPDATE
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = f"""
            UPDATE ci_store_ext_settings
            SET last_date_sync_for_sales_projection_forecasting = %s
            WHERE id = %s
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, (last_date_sync,store_id))
                # Commit perubahan ke database
                conn.commit()
                return True
        except Exception as e:
            # conn.rollback()
            print(f"Error saat memperbarui data {store_id}: {e}")
            raise e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
    
    def UpdateLastSyncForProfitProjectionForecasting(last_date_sync, store_id):
        # Jalankan query UPDATE
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = f"""
            UPDATE ci_store_ext_settings
            SET last_date_sync_for_profit_projection_forecasting = %s
            WHERE id = %s
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, (last_date_sync,store_id))
                # Commit perubahan ke database
                conn.commit()
                return True
        except Exception as e:
            conn.rollback()
            print(f"Error saat memperbarui data {store_id}: {e}")
            raise e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
    
    def GetSyncedStoreSalesProjectionForecasting(store_id):
        conn_generator = None
        conn = None
        # Mendapatkkan koneksi database dari fungsi get_db_connection
        conn_generator = get_db_connection_second()
        conn = next(conn_generator, None)  # Gunakan None sebagai default jika tidak ada koneksi

        if conn is None:
            return None  # Tidak melakukan apa-apa jika koneksi adalah None
        
        try:
            cursor = conn.cursor()
            query = f"""
            SELECT
                id,
                last_date_sync_for_sales_projection_forecasting
            FROM ci_store_ext_settings
            WHERE id = %s
            LIMIT 1
            """
            params = (store_id,)
            cursor.execute(query, params)
            data = cursor.fetchone()
            return data
        except Exception as e:
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def GetSyncedStoreProfitProjectionForecasting(store_id):
        conn_generator = None
        conn = None
        # Mendapatkan koneksi database dari fungsi get_db_connection
        conn_generator = get_db_connection_second()
        conn = next(conn_generator, None)  # Gunakan None sebagai default jika tidak ada koneksi

        if conn is None:
            return None  # Tidak melakukan apa-apa jika koneksi adalah None
        
        try:
            cursor = conn.cursor()
            query = f"""
            SELECT
                id,
                last_date_sync_for_profit_projection_forecasting
            FROM ci_store_ext_settings
            WHERE id = %s
            LIMIT 1
            """
            params = (store_id,)
            cursor.execute(query, params)
            data = cursor.fetchone()
            return data
        except Exception as e:
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()