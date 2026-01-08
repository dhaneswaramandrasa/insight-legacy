from config.database import get_db_connection, get_db_connection_second
from typing import Any
import logging

class SalesRecap:
    
    # Configure logging to display debug messages
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def SalesTrackingVision(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        
        query = """
            SELECT * FROM
                (
                SELECT
                    ci_store_sales_order_recap_days.store_id,
                    SUM(ci_store_sales_order_recap_days.total_sales_amount) AS total_amount,
                    ci_store_sales_order_recap_days.date_trx_days,
                    ci_store_sales_order_recap_days.all_sales_order_id_count as trx_sales_id_count,
                    0 AS flag
                FROM
                    ci_store_sales_order_recap_days 
                WHERE
                    ci_store_sales_order_recap_days.store_id = %s
                AND ci_store_sales_order_recap_days.is_paid = 1
               AND YEAR(ci_store_sales_order_recap_days.date_trx_days) = %s
                GROUP BY ci_store_sales_order_recap_days.date_trx_days UNION
                SELECT
                    ci_store_sales_return_recap_days.store_id,
                    SUM(ci_store_sales_return_recap_days.total_amount) AS total_amount,
                    ci_store_sales_return_recap_days.date_trx_days,
                    ci_store_sales_return_recap_days.all_sales_return_id_count as trx_sales_id_count,
                    1 AS flag
                FROM
                    ci_store_sales_return_recap_days 
                WHERE
                    ci_store_sales_return_recap_days.store_id = %s
                AND YEAR(ci_store_sales_return_recap_days.date_trx_days) = %s
                GROUP BY ci_store_sales_return_recap_days.date_trx_days
                    ) as salesvision
        """
        # params = (store_id,store_id)
        params = (store_id, date, store_id, date,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
        except Exception as e:
            raise e
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                print("Koneksi database ditutup.")
        
    
    @staticmethod
    def ProductPopulerVision(store_id: Any = 0, date: str = None):
        # print(f"Store ID: {store_id}")
        # Configure logging
        # logging.debug(f"Store ID: {store_id}, Date: {date}")
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        query = """
            SELECT
	            ci_store_sales_order_recap_days_by_items.store_id,
                ci_store_sales_order_recap_days_by_items.product_id,
                ci_store_sales_order_recap_days_by_items.product_variant_id,
                ci_store_sales_order_recap_days_by_items.product_combo_id,
                ci_store_sales_order_recap_days_by_items.total_qty_return,
                ci_store_sales_order_recap_days_by_items.total_cost_amount_return,
                ci_store_sales_order_recap_days_by_items.total_qty,
                ci_store_sales_order_recap_days_by_items.total_amount,
                ci_store_sales_order_recap_days_by_items.date_trx_days,
                ci_store_sales_order_recap_days_by_items.total_amount_return,
                ci_store_sales_order_recap_days_by_items.total_cost_amount,
                ci_store_sales_order_recap_days_by_items.total_qty_from_item_combo,
                ci_store_sales_order_recap_days_by_items.total_qty_unpaid,
	            ci_store_sales_order_recap_days_by_items.total_cost_amount_unpaid,
	            ci_store_sales_order_recap_days_by_items.total_amount_unpaid
            FROM
                ci_store_sales_order_recap_days_by_items 
            WHERE
                ci_store_sales_order_recap_days_by_items.store_id = %s 
                AND YEAR(ci_store_sales_order_recap_days_by_items.date_trx_days)=%s
            ORDER BY date_trx_days asc
        """
        # params = (store_id,date, store_id, date)
        params = (store_id,date,)
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
                print("Koneksi database ditutup.")
        
    
    @staticmethod
    def CustomerTrackingVision(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        
        query = """
            SELECT
                ci_store_sales_order_recap_days.store_id,
                ci_store_sales_order_recap_days.all_sales_order_id_count,
                ci_store_sales_order_recap_days.date_trx_days
            FROM ci_store_sales_order_recap_days
            WHERE ci_store_sales_order_recap_days.store_id = %s
            AND YEAR(ci_store_sales_order_recap_days.date_trx_days) = %s
        """
        params = (store_id,date,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
        finally:
            conn.close()
        
        return data
    
    @staticmethod
    def GetDeposit(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        query = """
                SELECT
                    store_id,
                    total_amount as total_deposit,
                    date_trx_days as transaction_date_deposit
                FROM
                    ci_store_sales_order_recap_days_by_items 
                WHERE store_id = %s 
                AND YEAR(date_trx_days) = %s
                AND is_deposit_item = 1
            """
        params = (store_id, date,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
        except Exception as error:
            raise error
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                print("Koneksi database ditutup.")
    
    @staticmethod
    def GetAllSummarySales(store_id: Any = 0, start_date: str = None, end_date : str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        query = """
                SELECT
                    ci_store_sales_order_recap_days.store_id,
                    SUM( ci_store_sales_order_recap_days.total_sales_amount ) AS total_amount,
                    SUM( ci_store_sales_order_recap_days.all_sales_order_id_count ) AS all_trx_id_count,
                    0 AS flag 
                FROM
                    ci_store_sales_order_recap_days 
                WHERE
                    ci_store_sales_order_recap_days.store_id = %s 
                AND ci_store_sales_order_recap_days.is_paid=1
                AND ci_store_sales_order_recap_days.date_trx_days BETWEEN %s AND %s UNION
                SELECT
                    ci_store_sales_return_recap_days.store_id,
                    SUM( ci_store_sales_return_recap_days.total_amount ) AS total_amount,
                    SUM( ci_store_sales_return_recap_days.all_sales_return_id_count ) AS all_trx_id_count,
                    1 AS flag 
                FROM
                    ci_store_sales_return_recap_days 
                WHERE
                    ci_store_sales_return_recap_days.store_id = %s
                AND ci_store_sales_return_recap_days.date_trx_days BETWEEN %s AND %s
            """
        params = (store_id, start_date, end_date, store_id, start_date, end_date,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
        except Exception as error:
            raise error
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                print("Koneksi database ditutup.")
        
    
    @staticmethod
    def GetAllSummarySalesProduct(store_id: Any = 0, start_date: str = None, end_date : str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
        query = """
                SELECT
                    ci_store_sales_order_recap_days_by_items.store_id,
                    SUM( ci_store_sales_order_recap_days_by_items.total_qty ) AS total_qty,
                    SUM( ci_store_sales_order_recap_days_by_items.total_qty_return ) AS total_qty_return,
                    0 AS flag,
                    SUM(ci_store_sales_order_recap_days_by_items.total_qty_from_item_combo) as total_qty_from_item_combo,
                    SUM(ci_store_sales_order_recap_days_by_items.total_cost_amount) AS total_cost_amount,
                    sum(ci_store_sales_order_recap_days_by_items.total_amount) AS total_amount,
                    SUM(ci_store_sales_order_recap_days_by_items.total_qty_unpaid) as total_qty_unpaid,
                    SUM(ci_store_sales_order_recap_days_by_items.total_cost_amount_unpaid) as total_cost_amount_unpaid,
                    SUM(ci_store_sales_order_recap_days_by_items.total_amount_unpaid) as total_amount_unpaid,
                    SUM(ci_store_sales_order_recap_days_by_items.total_cost_amount_return) as total_cost_amount_return
                FROM
                    ci_store_sales_order_recap_days_by_items 
                WHERE
                    ci_store_sales_order_recap_days_by_items.store_id = %s 
                AND ci_store_sales_order_recap_days_by_items.date_trx_days BETWEEN %s and %s 
                UNION
                SELECT
                    ci_store_sales_order.store_id,
                    SUM( soi.qty ) AS total_qty,
                    0 as total_qty_return,
                    1 AS flag,
                    0 as total_qty_from_item_combo,
                    0 as total_cost_amount,
                    0 as total_amount,
                    0 as total_qty_unpaid,
                    0 as total_cost_amount_unpaid,
                    0 as total_amount_unpaid,
                    0 as total_cost_amount_return
                FROM
                    ci_store_sales_order
                    LEFT JOIN ci_store ON ci_store.id = ci_store_sales_order.store_id
                    LEFT JOIN ci_store_customer_deposit_transaction ON ci_store_customer_deposit_transaction.sales_order_id = ci_store_sales_order.id
                    LEFT JOIN ci_store_sales_order_item AS soi ON soi.sales_order_id = ci_store_sales_order.id 
                WHERE
                    ci_store_sales_order.store_id = %s 
                    AND ci_store_sales_order.STATUS <> 'X' 
                    AND ci_store_sales_order.STATUS <> 'D' 
                    AND ci_store_customer_deposit_transaction.master_deposit_id >0
                    AND ci_store_sales_order.order_date BETWEEN %s and %s
            """
        params = (store_id, start_date, end_date, store_id, start_date, end_date,)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return data
        except Exception as error:
            raise error
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                print("Koneksi database ditutup.")
        
    
    def CheckDataRecordLog(store_id, modul):
        conn = None
        try:
            # Buat koneksi ke database
            conn_generator = get_db_connection_second()
            conn = next(conn_generator, None)

            if conn is None:
                raise ConnectionError("Failed to establish a database connection.")
            
            # Query SQL untuk mengecek apakah record sudah ada dalam tabel
            query_check = """
                SELECT store_id, modul, date_log_record
                FROM ci_log_record_datainsight
                WHERE store_id = %s AND modul = %s
                LIMIT 1
            """
            params = (store_id, modul,)
            with conn.cursor() as cursor:
                cursor.execute(query_check, params)
                existing_record = cursor.fetchone()  # Ambil hanya satu hasil
                cursor.fetchall()  # Membaca semua sisa hasil (meskipun tidak digunakan)
        
            return existing_record  # True jika record sudah ada, False jika tidak
        except Exception as error:
            raise error
        finally:
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi


    def CreateRecordLog(store_id, last_date, modul):
        try:
            # Buat koneksi ke database
            conn_generator = get_db_connection_second()
            conn = next(conn_generator, None)
            
            # Query SQL untuk menambahkan record baru ke dalam tabel
            query_insert = """
                INSERT INTO ci_log_record_datainsight (store_id, date_log_record, modul)
                VALUES (%s, %s, %s)
            """
            params_insert = (store_id, last_date, modul)
            with conn.cursor() as cursor:
                cursor.execute(query_insert, params_insert)
            
            # Commit perubahan ke dalam database
            conn.commit()
        except Exception as e:
            ## Rollback jika terjadi kesalahan
            if conn and conn.is_connected():
                conn.rollback()
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi

    def UpdateRecordLog(store_id, last_date, modul):
        try:
            # Buat koneksi ke database
            conn_generator = get_db_connection_second()
            conn = next(conn_generator, None)
            
            # Query SQL untuk memperbarui tanggal record yang sudah ada di tabel
            query_update = """
                UPDATE ci_log_record_datainsight
                SET date_log_record = %s
                WHERE store_id = %s AND modul = %s
            """
            params_update = (last_date, store_id, modul)
            with conn.cursor() as cursor:
                cursor.execute(query_update, params_update)
            
            # Commit perubahan ke dalam database
            conn.commit()
        except Exception as e:
            # Rollback jika terjadi kesalahan
            if conn and conn.is_connected():
                conn.rollback()
            raise e
        finally:
        # Tutup cursor dan koneksi
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                print("Koneksi database ditutup.")
