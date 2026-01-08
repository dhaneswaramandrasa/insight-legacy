from config.database import get_db_connection, get_db_connection_third
from typing import Any
from datetime import datetime
import os
import pytz
from dotenv import load_dotenv

load_dotenv('.env')

class SalesOrder:
    
    # Configure logging to display debug messages
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def GetDataSalesOrder(store_id: Any = 0, date: str = None, sales_order_id: Any = None, flag: int = 0):
        conn_generator = get_db_connection()
        conn = next(conn_generator)

        if flag == 1:
      
            query = """
                SELECT
                    cso.id,
                    cso.store_id,
                    cso.total_amount,
                    cso.total_cost_amount,
                    cso.customer_id,
                    cso.order_date,
                    cso.order_time,
                    cso.shipping_cost,
                    cso.service_charge_amount,
                    cso.tax_amount,
                    cso.exchange_rate,
                    IFNULL(ocp.payment_amount,0) as payment_amount_credit_payment,
                    ocp.payment_date as payment_date_credit_payment,
                    IFNULL(ssr.total_amount,0) as total_amount_return,
                    IFNULL(ssr.total_cost_amount,0) as total_cost_amount_return,
                    IFNULL(ssr.exchange_rate,0) as exchange_rate_return,
                    ssr.return_date as return_date,
                    cso.status,
                    cso.is_paid,
                    ocp.status as credit_payment_status,
                    ssr.status as sales_return_status,
                    spm.payment_type_id,
                    cso.order_source,
                    ssr.id as sales_return_id,
                    ocp.id as credit_payment_id		
                FROM
                    ci_store_sales_order as cso
                    LEFT JOIN ci_store_sales_order_credit_payment as ocp on ocp.sales_order_id = cso.id
                    LEFT JOIN ci_store_sales_return as ssr on ssr.sales_order_id = cso.id
                    LEFT JOIN ci_store_payment_mode as spm on spm.id = cso.payment_mode_id
                WHERE
                    cso.store_id = %s
                    AND cso.id = %s
                ORDER BY
                    cso.order_time DESC
                LIMIT 1
            """
            params = (store_id,sales_order_id,)

        else:

            query = """
                SELECT
                    cso.id,
                    cso.store_id,
                    cso.total_amount,
                    cso.total_cost_amount,
                    cso.customer_id,
                    cso.order_date,
                    cso.order_time,
                    cso.shipping_cost,
                    cso.service_charge_amount,
                    cso.tax_amount,
                    cso.exchange_rate,
                    IFNULL(ocp.payment_amount,0) as payment_amount_credit_payment,
                    ocp.payment_date as payment_date_credit_payment,
                    IFNULL(ssr.total_amount,0) as total_amount_return,
                    IFNULL(ssr.total_cost_amount,0) as total_cost_amount_return,
                    IFNULL(ssr.exchange_rate,0) as exchange_rate_return,
                    ssr.return_date as return_date,
                    cso.status,
                    cso.is_paid,
                    ocp.status as credit_payment_status,
                    ssr.status as sales_return_status,
                    spm.payment_type_id,
                    cso.order_source,
                    ssr.id as sales_return_id,
                    ocp.id as credit_payment_id		
                FROM
                    ci_store_sales_order as cso
                    LEFT JOIN ci_store_sales_order_credit_payment as ocp on ocp.sales_order_id = cso.id
                    LEFT JOIN ci_store_sales_return as ssr on ssr.sales_order_id = cso.id
                    LEFT JOIN ci_store_payment_mode as spm on spm.id = cso.payment_mode_id
                WHERE
                    cso.store_id = %s
                AND cso.order_date >= %s
                ORDER BY
                    cso.order_time DESC
            """
            params = (store_id,date,)

        # params = (store_id,date, store_id, date)
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

    @staticmethod
    def GetDataSalesOrderItem(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                soi.id,
                so.store_id,
                soi.product_id,
                soi.product_variant_id,
                soi.product_combo_id,
                soi.sales_order_id,
                soi.qty,
                so.order_time,
                soi.status
            FROM
                ci_store_sales_order_item as soi
            INNER JOIN ci_store_sales_order as so on so.id=soi.sales_order_id
            WHERE
                so.store_id = %s
                AND so.order_date >= %s
            ORDER BY
                so.order_date desc,
                so.order_time DESC
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
                # print("Koneksi database ditutup.")

    def InsertDataSalesOrder(salesOrder):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order"
        select_query = f"SELECT status,sales_order_id FROM {table_name} WHERE sales_order_id = %s"
        insert_query = f"""
                            INSERT INTO {table_name} (
                                sales_order_id, store_id, total_amount, total_cost_amount, customer_id, order_date, order_time, 
                                shipping_cost, service_charge_amount, tax_amount, exchange_rate, payment_amount_credit_payment, 
                                payment_date_credit_payment, total_amount_return, total_cost_amount_return, exchange_rate_return, 
                                return_date, status, is_paid, credit_payment_status, sales_return_status, payment_type_id, 
                                order_source, sales_return_id, credit_payment_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

        update_query = f"""
                            UPDATE {table_name} SET
                                store_id = %s, total_amount = %s, total_cost_amount = %s, customer_id = %s, order_date = %s, 
                                order_time = %s, shipping_cost = %s, service_charge_amount = %s, tax_amount = %s, exchange_rate = %s, 
                                payment_amount_credit_payment = %s, payment_date_credit_payment = %s, total_amount_return = %s, 
                                total_cost_amount_return = %s, exchange_rate_return = %s, return_date = %s, status = %s, is_paid = %s, 
                                credit_payment_status = %s, sales_return_status = %s, payment_type_id = %s, order_source = %s, 
                                sales_return_id = %s, credit_payment_id = %s, modified_time = %s WHERE sales_order_id = %s
                        """
        
        # Zona waktu Asia/Jakarta
        jakarta_timezone = pytz.timezone("Asia/Jakarta")
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
               # Looping untuk setiap baris data dalam salesOrder
                for order in salesOrder:
                    # Pengecekan apakah sales_order_id sudah ada
                    cursor.execute(select_query, (order[0],))
                    result = cursor.fetchone()
                    
                    if result:  # Jika data ada
                        current_status = result[0]
                        current_order_id = result[1]
                        if current_status != order[17] or current_order_id == order[0]:  # Hanya update jika status berbeda
                            modified_time = datetime.now(jakarta_timezone)  # Waktu saat ini di Asia/Jakarta
                            print(modified_time)
                            cursor.execute(update_query, (
                                order[1], order[2], order[3], order[4], order[5], order[6], order[7], order[8], 
                                order[9], order[10], order[11], order[12], order[13], order[14], order[15], order[16], 
                                order[17], order[18], order[19], order[20], order[21], order[22], order[23], order[24], modified_time, order[0]
                            ))
                    else:  # Jika data tidak ada, lakukan insert
                        cursor.execute(insert_query, order)
                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_sales_order.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting sales orders: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()

    def InsertDataSalesOrderItem(salesOrderItems):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_sales_order_item" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_sales_order_item"
        query = f"""
            INSERT INTO {table_name} (
                id,
                store_id,
                product_id,
                product_variant_id,
                product_combo_id,
                sales_order_id,
                qty,
                order_time,
                status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            # Memulai transaksi
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam salesOrderItems
                for item in salesOrderItems:
                    cursor.execute(query, (
                        item[0],  # id
                        item[1],  # store_id
                        item[2],  # product_id
                        item[3],  # product_variant_id
                        item[4],  # product_combo_id
                        item[5],  # sales_order_id
                        item[6],  # qty
                        item[7],  # order_time
                        item[8]   # status
                    ))

                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_sales_order_item.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting sales order items: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
    
    def GetDataOrderPromo(storeId, startDate, endDate):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                id AS order_id,
                store_id,
                order_date,
                order_time,
                discount_voucher_id,
                total_amount
            FROM
                ci_store_sales_order 
            WHERE
                store_id = %s 
                AND discount_voucher_id != 0 
                AND order_date BETWEEN %s AND %s
        """
        params = (storeId,startDate,endDate)
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

