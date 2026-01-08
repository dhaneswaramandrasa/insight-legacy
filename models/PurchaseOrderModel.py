from config.database import get_db_connection,get_db_connection_third
from typing import Any
import os
from dotenv import load_dotenv

load_dotenv('.env')
# import logging

class PurchaseOrder:
    @staticmethod
    def GetDataPurchaseOrder(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                id,
                store_id,
                purchase_date,
                purchase_time,
                discount_amount,
                shipping_cost,
                tax_amount,
                exchange_rate,
                status,
                is_paid
            FROM
                ci_store_purchase_order 
            WHERE
                store_id = %s 
                AND purchase_date >= %s 
            ORDER BY
                purchase_date DESC
        """
        # params = (store_id,date, store_id, date)
        params = (store_id,date,)
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
                # print("Koneksi database ditutup.")
    
    def InsertDataPurchaseOrder(purchaseOrders):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_purchase_order" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_purchase_order"
        query = f"""
            INSERT INTO {table_name} (
                id,
                store_id,
                purchase_date,
                purchase_time,
                discount_amount,
                shipping_cost,
                tax_amount,
                exchange_rate,
                status,
                is_paid
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            # Memulai transaksi
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam purchaseOrders
                for order in purchaseOrders:
                    cursor.execute(query, (
                        order[0],  # id
                        order[1],  # store_id
                        order[2],  # purchase_date
                        order[3],  # purchase_time
                        order[4],  # discount_amount
                        order[5],  # shipping_cost
                        order[6],  # tax_amount
                        order[7],  # exchange_rate
                        order[8],  # status
                        order[9]   # is_paid
                    ))

                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_purchase_order.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting purchase orders: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
