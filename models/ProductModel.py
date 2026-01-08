from config.database import get_db_connection, get_db_connection_second
from typing import Any
import logging

class Product:
    
    # Configure logging to display debug messages
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def ProductMaster(store_id: Any = 0):
        conn_generator = get_db_connection()
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
    
    @staticmethod
    def ProductVariant(store_id: Any = 0):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
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