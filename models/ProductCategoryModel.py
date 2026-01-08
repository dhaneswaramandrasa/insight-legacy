from config.database import get_db_connection
from typing import Any
import logging

class ProductCategory:
    
    # Configure logging to display debug messages
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def GetDataIndex():
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                id,
                name_en as name
            FROM
                ci_cd_category 
            ORDER BY
                name_en
        """
        # params = (store_id,date, store_id, date)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query,)
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
    