
from config.database import get_db_connection, get_db_connection_third
from typing import Any
import os
from dotenv import load_dotenv

load_dotenv('.env')
# import logging

class StockOpnameItems:
    @staticmethod
    def GetDataStockOpnameItems(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                so.store_id,
                soi.opname_id,
                soi.product_id,
                soi.product_variant_id,
                soi.qty,
                soi.qty_sys,
                so.date 
            FROM
                ci_store_stock_opname_items AS soi
                LEFT JOIN ci_store_stock_opname AS so ON so.id = soi.opname_id 
            WHERE
                so.store_id = %s 
                AND so.STATUS = 'P' 
                AND so.record_to_report_profit_loss = 1 
                AND so.date >= %s 
                AND soi.STATUS = 'A' 
            ORDER BY
                so.date DESC
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
    
    def InsertDataStockOpnameItems(stockOpnameItems):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_stock_opname_items" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_stock_opname_items"
        query = f"""
            INSERT INTO {table_name} (
                store_id,
                opname_id,
                product_id,
                product_variant_id,
                qty,
                qty_sys,
                date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            # Memulai transaksi
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam stockOpnameItems
                for item in stockOpnameItems:
                    cursor.execute(query, (
                        item[0],  # store_id
                        item[1],  # opname_id
                        item[2],  # product_id
                        item[3],  # product_variant_id
                        item[4],  # qty
                        item[5],  # qty_sys
                        item[6]   # date
                    ))

                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_stock_opname_items.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting stock opname items: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
