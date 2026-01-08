
from config.database import get_db_connection,get_db_connection_third
from typing import Any
import os
from dotenv import load_dotenv

load_dotenv('.env')

class StockInOutItems:
    @staticmethod
    def GetDataStockInOutItems(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT
                ci_store.id as store_id,
                ioi.in_out_id,
                ioi.product_id,
                ioi.product_variant_id,
                ioi.qty,
                ioi.new_buy_price,
                ioi.last_buy_price,
                ioi.last_qty,
                ioi.avg_buy_price,
                ci_store_stock_in_out.date
            FROM
                ci_store_stock_in_out_items AS ioi
                LEFT JOIN ci_store_stock_in_out_ext ON ci_store_stock_in_out_ext.in_out_id = ioi.in_out_id
                INNER JOIN ci_store_stock_in_out ON ci_store_stock_in_out.id = ci_store_stock_in_out_ext.in_out_id
                INNER JOIN ci_store ON ci_store.id = ci_store_stock_in_out.store_id 
            WHERE
                ci_store_stock_in_out.type = 'O' 
                AND ci_store_stock_in_out_ext.record_to_report_profit_loss = 1 
                AND ci_store_stock_in_out.STATUS = 'P' 
                AND ci_store_stock_in_out.store_id = %s 
                AND ci_store_stock_in_out.date >= %s
                AND ci_store_stock_in_out_ext.ref_id IS NULL 
                AND ioi.STATUS = 'A'
                ORDER BY ci_store_stock_in_out.date desc
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

    def InsertDataStockInOutItems(stockInOutItems):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_stock_in_out_items" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_stock_in_out_items"
        query = f"""
            INSERT INTO {table_name} (
                store_id,
                in_out_id,
                product_id,
                product_variant_id,
                qty,
                new_buy_price,
                last_buy_price,
                last_qty,
                avg_buy_price,
                date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            # Memulai transaksi
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam stockInOutItems
                for item in stockInOutItems:
                    cursor.execute(query, (
                        item[0],  # store_id
                        item[1],  # in_out_id
                        item[2],  # product_id
                        item[3],  # product_variant_id
                        item[4],  # qty
                        item[5],  # new_buy_price
                        item[6],  # last_buy_price
                        item[7],  # last_qty
                        item[8],  # avg_buy_price
                        item[9]   # date
                    ))

                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_stock_in_out_items.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting stock in/out items: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()
