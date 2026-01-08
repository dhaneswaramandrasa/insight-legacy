
from config.database import get_db_connection,get_db_connection_third
from typing import Any
import os
from dotenv import load_dotenv

load_dotenv('.env')
# import logging

class InexTrans:
    @staticmethod
    def GetDataInexTrans(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
           SELECT
                ci_store_inex_trans.id,
                ci_store_inex_trans.trans_no,
                ci_store_inex_trans.amount,
                ci_store_inex_trans.store_id,
                ci_store_inex_trans.trans_date,
                ci_store_inex_trans.status,
                itt.STATUS AS status_trans_type 
            FROM
                ci_store_inex_trans
                LEFT JOIN ci_store_inex_trans_type AS itt ON itt.id = ci_store_inex_trans.trans_type_id 
            WHERE
                ci_store_inex_trans.store_id = %s 
                AND ci_store_inex_trans.trans_date >= %s
            ORDER BY
                ci_store_inex_trans.trans_date DESC
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

    def InsertDataInexTrans(inexTransactions):
        conn_generator = get_db_connection_third()
        conn = next(conn_generator)
        table_name = "staging_olsera_datainsight_inex_trans" if os.getenv("RELEASE_STAGE") == "Staging" else "olsera_datainsight_inex_trans"
        query = f"""
            INSERT INTO {table_name} (
                id,
                trans_no,
                amount,
                store_id,
                trans_date,
                status,
                status_trans_type
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            # Memulai transaksi
            with conn.cursor() as cursor:
                # Looping untuk setiap baris data dalam inexTransactions
                for transaction in inexTransactions:
                    cursor.execute(query, (
                        transaction[0],  # id
                        transaction[1],  # trans_no
                        transaction[2],  # amount
                        transaction[3],  # store_id
                        transaction[4],  # trans_date
                        transaction[5],  # status
                        transaction[6]   # status_trans_type
                    ))

                # Commit perubahan ke database
                conn.commit()
                print("Data successfully inserted into olsera_datainsight_inex_trans.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error inserting inex transactions: {e}")
            return e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()