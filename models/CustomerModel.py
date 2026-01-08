
from config.database import get_db_connection
from typing import Any

class Customer:
    
    @staticmethod
    def GetListBuyer(store_id: Any = 0, date: str = None):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
           SELECT
                sc.id,
                sc.store_id,
                sc.NAME,
                CONCAT('https://d1d8o7q9jg8pjk.cloudfront.net/ca/',ca.img) as avatar,
                sc.phone,
                count(sso.id) as total_trx,
                sum(sso.total_amount) as total_spending,
                sso.order_time
            FROM
                ci_store_customer AS sc
            LEFT JOIN ci_store_customer_ext as sce on sce.id = sc.id
            LEFT JOIN ci_cd_customer_avatar as ca on ca.id = sce.avatar_id
            LEFT JOIN ci_store_sales_order as sso on sso.customer_id = sc.id
            WHERE
                sc.store_id = %s
            AND sso.is_paid=1
            AND YEAR(sso.order_date) = %s
            GROUP BY sc.id
            ORDER BY total_trx desc
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
