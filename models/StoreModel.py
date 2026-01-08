from config.database import get_db_connection

class Store:
    
    @staticmethod
    def GetIndexData():
        conn_generator = None
        conn = None
        # Mendapatkan koneksi database dari fungsi get_db_connection
        conn_generator = get_db_connection()
        conn = next(conn_generator, None)  # Gunakan None sebagai default jika tidak ada koneksi

        if conn is None:
            return None  # Tidak melakukan apa-apa jika koneksi adalah None
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                       SELECT
                            id,
                            is_datainsight 
                        FROM
                            ci_store_ext_settings 
                        WHERE
                            is_datainsight=1
                        """)
            data = cursor.fetchall()
            cursor.close()
            return data
        except Exception as error:
            raise error
        finally:
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
    
    def FindStore(storeid):
        conn_generator = None
        conn = None
        # Mendapatkan koneksi database dari fungsi get_db_connection
        conn_generator = get_db_connection()
        conn = next(conn_generator, None)  # Gunakan None sebagai default jika tidak ada koneksi

        if conn is None:
            return None  # Tidak melakukan apa-apa jika koneksi adalah None
        
        try:
            cursor = conn.cursor()
            query = """
                SELECT 
                    id 
                FROM ci_store
                WHERE
                    id = %s
            """
            params = (storeid,)
            
            # try:
            #     with conn.cursor() as cursor:
            #         cursor.execute(query, params)
            #         data = cursor.fetchone()  # Use fetchone to get a single row
            #         return data  # data will be None if no result is found
            # finally:
            #     conn.close()
            cursor.execute(query, params)
            data = cursor.fetchone()
            cursor.close()
            return data
        
        except Exception as e:
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
