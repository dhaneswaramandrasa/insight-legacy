from config.database import get_db_connection_second

class StoreExtSettings:
    
    def activation(store_id, activation_date, last_date_sync):    
        # Jalankan query UPDATE
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = """
            UPDATE ci_store_ext_settings
            SET is_datainsight=1, 
                activation_date_datainsight = %s, 
                flag_first_sync_datainsight = 1, 
                last_date_sync_datainsight = %s
            WHERE id = %s 
            AND is_datainsight = 0
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, (activation_date, last_date_sync, store_id))
                # Commit perubahan ke database
                conn.commit()
                print(f"ID {store_id} berhasil diperbarui menjadi is_datainsight=1.")
                return True

        except Exception as e:
            # conn.rollback()
            print(f"Error saat memperbarui data: {e}")
            raise e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()

    def checkActivation(store_id):
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = """
            SELECT
                id,
                is_datainsight,
                activation_date_datainsight
            FROM
                ci_store_ext_settings 
            WHERE
                id = %s;
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (store_id,))
                result = cursor.fetchone()  # Mengambil satu baris hasil query
                if result:
                    return result
                else:
                    # print(f"No data found for store_id: {store_id}")
                    return None
        except Exception as e:
            print(f"Error checking activation: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()

    @staticmethod
    def GetStoreDataInsight():
        conn_generator = None
        conn = None
        # Mendapatkan koneksi database dari fungsi get_db_connection
        conn_generator = get_db_connection_second()
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
                        where is_datainsight=1
                        """)
            data = cursor.fetchall()
            cursor.close()
            return data
        except Exception as error:
            raise error
        finally:
            if conn and conn.is_connected():
                conn.close()  # Menutup koneksi
                
    @staticmethod
    def UpdateLastSync(last_date_sync, store_id):    
        # Jalankan query UPDATE
        conn_generator = get_db_connection_second()
        conn = next(conn_generator)
        query = """
            UPDATE ci_store_ext_settings
            SET last_date_sync_datainsight = %s
            WHERE id = %s 
            AND is_datainsight = 1
        """
        try:
            # Memulai transaksi
            # conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(query, (last_date_sync,store_id))
                # Commit perubahan ke database
                conn.commit()
                print(f"ID {store_id} berhasil diperbarui menjadi is_datainsight=1.")
                return True

        except Exception as e:
            # conn.rollback()
            print(f"Error saat memperbarui data: {e}")
            raise e
        finally:
            # Tutup koneksi
            if conn and conn.is_connected():
                conn.close()


