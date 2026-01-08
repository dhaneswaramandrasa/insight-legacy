import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
import os

# Muat variabel lingkungan dari file .env
load_dotenv('.env')

class Database:
    connection_pool = None
    connection_pool_second = None  # Tambahkan pool koneksi kedua
    connection_pool_third = None


    @classmethod
    def initialize(cls):
        if cls.connection_pool is None:
            try:
                cls.connection_pool = pooling.MySQLConnectionPool(
                    pool_name="my_pool",
                    pool_size=10,  # Ukuran pool ditingkatkan menjadi 20
                    pool_reset_session=True,
                    host=os.getenv('HOST_DB'),
                    user=os.getenv('USER_DB'),
                    password=os.getenv('PASS_DB'),
                    database=os.getenv('NAME_DB')
                )
                print("Connection pool created successfully")
            except mysql.connector.Error as err:
                print(f"Error initializing connection pool: {err}")
                cls.connection_pool = None

        if cls.connection_pool_second is None:  # Inisialisasi pool koneksi kedua
            try:
                cls.connection_pool_second = pooling.MySQLConnectionPool(
                    pool_name="my_pool_second",
                    pool_size=10,
                    pool_reset_session=True,
                    host=os.getenv('SECOND_HOST_DB'),
                    user=os.getenv('SECOND_USER_DB'),
                    password=os.getenv('SECOND_PASS_DB'),
                    database=os.getenv('SECOND_NAME_DB')
                )
                print("Second connection pool created successfully")
            except mysql.connector.Error as err:
                print(f"Error initializing second connection pool: {err}")
                cls.connection_pool_second = None

        if cls.connection_pool_third is None:  # Inisialisasi pool koneksi kedua
            try:
                cls.connection_pool_third = pooling.MySQLConnectionPool(
                    pool_name="my_pool_second",
                    pool_size=10,
                    pool_reset_session=True,
                    host=os.getenv('THIRD_HOST_DB'),
                    user=os.getenv('THIRD_USER_DB'),
                    password=os.getenv('THIRD_PASS_DB'),
                    database=os.getenv('THIRD_NAME_DB')
                )
                print("third connection pool created successfully")
            except mysql.connector.Error as err:
                print(f"Error initializing second connection pool: {err}")
                cls.connection_pool_third = None

    @classmethod
    def get_connection(cls):
        cls.initialize()
        if cls.connection_pool:
            try:
                connection = cls.connection_pool.get_connection()
                if connection and connection.is_connected():
                    print("Connection established")
                    return connection
                else:
                    print("Connection failed to establish")
                    return None
            except mysql.connector.Error as err:
                print(f"Error getting connection: {err}")
                return None
        print("Connection pool is not initialized or connection failed")
        return None
    
    @classmethod
    def get_second_connection(cls):  # Metode untuk koneksi kedua
        cls.initialize()
        if cls.connection_pool_second:
            try:
                connection = cls.connection_pool_second.get_connection()
                if connection and connection.is_connected():
                    print("Second connection established")
                    return connection
                else:
                    print("Second connection failed to establish")
                    return None
            except mysql.connector.Error as err:
                print(f"Error getting second connection: {err}")
                return None
        print("Second connection pool is not initialized or connection failed")
        return None
    
    @classmethod
    def get_third_connection(cls):  # Metode untuk koneksi kedua
        cls.initialize()
        if cls.connection_pool_third:
            try:
                connection = cls.connection_pool_third.get_connection()
                if connection and connection.is_connected():
                    print("Second connection established")
                    return connection
                else:
                    print("Second connection failed to establish")
                    return None
            except mysql.connector.Error as err:
                print(f"Error getting second connection: {err}")
                return None
        print("Second connection pool is not initialized or connection failed")
        return None

def get_db_connection():
    connection = Database.get_connection()
    if connection and connection.is_connected():
        # try:
        yield connection
        # finally:
        #     try:
        #         if connection.is_connected():
        #             connection.close()
        #             print("Connection closed successfully")
        #         else:
        #             print("Connection was not open")
        #     except mysql.connector.Error as err:
        #         print(f"Error closing connection: {err}")
    else:
        print("No connection available")
        yield None

def get_db_connection_second():
    connection = Database.get_second_connection()
    if connection and connection.is_connected():
        yield connection
    else:
        print("No second connection available")
        yield None

def get_db_connection_third():
    connection = Database.get_third_connection()
    if connection and connection.is_connected():
        yield connection
    else:
        print("No second connection available")
        yield None

# Untuk keperluan pengujian
if __name__ == "__main__":
    try:
        with get_db_connection() as connection:
            if connection:
                print("Test connection successful")
            else:
                print("Test connection failed")
    except Exception as e:
        print(f"Test connection exception: {e}")
