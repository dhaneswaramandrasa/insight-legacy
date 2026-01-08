from config.s3 import get_s3_client
from typing import Any
from botocore.exceptions import ClientError
import os, json
import time
import hashlib
import base64

class S3_utils():

    @staticmethod
    def calculate_md5(file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return base64.b64encode(hash_md5.digest()).decode('utf-8')

    @staticmethod
    def uploadToS3(fileName: Any = None, aws_bucket: Any = None, directory: Any = None):
        try:
            # Cek apakah file ada
            if not os.path.isfile(fileName):
                print(f"File {fileName} tidak ditemukan.")
                return False

            # Panggil s3 client
            s3_client = get_s3_client()
            fullPath = f"{directory}/{os.path.basename(fileName)}"

            # Hitung MD5 secara manual
            md5_checksum = S3_utils.calculate_md5(fileName)

            # Baca file dan dapatkan data biner
            with open(fileName, "rb") as file_data:
                file_content = file_data.read()

            # Kirim/upload file dengan put_object
            s3_client.put_object(
                Bucket=aws_bucket,
                Key=fullPath,
                Body=file_content,
                ContentMD5=md5_checksum
            )
            print(f"File {fileName} berhasil diunggah ke bucket {aws_bucket}")

            # Tunggu 5 detik sebelum menghapus file lokal
            time.sleep(5)
            # Cek kembali apakah file masih ada sebelum dihapus
            if os.path.isfile(fileName):
                try:
                    os.remove(fileName)
                    print(f"File {fileName} berhasil dihapus dari lokal setelah diunggah")
                except OSError as e:
                    print(f"Gagal menghapus file {fileName}: {e}")
            else:
                print(f"File {fileName} tidak ditemukan di lokal setelah diunggah")

            return True

        except ClientError as e:
            print(f"Terjadi kesalahan saat mengunggah file ke S3: {e}")
            return False
        except Exception as error:
            print(f"Terjadi kesalahan tidak terduga: {error}")
            raise error

    @staticmethod
    def downloadFromS3(fileName: Any = None, aws_bucket: Any = None, directory: Any = None):
        try:
            s3_client = get_s3_client()
            s3_client.download_file(aws_bucket, fileName, directory)
            print(f"File {fileName} berhasil diunduh dari bucket {aws_bucket} ke {directory}")
        except ClientError as e:
            raise e
        except Exception as e:
            raise e


    @staticmethod
    def ensure_directory_exists(aws_bucket: str, directory: str) -> bool:
        """
        Memastikan directory/prefix ada di S3
        Returns:
            bool: True jika directory sudah ada atau berhasil dibuat, False jika gagal
        """
        s3_client = get_s3_client()
        
        try:
            # Normalisasi path directory
            directory = directory.rstrip('/') + '/'  # Pastikan diakhiri dengan slash
            
            # Cek apakah directory ada dengan head_object
            s3_client.head_object(Bucket=aws_bucket, Key=directory)
            print(f"Directory '{directory}' sudah ada di S3")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Directory '{directory}' tidak ditemukan, membuat baru...")
                try:
                    # Membuat directory dengan empty object
                    s3_client.put_object(
                        Bucket=aws_bucket,
                        Key=directory,
                        Body=b'',
                        ContentType='application/x-directory'
                    )
                    print(f"Directory '{directory}' berhasil dibuat")
                    return True
                except ClientError as ce:
                    print(f"Gagal membuat directory: {ce}")
                    return False
            else:
                print(f"Error saat mengecek directory: {e}")
                return False
        except Exception as ex:
            print(f"Unexpected error: {ex}")
            return False
        
    @staticmethod
    def readFileJsonFromS3(fileName, aws_bucket, directory):
        try:
            s3_client = get_s3_client()
            file_path = f"{directory}/{fileName}"  # Path lengkap di S3

            # Mengambil objek dari S3
            response = s3_client.get_object(Bucket=aws_bucket, Key=file_path)

            # Membaca isi file dan menguraikan JSON
            file_content = response["Body"].read().decode("utf-8")
            json_data = json.loads(file_content)

            return json_data  # Mengembalikan data JSON sebagai dictionary/list

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                print(f"File {file_path} tidak ditemukan di S3.")
                return None
            else:
                print(f"Terjadi kesalahan saat membaca file dari S3: {e}")
                raise e
        except json.JSONDecodeError:
            print(f"File {file_path} bukan JSON yang valid.")
            return None
        except Exception as e:
            print(f"Terjadi kesalahan tidak terduga: {e}")
            raise e
        
    @staticmethod
    def checkFileExists(aws_bucket: str, file_path: str) -> bool:
        """
        Mengecek apakah file ada di dalam bucket S3.
        
        Args:
            aws_bucket (str): Nama bucket S3.
            file_path (str): Path lengkap file di dalam bucket.
        
        Returns:
            bool: True jika file ada, False jika tidak.
        """
        s3_client = get_s3_client()  # Menggunakan client yang sudah dikonfigurasi
        try:
            s3_client.head_object(Bucket=aws_bucket, Key=file_path)
            return True  # File ditemukan
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False  # File tidak ditemukan
            else:
                print(f"Terjadi kesalahan saat mengecek file: {e}")
                raise e  # Jika error lain, tetap lempar exception
