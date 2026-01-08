
from config.database import get_db_connection
from typing import Any
# import logging

class ProductVariantMaterials:
    @staticmethod
    def GetDataProductVariantMaterials(store_id: Any = 0):
        conn_generator = get_db_connection()
        conn = next(conn_generator)
      
        query = """
            SELECT 
                ci_store_product_materials.id,
                ci_store_product_materials.product_id,
                ci_store_product_materials.product_variant_id,
                ci_store_product_materials.material_product_id,
                ci_store_product_materials.material_product_variant_id,
                ci_store_product_materials.qty,
                ci_store_product_materials.uom,
                ci_store_product_materials.uom_conversion
            FROM 
                ci_store_product_materials
            INNER JOIN 
                ci_store_product ON ci_store_product_materials.material_product_id = ci_store_product.id
            INNER JOIN ci_store_product_variants as spv on spv.id = ci_store_product_materials.product_variant_id
            WHERE 
                ci_store_product.store_id= %s
            ORDER BY 
                ci_store_product_materials.id ASC
        """
        # params = (store_id,date, store_id, date)
        params = (store_id,)
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