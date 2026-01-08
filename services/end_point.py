import time, os,sys
import datetime as dt
import pandas as pd
import calendar
import numpy as np

from fastapi import HTTPException
from utilities.upload.S3_utils import S3_utils
from utilities.utils import *
from config.s3 import download_file_from_s3
from config.bugsnag import get_logger
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from config.s3 import download_json_file_from_s3
from sqlalchemy import text

load_dotenv('.env')
logger = get_logger()

import config.olsera_replica_config as olsera_replica_config
import config.olsera_analytic_config as olsera_analytic_config

class EndPoint:
    directory = "data-lake-python-feather"

    @staticmethod
    def GetSales(store_id: int, start_date: str, end_date: str):
        start_date_obj = validate_date(start_date)
        end_date_obj = validate_date(end_date)
        n_days = (end_date_obj - start_date_obj).days + 1

        start_date_comp, end_date_comp = get_date_comparison(start_date_obj, end_date_obj)

        start_penarikan = start_date_comp.strftime("%Y")
        end_penarikan = end_date_obj.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        # Penarikan Summary
        path_summary = f"{EndPoint.directory}/summary-sales-vision/{store_id}_summary_sales_vision.feather"
        df_summary = download_file_from_s3(path_summary)
        if df_summary.empty:
            total_sales_all_time = 0.0
        else:
            df_summary["total_amount"].fillna(0, inplace=True)
            total_sales_all_time = float(df_summary.loc[df_summary["flag"] == 0, "total_amount"].sum()) - float(df_summary.loc[df_summary["flag"] == 1, "total_amount"].sum())
        
        
        empty_data_response = {
            "data": {
                "this_period_total": 0,
                "all_time_total": total_sales_all_time,
                "growth_percentage": 0.0,
                "higher_total_by_date": 0,
                "lower_total_by_date": 0,
                "mean_total_by_date": 0.0,
                "graph_data_by_date": pd.DataFrame({"order_date": [], "total": []}).to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }

        # Download Data Sales
        dfs = []
        for date in dates:
            path_sales = f"{EndPoint.directory}/sales-tracking-vision/{store_id}_{date}_sales_tracking_vision.feather"
            df_sales = download_file_from_s3(path_sales)
            dfs.append(df_sales)
        
        df_sales = pd.concat(dfs)

        if df_sales.empty:
            return empty_data_response

        df_sales = df_sales.groupby(["date_trx_days", "flag"], as_index=False)["total_amount"].sum()
        df_sales["total_amount"] = df_sales["total_amount"].fillna(0)

        conn = olsera_replica_config.get_sqlalchemy_engine()
        deposit_query = text("""
        SELECT
            ci_store_ext_settings.is_deposit_active
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = :store_id
        """)
        
        try:
            with conn.connection() as connection:
                is_deposit_activated = pd.read_sql(deposit_query, con=connection, params={"store_id": store_id}) 
                is_deposit_activated = is_deposit_activated.iloc[0, 0]
        except:
            is_deposit_activated = 1
        
        conn.dispose()

        # Check Sales Deposit
        dfs = []
        if is_deposit_activated == 0:
            for date in dates:
                path_deposit = f"{EndPoint.directory}/sales-deposit-tracking-vision/{store_id}_{date}_deposit_tracking_vision.feather"
                df_deposit = download_file_from_s3(path_deposit)
                dfs.append(df_deposit)
        
        # Ubah data string ke datetime type
        df_sales["date_trx_days"] = pd.to_datetime(df_sales["date_trx_days"], format="%Y-%m-%d")
        min_date = df_sales["date_trx_days"].min()
        max_date = df_sales["date_trx_days"].max()
        is_possible_to_compare = min_date <= start_date_comp
        is_possible_to_visualize = start_date_obj <= max_date
        
        if is_possible_to_visualize:
            # Data untuk Grafik filter date
            df_sales_graph = df_sales.loc[(df_sales["date_trx_days"] >= start_date_obj) & (df_sales["date_trx_days"] <= end_date_obj), :]
            
            if df_sales_graph.empty:
                return empty_data_response

            sales = df_sales_graph.loc[df_sales_graph["flag"] == 0, :].rename(columns={'total_amount': 'total_sales'})
            sales.drop("flag", axis=1, inplace=True)

            refunds = df_sales_graph.loc[df_sales_graph['flag'] == 1, ["total_amount", "date_trx_days"]].rename(columns={'total_amount': 'total_refund'})

            if refunds.empty:
                merged = sales.copy()
                merged["total"] = merged["total_sales"]
            else:
                # Merge sales and refunds on id and date
                merged = pd.merge(sales, refunds, on=['date_trx_days'], how='outer')

                # Fill NaN values with 0
                merged['total_sales'].fillna(0, inplace=True)
                merged['total_refund'].fillna(0, inplace=True)
                merged["total"] = merged["total_sales"] - merged["total_refund"]

            # Mengurangi Data dengan Deposit
            if dfs:
                df_deposit = pd.concat(dfs)
                df_deposit.transaction_date_deposit = pd.to_datetime(df_deposit.transaction_date_deposit)
                df_deposit = df_deposit[(df_deposit.transaction_date_deposit >= start_date_obj) & (df_deposit.transaction_date_deposit <= end_date_obj)]

                if not(df_deposit.empty):
                    df_deposit.rename(columns={"transaction_date_deposit": "date_trx_days"}, inplace=True)
                    df_deposit["total_deposit"] = df_deposit["total_deposit"].fillna(0).astype(float)
                    df_deposit = df_deposit.groupby("date_trx_days", as_index=False)["total_deposit"].sum()
                    
                    merged = merged.merge(df_deposit, on=["date_trx_days"], how="outer")
                    merged["total_deposit"] = merged["total_deposit"].fillna(0).astype(float)
                    merged["total"] = merged["total"].fillna(0).astype(float)
                    merged["total"] = merged["total"] - merged["total_deposit"]
                    merged.drop("total_deposit", axis=1, inplace=True)

            total_sales_this_period = float(merged["total"].sum())
            total_store_close = float((merged["total"] <= 0.0).sum())
            
            merged.total = pd.to_numeric(merged.total, errors="coerce")
            merged = merged.dropna(subset=["total"])
            idx_max_sales = merged["total"].idxmax()
            max_sales = float(merged.loc[idx_max_sales, "total"])
            max_sales_date = str(merged.loc[idx_max_sales, "date_trx_days"].strftime("%Y-%m-%d"))

            filtered = merged[merged["total"] > 0.0]
            idx_min_sales = filtered["total"].idxmin()
            min_sales = float(filtered.loc[idx_min_sales, "total"])
            min_sales_date = str(filtered.loc[idx_min_sales, "date_trx_days"].strftime("%Y-%m-%d"))
            mean_sales = total_sales_this_period / float(n_days - total_store_close)

            # DataFrame untuk Perbandingan dengan periode sebelumnya
            if is_possible_to_compare:
                df_comparison = df_sales.loc[(df_sales["date_trx_days"] >= start_date_comp) & (df_sales["date_trx_days"] <= end_date_comp), :]
                total_period_before = float(df_comparison.loc[df_comparison["flag"] == 0, "total_amount"].sum()) - float(df_comparison.loc[df_comparison["flag"] == 1, "total_amount"].sum())
                try:
                    growth_rate = float((total_sales_this_period - total_period_before) / total_period_before * 100)
                except:
                    growth_rate = float(100.0)
            else:
                growth_rate = float(100.0)
            
            merged = merged.loc[:, ["date_trx_days", "total"]]
            merged["date_trx_days"] = merged["date_trx_days"].dt.strftime("%Y-%m-%d")
            merged.rename(columns={"date_trx_days": "order_date"}, inplace=True)
            sales_json_data = {
                "data": {
                    "this_period_total": total_sales_this_period,
                    "all_time_total": total_sales_all_time,
                    "growth_percentage": growth_rate,
                    "highest_sales_by_date": max_sales,
                    "lowest_sales_by_date": min_sales,
                    "lowest_sales_date": min_sales_date,
                    "highest_sales_date": max_sales_date,
                    "mean_total_by_date": mean_sales,
                    "graph_data_by_date": merged.to_dict(orient='records')
                },
                "status": 200,
                "error": 0
            }
            return sales_json_data

        else:
            return empty_data_response
    
    @staticmethod
    def GetProduct(store_id: int, start_date: str, end_date:str):
        now_wib = dt.datetime.utcnow() - dt.timedelta(hours=7)
        now_wib = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)

        this_year, this_month = now_wib.year, now_wib.month
        first_year_date = dt.datetime(this_year, 1, 1)

        first_month = now_wib.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        start_date_obj = validate_date(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_obj = validate_date(end_date).replace(hour=0, minute=0, second=0, microsecond=0)

        difference = relativedelta(end_date_obj, start_date_obj)
        n_months = difference.years * 12 + difference.months + 1
        n_days = (end_date_obj - start_date_obj).days + 1

        start_date_comp, end_date_comp = get_date_comparison(start_date_obj, end_date_obj)
        start_date_comp = start_date_comp.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_comp = end_date_comp.replace(hour=0, minute=0, second=0, microsecond=0)

        start_penarikan = start_date_comp.strftime("%Y")
        end_penarikan = end_date_obj.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        # Download Data Sales
        dfs = []
        for date in dates:
            try:
                path_product = f"{EndPoint.directory}/product-populer-vision/{store_id}_{date}_product_populer_vision.feather"
                df_product = download_file_from_s3(path_product)
                dfs.append(df_product)
            except:
                continue
        
        df_product = pd.concat(dfs)
        
        response = {
            "data": {
                "this_period_total": 0,
                "all_time_total": 0,
                "higher_total_by_date": 0,
                "growth_percentage": 0.0,
                "lower_total_by_date": 0,
                "mean_total_by_date": 0.0,
                "top5_product_id": [],
                "top5_product_qty": [],
                "graph_data_by_date": pd.DataFrame({"order_date": [], "total": []}).to_dict(orient="records")
            },
            "status": 200,
            "error": 0
        }

        if df_product.empty:
            return response
        
        conn = olsera_replica_config.get_sqlalchemy_engine()
        deposit_query = text("""
        SELECT
            ci_store_ext_settings.is_deposit_active
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = :store_id
        """)
        
        try:
            with conn.connect() as connection:
                is_deposit_activated = pd.read_sql(deposit_query, con=connection, params={"store_id": store_id}) 
                is_deposit_activated = is_deposit_activated.iloc[0, 0]
        except:
            is_deposit_activated = 1

        conn.dispose()
        # Penarikan Summary
        try:
            path_summary = f"{EndPoint.directory}/summary-sales-product-vision/{store_id}_summary_sales_product_vision.feather"
            df_summary = download_file_from_s3(path_summary)
            if is_deposit_activated == 0:
                total_deposit_all_time = float(df_summary.loc[df_summary["flag"] == 1, "total_qty"].sum())
                if ~np.isfinite(total_deposit_all_time):
                    total_deposit_all_time = 0.0
            else:
                total_deposit_all_time = 0.0
        except:
            total_deposit_all_time = 0.0
        

        df_product["date_trx_days"] = pd.to_datetime(df_product["date_trx_days"])
        df_product["total_qty"] = df_product["total_qty"].fillna(0).astype(float)
        df_product["total_qty_return"] =  df_product["total_qty_return"].fillna(0).astype(float)
        df_product["total_qty_unpaid"] = df_product["total_qty_unpaid"].fillna(0).astype(float)
        df_product["total_qty"] = df_product["total_qty"] - df_product["total_qty_return"] - df_product["total_qty_unpaid"]
        index_filter = (df_product["total_qty"] == 0)
        df_product = df_product[~index_filter]

        year_filter = (df_product["date_trx_days"] >= first_year_date) & (df_product["date_trx_days"] <= end_date_obj)
        total_product_sold_all_time = float(df_product.loc[year_filter, "total_qty"].sum()) - total_deposit_all_time
        
        df_product = df_product.loc[:, ["product_id", "product_variant_id", "total_qty", "date_trx_days"]]
        
        df_product["date_trx_days"] = pd.to_datetime(df_product["date_trx_days"])
        min_date = df_product["date_trx_days"].min()
        max_date = df_product["date_trx_days"].max()
        is_possible_to_compare = min_date <= start_date_comp
        is_possible_to_visualize = start_date_obj <= max_date

        response["data"]["all_time_total"] = total_product_sold_all_time

        if is_possible_to_visualize:
            # Data untuk Grafik filter date
            df_product_graph = df_product[(df_product["date_trx_days"] >= start_date_obj) & (df_product["date_trx_days"] <= end_date_obj)]
            
            if df_product_graph.empty:
                return response

            df_product_graph = df_product_graph.groupby("date_trx_days", as_index=False)["total_qty"].sum()
            total_product_this_period = float(df_product_graph["total_qty"].sum())
            total_close = float((df_product_graph["total_qty"] <= 0.0).sum())

            df_product_graph["total_qty"] = pd.to_numeric(df_product_graph["total_qty"], errors="coerce")
            df_product_graph = df_product_graph.dropna(subset=["total_qty"])

            idx_max_product = df_product_graph["total_qty"].idxmax()
            max_product = float(df_product_graph.loc[idx_max_product, "total_qty"])
            max_product_date = str(df_product_graph.loc[idx_max_product, "date_trx_days"].strftime("%Y-%m-%d"))

            filtered = df_product_graph[df_product_graph["total_qty"] > 0]
            idx_min_product = filtered["total_qty"].idxmin()
            min_product = float(filtered.loc[idx_min_product, "total_qty"])
            min_product_date = str(filtered.loc[idx_min_product, "date_trx_days"].strftime("%Y-%m-%d"))

            mean_product = total_product_this_period / float(n_days - total_close)

            # DataFrame untuk Perbandingan
            if is_possible_to_compare:
                df_product_comparison = df_product.loc[(df_product["date_trx_days"] >= start_date_comp) & (df_product["date_trx_days"] <= end_date_comp), :]  
                total_period_product_before = float(df_product_comparison["total_qty"].sum())

                try:
                    growth_product = (total_product_this_period - total_period_product_before) / (total_period_product_before) * 100
                    growth_product = float(growth_product)
                except:
                    growth_product = float(100.0)
            else:
                growth_product = float(100.0)
            
            df_product_graph = df_product_graph.loc[:, ["date_trx_days", "total_qty"]]
            df_product_graph["date_trx_days"] = df_product_graph["date_trx_days"].dt.strftime("%Y-%m-%d")

            df_product_graph.rename(columns={"date_trx_days": "order_date", "total_qty": "total"}, inplace=True)
            df_product_graph["total"] = pd.to_numeric(df_product_graph["total"], errors="coerce")
            df_product_graph = df_product_graph.dropna(subset=["total"])

            df_product = df_product.loc[(df_product["date_trx_days"] >= start_date_obj) & (df_product["date_trx_days"] <= end_date_obj), :]
            df_product["product_id"] = df_product["product_id"].fillna(0)
            df_product["product_variant_id"] = df_product["product_variant_id"].fillna(0)
            df_product["product_id"] = df_product["product_id"].astype(str) + "-" + df_product["product_variant_id"].astype(str)
            
            top5 = df_product.groupby(["product_id"], as_index=False)["total_qty"].sum()
            top5 = top5.sort_values("total_qty", ascending=False).head(5)
            top5_product_id = top5["product_id"].astype(str).values.tolist()
            top5_product_qty  = top5["total_qty"].astype(float).values.tolist()
            
            product_json_data = {
                "data": {
                    "this_period_total": total_product_this_period,
                    "all_time_total": total_product_sold_all_time,
                    
                    "highest_total_by_date": max_product,
                    "highest_total_date": max_product_date,
                    "growth_percentage": growth_product,
                    
                    "lowest_total_by_date": min_product,
                    "lowest_total_date": min_product_date,

                    "mean_total_by_date": mean_product,
                    "top5_product_id": top5_product_id,
                    "top5_product_qty": top5_product_qty,
                    "graph_data_by_date": df_product_graph.to_dict(orient="records")
                },
                "status": 200,
                "error": 0
            }
            return product_json_data
        else:
            return response
    
    @staticmethod
    def GetCustomer(store_id: int, start_date: str, end_date: str):
        start_date_obj = validate_date(start_date)
        end_date_obj = validate_date(end_date)
        n_days = (end_date_obj - start_date_obj).days + 1

        start_date_comp, end_date_comp = get_date_comparison(start_date_obj, end_date_obj)

        start_penarikan = start_date_comp.strftime("%Y")
        end_penarikan = end_date_obj.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        # Download Data Customer dari s3
        dfs = []
        for date in dates:
            path_cust = f"{EndPoint.directory}/sales-tracking-vision/{store_id}_{date}_sales_tracking_vision.feather"
            df_cust = download_file_from_s3(path_cust)
            dfs.append(df_cust)
        
        
        df_cust = pd.concat(dfs)
        empty_data_response = {
            "data": {
                "this_period_total": 0,
                "all_time_total": 0.0,
                "growth_percentage": 0,
                "higher_total_by_date": 0,
                "lower_total_by_date": 0,
                "mean_total_by_date": 0,
                "graph_data_by_date": pd.DataFrame({"order_date":[], "total": []}).to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }

        if df_cust.empty:
            return empty_data_response

        # Download Summary
        try:
            path_summary = f"{EndPoint.directory}/summary-sales-vision/{store_id}_summary_sales_vision.feather"
            df_summary = download_file_from_s3(path_summary)
            df_summary["all_trx_id_count"].fillna(0, inplace=True)
            total_cust_all_time = int(df_summary.loc[df_summary["flag"] == 0, "all_trx_id_count"].sum())
            total_cust_all_time -= int(df_summary.loc[df_summary["flag"] == 1, "all_trx_id_count"].sum())
        except:
            total_cust_all_time = 0
        
        empty_data_response["data"]["all_time_total"] = total_cust_all_time

        df_cust["trx_sales_id_count"].fillna(0, inplace=True)
        df_cust["date_trx_days"] = pd.to_datetime(df_cust["date_trx_days"], format="%Y-%m-%d")
        
        min_date = df_cust["date_trx_days"].min()
        max_date = df_cust["date_trx_days"].max()
        is_possible_to_compare = min_date <= start_date_comp
        is_possible_to_visualize = start_date_obj <= max_date

        if is_possible_to_visualize:

            # Data untuk Grafik filter date
            df_cust_graph = df_cust.loc[(df_cust["date_trx_days"] >= start_date_obj) & (df_cust["date_trx_days"] <= end_date_obj), :]

            if df_cust_graph.empty:
                return empty_data_response

            cust = df_cust_graph.loc[df_cust_graph["flag"] == 0, ["date_trx_days", "trx_sales_id_count"]].rename(columns={"trx_sales_id_count": "total_visitor"})
            cust["total_visitor"].fillna(0, inplace=True)   

            refund_cust = df_cust_graph.loc[df_cust_graph["flag"] == 1, ["date_trx_days", "trx_sales_id_count"]].rename(columns={"trx_sales_id_count": "total_refund_visitor"})
            refund_cust["total_refund_visitor"].fillna(0, inplace=True)

            cust_agg = cust.groupby(["date_trx_days"], as_index=False)["total_visitor"].sum()
            cust_refund_agg = refund_cust.groupby(["date_trx_days"], as_index=False)["total_refund_visitor"].sum()
            if cust_refund_agg.empty:
                merged = cust_agg.copy()
            else:
                merged = pd.merge(cust_agg, cust_refund_agg, on=["date_trx_days"], how="outer")
                merged["total_visitor"].fillna(0, inplace=True)
                merged["total_refund_visitor"].fillna(0, inplace=True)
                merged["total_visitor"] -= merged["total_refund_visitor"]

            # df_cust_graph = df_cust_graph.groupby("date_trx_days", as_index = False)["trx_sales_id_count"].sum()
            total_cust_this_period = int(merged["total_visitor"].sum())
            total_close = int((merged["total_visitor"] <= 0.0).sum())

            idx_max_cust = int(merged["total_visitor"].idxmax())
            max_cust = int(merged.loc[idx_max_cust, "total_visitor"])
            max_cust_date = str(merged.loc[idx_max_cust, "date_trx_days"])

            filtered = merged[merged["total_visitor"] > 0.0]
            idx_min_cust = int(filtered["total_visitor"].idxmin())
            min_cust = int(filtered.loc[idx_min_cust, "total_visitor"])
            min_cust_date = str(filtered.loc[idx_min_cust, "date_trx_days"].strftime("%Y-%m-%d"))

            mean_cust = float(merged["total_visitor"].sum()) / float(n_days-total_close)

            # DataFrame untuk Perbandingan
            if is_possible_to_compare:
                df_cust_comparison = df_cust.loc[(df_cust["date_trx_days"] >= start_date_comp) & (df_cust["date_trx_days"] <= end_date_comp), :]
                
                total_period_cust_before = int(df_cust_comparison.loc[df_cust_comparison["flag"] == 0, "trx_sales_id_count"].sum())
                # Kurangkan dengan Refund
                total_period_cust_before -= int(df_cust_comparison.loc[df_cust_comparison["flag"] == 1, "trx_sales_id_count"].sum())
                
                try:
                    growth_cust = (total_cust_this_period - total_period_cust_before) / (total_period_cust_before)
                    growth_cust = float(growth_cust)
                except:
                    growth_cust = float(100.0)
            else:
                growth_cust = float(100.0)
            
            merged = merged.loc[:, ["date_trx_days", "total_visitor"]]
            merged["date_trx_days"] = merged["date_trx_days"].dt.strftime("%Y-%m-%d")

            merged.rename(columns={"date_trx_days": "order_date", "total_visitor": "total"}, inplace=True)
            merged = merged.groupby("order_date", as_index=False)["total"].sum()

            cust_json_data = {
                "data": {
                    "this_period_total": total_cust_this_period,
                    "all_time_total": total_cust_all_time,
                    "growth_percentage": growth_cust,

                    "higher_total_by_date": max_cust,
                    "lower_total_by_date":min_cust,

                    "highest_total_date": max_cust_date,
                    "lowest_total_by_date": min_cust_date,
                    "mean_total_by_date": mean_cust,
                    "graph_data_by_date": merged.to_dict(orient='records')
                },
                "status": 200,
                "error": 0
            }
            return cust_json_data

        else:
            return empty_data_response
       
    @staticmethod
    def GetSalesML(store_id: int):
        now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        now_wib = now_utc + timedelta(hours=7) - timedelta(days=1)
        now_wib = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)
        today, this_month, this_year = now_wib.day, now_wib.month, now_wib.year

        this_month_beginning = now_wib.replace(day=1).replace(hour=0, minute=0, second=0, microsecond=0)
        this_month_end_day = calendar.monthrange(this_year, this_month)[1]
        this_month_end_date = this_month_beginning.replace(day=this_month_end_day, hour=0, minute=0, second=0, microsecond=0)
        
        last_month = this_month_beginning - timedelta(days=1)
        last_month_beginning = last_month.replace(day=1)
        last_month_end_day = calendar.monthrange(last_month.year, last_month.month)[1]
        last_month_end_date = last_month_beginning.replace(day=last_month_end_day)
        last_month_end_date = last_month_end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        if (today == 31 and last_month_end_day <= 30) or (today == 30 and last_month_end_day <= 29):
            n_diff = this_month_end_day - last_month_end_day
            last_month_to_today_date = last_month_beginning.replace(day=today-n_diff)
        else:
            last_month_to_today_date = last_month_beginning.replace(day=today)

        next_month = this_month_end_date + timedelta(days=1)
        next_month = next_month.replace(hour=0, minute=0, second=0, microsecond=0)
        next_month_end_day = calendar.monthrange(next_month.year, next_month.month)[1]
        next_month_end_date = next_month.replace(day=next_month_end_day, hour=0, minute=0, second=0, microsecond=0)

        query = text("""
            SELECT
                total_sales,
                sales_projection,
                sales_forecasting,
                transaction_date
            FROM olsera_datainsight_sales_projection_forecasting
            WHERE store_id = :store_id
            AND transaction_date BETWEEN :start_date AND :end_date
        """)
        
        analytic_conn = olsera_analytic_config.get_sqlalchemy_engine()
        with analytic_conn.connect() as connection:
            df = pd.read_sql(
                query, 
                con=connection,
                params={
                    "store_id": store_id, 
                    "start_date": last_month_beginning.strftime("%Y-%m-%d"),
                    "end_date": next_month_end_date.strftime("%Y-%m-%d")
                }
            )

        analytic_conn.dispose()
        empty_data_response = {
            "data": {
                "total_sales": 0,
                "total_sales_prediction": 0,
                "sales_growth_rate": 0.0,
                "sales_prediction_growth_rate": 0.0,

                "highest_sales_date": "0000-00-00",
                "highest_sales_total_by_date": 0,
                "lowest_sales_date": "0000-00-00",
                "lowest_sales_total_by_date": 0,

                "graph_sales_by_date": pd.DataFrame({"order_date": [], "total_sales": []}).to_dict(orient='records'),
                "graph_sales_projection_by_date": pd.DataFrame({"order_date": [], "total_projection": []}).to_dict(orient='records'),
                "graph_sales_forecasting_by_date": pd.DataFrame({"order_date": [], "total_forecasting": []}).to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }
        if df.empty:
            return empty_data_response
        
        
        start_penarikan = last_month_beginning.strftime("%Y")
        end_penarikan = next_month_end_date.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        conn = olsera_replica_config.get_sqlalchemy_engine()
        deposit_query = text("""
        SELECT
            ci_store_ext_settings.is_deposit_active
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = :store_id
        """)
        
        try:
            with conn.connection() as connection:
                is_deposit_activated = pd.read_sql(deposit_query, con=connection, params={"store_id": store_id}) 
                is_deposit_activated = is_deposit_activated.iloc[0, 0]
        except:
            is_deposit_activated = 1

        conn.dispose()
        dfs_deposit = []
        if is_deposit_activated == 0:
            for date in dates:
                path_deposit = f"{EndPoint.directory}/sales-deposit-tracking-vision/{store_id}_{date}_deposit_tracking_vision.feather"
                df_deposit = download_file_from_s3(path_deposit)
                dfs_deposit.append(df_deposit)
                

        # Mengurangi Data dengan Deposit
        if dfs_deposit:
            df_deposit = pd.concat(dfs_deposit)
            if not(df_deposit.empty):
                df_deposit.rename(columns={"transaction_date_deposit": "transaction_date"}, inplace=True)
                df_deposit.transaction_date = pd.to_datetime(df_deposit.transaction_date)
                df_deposit = df_deposit[(df_deposit.transaction_date >= last_month_beginning) & (df_deposit.transaction_date <= next_month_end_date)]
                df_deposit["total_deposit"] = df_deposit["total_deposit"].fillna(0)
                df_deposit = df_deposit.groupby("transaction_date", as_index=False)["total_deposit"].sum()
                
                df = df.merge(df_deposit, on=["transaction_date"], how="outer")
                df["total_deposit"] = df["total_deposit"].fillna(0).astype(float)
                df["total_sales"] = df["total_sales"] - df["total_deposit"]
                df.drop("total_deposit", axis=1, inplace=True)
                
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])

        beginning_month_to_today_date = (df["transaction_date"] >= this_month_beginning) & (df["transaction_date"] <= now_wib)
        last_month_per_date_filter = (df["transaction_date"] >= last_month_beginning) & (df["transaction_date"] <= last_month_to_today_date)
        
        # Total sales and its growth rate
        df_this_month = df.loc[beginning_month_to_today_date, ["total_sales", "transaction_date"]]
        total_sales_this_month = float(df_this_month.loc[:, "total_sales"].sum())

        df["total_sales"] = df["total_sales"].astype(float)
        df_graph_sales_by_date = df.loc[:, ["transaction_date", "total_sales"]]
        df_graph_sales_by_date.transaction_date = df_graph_sales_by_date.transaction_date.dt.strftime("%Y-%m-%d")

        df_graph_sales_projection_by_date = df.loc[:, ["transaction_date", "sales_projection"]]
        df_graph_sales_projection_by_date.sales_projection = df_graph_sales_projection_by_date.sales_projection.astype(float)
        df_graph_sales_forecasting_by_date = df.loc[:, ["transaction_date", "sales_forecasting"]]
        df_graph_sales_forecasting_by_date.sales_forecasting = df_graph_sales_forecasting_by_date.sales_forecasting.astype(float)
        
        if total_sales_this_month == 0:
            return empty_data_response

        last_month_per_date_filter = (df["transaction_date"] >= last_month_beginning) & (df["transaction_date"] <= last_month_to_today_date)
        total_sales_last_month = float(df.loc[last_month_per_date_filter, "total_sales"].sum())

        if total_sales_last_month == 0:
            sales_growth_rate = 100
        else:
            sales_growth_rate = (total_sales_this_month - total_sales_last_month) / total_sales_last_month * 100

        # Total Sales prediction
        prediction_filter_date = (df["transaction_date"] > now_wib) & (df["transaction_date"] <= this_month_end_date)
        total_sales_prediction = total_sales_this_month + float(df.loc[prediction_filter_date, "sales_forecasting"].sum())
        
        if total_sales_last_month == 0:
            sales_prediction_growth_rate = 100
        else:
            sales_prediction_growth_rate = (total_sales_prediction - total_sales_last_month) / total_sales_last_month * 100

        # Change the datetime type from transaction_date column into string
        df_this_month["transaction_date"] = df_this_month["transaction_date"].dt.strftime("%Y-%m-%d")
        
        df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d")
        df_graph_sales_by_date = df.loc[:, ["transaction_date", "total_sales"]]
        df_graph_sales_projection_by_date = df.loc[:, ["transaction_date", "sales_projection"]]
        df_graph_sales_forecasting_by_date = df.loc[:, ["transaction_date", "sales_forecasting"]]
        
        # Find the row with the maximum total_sales and Extract maximum sales and date
        max_index = df_this_month.loc[df_this_month["total_sales"].idxmax()]
        max_sales = int(max_index["total_sales"])
        max_sales_date = str(max_index["transaction_date"])

        # Find the row with the maximum total_sales and Extract maximum sales and date
        filtered = df_this_month.loc[df_this_month["total_sales"] > 0.0]
        min_idx = int(filtered["total_sales"].idxmin())
        min_sales = int(filtered.loc[min_idx, "total_sales"])
        min_sales_date = str(filtered.loc[min_idx, "transaction_date"])

        response = {
            "data": {
                "total_sales": total_sales_this_month,
                "total_sales_prediction": total_sales_prediction,
                "sales_growth_rate": sales_growth_rate,
                "sales_prediction_growth_rate": sales_prediction_growth_rate,

                "highest_sales_date": max_sales_date,
                "highest_sales_total_by_date": max_sales,
                "lowest_sales_date": min_sales_date,
                "lowest_sales_total_by_date": min_sales,

                "graph_sales_by_date": df_graph_sales_by_date.to_dict(orient='records'),
                "graph_sales_projection_by_date": df_graph_sales_projection_by_date.to_dict(orient='records'),
                "graph_sales_forecasting_by_date": df_graph_sales_forecasting_by_date.to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }

        return response
    
    @staticmethod
    def GetLabaML(store_id: int):
        now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        now_wib = now_utc + timedelta(hours=7) - timedelta(days=1)
        now_wib = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)
        today, this_month, this_year = now_wib.day, now_wib.month, now_wib.year

        this_month_beginning = now_wib.replace(day=1).replace(hour=0, minute=0, second=0, microsecond=0)
        this_month_end_day = calendar.monthrange(this_year, this_month)[1]
        this_month_end_date = this_month_beginning.replace(day=this_month_end_day, hour=0, minute=0, second=0, microsecond=0)
        
        last_month = this_month_beginning - timedelta(days=1)
        last_month_beginning = last_month.replace(day=1)
        last_month_end_day = calendar.monthrange(last_month.year, last_month.month)[1]
        last_month_end_date = last_month_beginning.replace(day=last_month_end_day)
        last_month_end_date = last_month_end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        if (today == 31 and last_month_end_day <= 30) or (today == 30 and last_month_end_day <= 29):
            n_diff = this_month_end_day - last_month_end_day
            last_month_to_today_date = last_month_beginning.replace(day=today-n_diff)
        else:
            last_month_to_today_date = last_month_beginning.replace(day=today)

        next_month = this_month_end_date + timedelta(days=1)
        next_month = next_month.replace(hour=0, minute=0, second=0, microsecond=0)
        next_month_end_day = calendar.monthrange(next_month.year, next_month.month)[1]
        next_month_end_date = next_month.replace(day=next_month_end_day, hour=0, minute=0, second=0, microsecond=0)

        start_penarikan = last_month_beginning.strftime("%Y")
        end_penarikan = next_month_end_date.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        conn = olsera_replica_config.get_sqlalchemy_engine()
        deposit_query = text("""
        SELECT
            ci_store_ext_settings.is_deposit_active
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = :store_id
        """)

        try:
            with conn.connection() as connection:
                is_deposit_activated = pd.read_sql(deposit_query, con=connection, params={"store_id": store_id})
                is_deposit_activated = is_deposit_activated.iloc[0, 0]
        except:
            is_deposit_activated = 1

        dfs_deposit = []
        conn.dispose()
        if is_deposit_activated == 0:
            for date in dates:
                path_deposit = f"{EndPoint.directory}/sales-deposit-tracking-vision/{store_id}_{date}_deposit_tracking_vision.feather"
                df_deposit = download_file_from_s3(path_deposit)
                dfs_deposit.append(df_deposit)
        
        analytic_conn = olsera_analytic_config.get_sqlalchemy_engine()
        query = text("""
        select
            laba,
            laba_projection,
            laba_forecasting,
            transaction_date
        from olsera_datainsight_profit_projection_forecasting
        where store_id = :store_id
        AND transaction_date BETWEEN :start_date AND :end_date
        """)
        
        with analytic_conn.connect() as connection:
            df = pd.read_sql(
                query, 
                con=connection, 
                params={
                    "store_id": store_id, 
                    "start_date": last_month_beginning.strftime("%Y-%m-%d"), 
                    "end_date": next_month_end_date.strftime("%Y-%m-%d")
                }
            )

        analytic_conn.dispose()
        empty_data_response = {
            "data": {
                "total_laba": 0,
                "total_laba_prediction": 0,
                "laba_growth_rate": 0.0,
                "laba_prediction_growth_rate": 0.0,

                "highest_laba_date": "0000-00-00",
                "highest_laba_total_by_date": 0,
                "lowest_laba_date": "0000-00-00",
                "lowest_laba_total_by_date": 0,

                "graph_laba_by_date": pd.DataFrame({"order_date": [], "total_laba": []}).to_dict(orient='records'),
                "graph_laba_projection_by_date": pd.DataFrame({"order_date": [], "total_projection": []}).to_dict(orient='records'),
                "graph_laba_forecasting_by_date": pd.DataFrame({"order_date": [], "total_forecasting": []}).to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }

        if df.empty:
            return empty_data_response

        # Mengurangi Data dengan Deposit
        if dfs_deposit:
            df_deposit = pd.concat(dfs_deposit)
            if not(df_deposit.empty):
                df_deposit.rename(columns={"transaction_date_deposit": "transaction_date"}, inplace=True)
                df_deposit["total_deposit"] = df_deposit["total_deposit"].fillna(0)
                df_deposit.transaction_date = pd.to_datetime(df_deposit.transaction_date)
                df_deposit = df_deposit[(df_deposit.transaction_date >= last_month_beginning) & (df_deposit.transaction_date >= next_month_end_date)]

                df_deposit = df_deposit.groupby("transaction_date", as_index=False)["total_deposit"].sum()
                df = df.merge(df_deposit, on=["transaction_date"], how="outer")
                df["total_deposit"] = df["total_deposit"].fillna(0).astype(float)
                df["laba"] = df["laba"].fillna(0)
                df["laba"] = df["laba"] - df["total_deposit"]
                df.drop("total_deposit", axis=1, inplace=True)

        df["transaction_date"] = pd.to_datetime(df["transaction_date"])

        beginning_month_to_today_date = (df["transaction_date"] >= this_month_beginning) & (df["transaction_date"] <= now_wib)
        last_month_per_date_filter = (df["transaction_date"] >= last_month_beginning) & (df["transaction_date"] <= last_month_to_today_date)

        # Total laba and its growth rate
        df_this_month = df.loc[beginning_month_to_today_date, ["laba", "transaction_date"]]
        total_laba_this_month = float(df_this_month.loc[:, "laba"].sum())
        
        df["laba"] = df["laba"].astype(float)
        df_graph_laba_by_date = df.loc[:, ["transaction_date", "laba"]]
        df_graph_laba_by_date.transaction_date = df_graph_laba_by_date.transaction_date.dt.strftime("%Y-%m-%d")

        df_graph_laba_projection_by_date = df.loc[:, ["transaction_date", "laba_projection"]]
        df_graph_laba_projection_by_date.transaction_date = df_graph_laba_projection_by_date.transaction_date.dt.strftime("%Y-%m-%d")
        
        df_graph_laba_forecasting_by_date = df.loc[:, ["transaction_date", "laba_forecasting"]]
        df_graph_laba_forecasting_by_date.transaction_date = df_graph_laba_forecasting_by_date.transaction_date.dt.strftime("%Y-%m")

        if total_laba_this_month == 0:
            return empty_data_response

        total_laba_last_month = float(df.loc[last_month_per_date_filter, "laba"].sum())
        if total_laba_last_month == 0:
            laba_growth_rate = 100
        else:
            laba_growth_rate = (total_laba_this_month - total_laba_last_month) / total_laba_last_month * 100

        # Total laba prediction
        prediction_filter_date = (df["transaction_date"] > now_wib) & (df["transaction_date"] <= this_month_end_date)
        total_laba_prediction = total_laba_this_month + float(df.loc[prediction_filter_date, "laba_forecasting"].sum())
        total_laba_prediction = float(total_laba_prediction)

        if total_laba_last_month == 0:
            laba_prediction_growth_rate = 100
        else:
            laba_prediction_growth_rate = (total_laba_prediction - total_laba_last_month) / total_laba_last_month * 100

        df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d")
        df_graph_laba_by_date = df.loc[:, ["transaction_date", "laba"]]
        df_graph_laba_by_date["laba"] = df_graph_laba_by_date["laba"].fillna(0)

        df_graph_laba_projection_by_date = df.loc[:, ["transaction_date", "laba_projection"]]
        df_graph_laba_projection_by_date["laba_projection"] = df_graph_laba_projection_by_date["laba_projection"].fillna(0)

        df_graph_laba_forecasting_by_date = df.loc[:, ["transaction_date", "laba_forecasting"]]
        df_graph_laba_forecasting_by_date["laba_forecasting"] = df_graph_laba_forecasting_by_date["laba_forecasting"].fillna(0)

        df_this_month["transaction_date"] = df_this_month["transaction_date"].dt.strftime("%Y-%m-%d")
        
        # Extract maximum laba and date
        max_index = df_this_month.loc[df_this_month["laba"].idxmax()]
        max_laba = int(max_index["laba"])
        max_laba_date = str(max_index["transaction_date"])

        # Extract minimum laba and date
        min_index = df_this_month.loc[df_this_month["laba"].idxmin()]
        min_laba = int(min_index["laba"])
        min_laba_date = str(min_index["transaction_date"])

        data_response = {
            "data": {
                "total_laba": total_laba_this_month,
                "total_laba_prediction": total_laba_prediction,
                "laba_growth_rate": laba_growth_rate,
                "laba_prediction_growth_rate": laba_prediction_growth_rate,

                "highest_laba_date": max_laba_date,
                "highest_laba_total_by_date": max_laba,
                "lowest_laba_date": min_laba_date,
                "lowest_laba_total_by_date": min_laba,

                "graph_laba_by_date": df_graph_laba_by_date.to_dict(orient='records'),
                "graph_laba_projection_by_date": df_graph_laba_projection_by_date.to_dict(orient='records'),
                "graph_laba_forecasting_by_date": df_graph_laba_forecasting_by_date.to_dict(orient='records')
            },
            "status": 200,
            "error": 0
        }
        
        return data_response

    @staticmethod
    def GetProductDetail(store_id: int, start_date: str, end_date: str):

        now_wib = dt.datetime.utcnow() - dt.timedelta(hours=7)
        now_wib = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)

        this_year, this_month = now_wib.year, now_wib.month
        first_month = now_wib.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        start_date_obj = validate_date(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_obj = validate_date(end_date).replace(hour=0, minute=0, second=0, microsecond=0)
        
        n_days = (end_date_obj - start_date_obj).days + 1
        difference = relativedelta(end_date_obj, start_date_obj)
        n_months = difference.years * 12 + difference.months + 1
        print(n_months)
        start_date_comp, end_date_comp = get_date_comparison(start_date_obj, end_date_obj)

        start_penarikan = start_date_comp.strftime("%Y")
        end_penarikan = end_date_obj.strftime("%Y")
        dates_raw = [start_penarikan, end_penarikan]

        seen = set()
        dates = []

        for item in dates_raw:
            if item not in seen:
                seen.add(item)
                dates.append(item)

        # Download Data Sales
        dfs = []
        for date in dates:
            try:
                path_product = f"{EndPoint.directory}/product-populer-vision/{store_id}_{date}_product_populer_vision.feather"
                df_product = download_file_from_s3(path_product)
                dfs.append(df_product)
            except:
                continue
        
        df_product = pd.concat(dfs)
        
        empty_data_response = {
            "data": {
                "product_sold_this_period": 0,
                "product_growth_rate": 0.0,

                "sales_amount_this_period": 0,
                "sales_growth_rate": 0,
                
                "product_sold_all_time": 0.0,
                "average_product_sold_monthly": 0.0,

                "highest_total_date": "0000-00-00",
                "highest_total_by_date": 0,

                "lowest_total_date": "0000-00-00",
                "lowest_total_by_date": 0,
                "mean_total_by_date": 0.0,

                "highest_sales_date": "0000-00-00",
                "highest_sales_by_date": 0,

                "lowest_sales_date": "0000-00-00",
                "lowest_sales_by_date": 0,
                "mean_sales_by_date": 0.0,

                "highest_laba": 0.0,
                "lowest_laba": 0.0,
                "highest_sold": 0.0,
                "lowest_sold": 0.0,

                "top5_product_id": [],
                "top5_product_qty": [],

                "bottom5_product_id": [],
                "bottom5_product_qty": [],
                
                "graph_data_by_date": pd.DataFrame({"order_date": [], "total": []}).to_dict(orient="records"),
                "graph_sales_by_date": pd.DataFrame({"order_date": [], "total": []}).to_dict(orient="records"),
                
                "median_laba": 0,
                "median_transaction": 0,
                "total_star": 0,
                "total_puzzle": 0,
                "total_horse": 0,
                "total_dog": 0,
                "product_menu_matrix": pd.DataFrame({"product_id": [], "total_qty": [], "laba": [], "category": []}).to_dict(orient="records")
            },
            "status": 200,
            "error": 0
        }

        if df_product.empty:
            return empty_data_response

        conn = olsera_replica_config.get_sqlalchemy_engine()
        deposit_query = text("""
        SELECT
            ci_store_ext_settings.is_deposit_active
        FROM ci_store_ext_settings
        left join ci_store on ci_store.id = ci_store_ext_settings.id
        WHERE ci_store_ext_settings.id = :store_id
        """)
        
        try:
            with conn.connect() as connection:
                is_deposit_activated = pd.read_sql(deposit_query, con=connection, params={"store_id": store_id}) 
                is_deposit_activated = is_deposit_activated.iloc[0, 0]
        except:
            is_deposit_activated = 1
        conn.dispose()
        # Penarikan Summary
        try:
            path_summary = f"{EndPoint.directory}/summary-sales-product-vision/{store_id}_summary_sales_product_vision.feather"
            df_summary = download_file_from_s3(path_summary)
            if is_deposit_activated == 0:
                total_deposit_all_time = float(df_summary.loc[df_summary["flag"] == 1, "total_qty"].sum())
            else:
                total_deposit_all_time = 0.0
        except:
            total_deposit_all_time = 0.0

        df_product["date_trx_days"] = pd.to_datetime(df_product["date_trx_days"])
        df_product["total_qty"] = df_product["total_qty"].fillna(0).astype(float)
        df_product["total_qty_return"] =  df_product["total_qty_return"].fillna(0).astype(float)
        df_product["total_qty_unpaid"] = df_product["total_qty_unpaid"].fillna(0).astype(float)
        df_product["total_qty"] = df_product["total_qty"] - df_product["total_qty_return"] - df_product["total_qty_unpaid"]
        # index_filter = (df_product["total_qty"] == 0) & (df_product["total_qty_from_item_combo"] > 0)
        index_filter = (df_product["total_qty"] == 0)
        df_product = df_product[~index_filter]

        df_product["total_amount"] = df_product["total_amount"].fillna(0.0).astype(float)
        df_product["total_amount_return"] = df_product["total_amount_return"].fillna(0.0).astype(float)
        df_product["total_amount_unpaid"] = df_product["total_amount_unpaid"].fillna(0.0).astype(float)
        df_product["total_amount"] = df_product["total_amount"] - df_product["total_amount_return"] - df_product["total_amount_unpaid"]
        df_product["total_cost_amount"] = df_product["total_cost_amount"] - df_product["total_cost_amount_return"] - df_product["total_cost_amount_unpaid"]
        df_product["total_cost_amount"] = df_product["total_cost_amount"].fillna(0.0).astype(float)
        
        # Calculate Total Sold all the time from january
        df_this_year = df_product[df_product["date_trx_days"] >= first_month]
        
        total_product_sold_all_time = float(df_this_year["total_qty"].sum()) - total_deposit_all_time
        total_product_sold_all_time = float(total_product_sold_all_time)
        average_sold_per_month = total_product_sold_all_time / n_months

        df_product = df_product.loc[:, ["product_id", "product_variant_id", "total_qty", "total_amount", "total_cost_amount" ,"date_trx_days"]]
        
        min_date = df_product["date_trx_days"].min()
        max_date = df_product["date_trx_days"].max()
        is_possible_to_compare = min_date <= start_date_comp
        is_possible_to_visualize = start_date_obj <= max_date

        empty_data_response["data"]["product_sold_all_time"] = total_product_sold_all_time
        empty_data_response["data"]["average_product_sold_monthly"] = average_sold_per_month
        
        if is_possible_to_visualize:
            # Data untuk Grafik filter date
            df_product_graph = df_product.loc[(df_product["date_trx_days"] >= start_date_obj) & (df_product["date_trx_days"] <= end_date_obj), :]
            if df_product_graph.empty:
                return empty_data_response
            
            df_sales_graph = df_product_graph.groupby("date_trx_days", as_index=False)["total_amount"].sum()
            sales_amount_this_period = float(df_sales_graph["total_amount"].sum())

            df_product_graph = df_product_graph.groupby("date_trx_days", as_index=False)["total_qty"].sum()
            total_product_this_period = float(df_product_graph["total_qty"].sum())
            
            if total_product_this_period == 0:
                return empty_data_response
            
            if np.isfinite(total_deposit_all_time):
                total_deposit_all_time = 0.0

            df_product_graph.total_qty = pd.to_numeric(df_product_graph.total_qty, errors="coerce")
            df_product_graph = df_product_graph.dropna(subset=["total_qty"])
            max_index_qty = df_product_graph.loc[df_product_graph["total_qty"].idxmax()]
        
            max_product = int(max_index_qty["total_qty"])
            max_product_date = str(max_index_qty["date_trx_days"])[:10]

            filtered = df_product_graph[df_product_graph["total_qty"] > 0.0]
            min_index_qty = filtered.loc[filtered["total_qty"].idxmin()]
            min_product = int(min_index_qty["total_qty"])
            min_product_date = str(min_index_qty["date_trx_days"])[:10]

            total_close = df_product_graph[df_product_graph["total_qty"] <= 0.0].shape[0]
            mean_product = total_product_this_period / (n_days - total_close)

            df_sales_graph.total_amount = pd.to_numeric(df_sales_graph.total_amount, errors="coerce")
            df_sales_graph = df_sales_graph.dropna(subset=["total_amount"])
            max_index_sales = df_sales_graph.loc[df_sales_graph["total_amount"].idxmax()]
            max_sales = int(max_index_sales["total_amount"])
            max_sales_date = str(max_index_sales["date_trx_days"])[:10]

            min_index_sales = df_sales_graph.loc[df_sales_graph["total_amount"].idxmin()]
            min_sales = int(min_index_sales["total_amount"])
            min_sales_date = str(min_index_sales["date_trx_days"])[:10]
            
            mean_sales = float(df_sales_graph["total_amount"].sum()) / n_days

            # DataFrame untuk Perbandingan
            if is_possible_to_compare:
                df_product_comparison = df_product.loc[(df_product["date_trx_days"] >= start_date_comp) & (df_product["date_trx_days"] <= end_date_comp), :]
                total_period_product_before = float(df_product_comparison["total_qty"].sum())
                sales_amount_last_period = float(df_product_comparison["total_amount"].sum())

                try:
                    growth_product = (total_product_this_period - total_period_product_before) / (total_period_product_before) * 100
                    growth_product = float(growth_product)
                except:
                    growth_product = float(100.0)

                try:
                    growth_sales = (sales_amount_this_period - sales_amount_last_period) / (sales_amount_last_period) * 100
                    growth_sales = float(growth_sales)
                except:
                    growth_sales = float(100.0)

            else:
                growth_sales = float(100.0)
                growth_product = float(100.0)
            
            df_sales_graph = df_sales_graph.loc[:, ["date_trx_days", "total_amount"]]
            df_sales_graph["date_trx_days"] = df_sales_graph["date_trx_days"].dt.strftime("%Y-%m-%d")
            df_sales_graph.total_amount = df_sales_graph.total_amount.astype(float)
            df_product_graph = df_product_graph.loc[:, ["date_trx_days", "total_qty"]]
            df_product_graph["date_trx_days"] = df_product_graph["date_trx_days"].dt.strftime("%Y-%m-%d")

            df_product_graph.rename(columns={"date_trx_days": "order_date", "total_qty": "total"}, inplace=True)
            df_product_graph.total = df_product_graph.total.astype(float)
            df_product = df_product.loc[(df_product["date_trx_days"] >= start_date_obj) & (df_product["date_trx_days"] <= end_date_obj), :]

            df_product["product_id"].fillna(0, inplace=True)
            df_product["product_id"] = df_product["product_id"].astype(int)

            df_product["product_variant_id"].fillna(0, inplace=True)
            df_product["product_variant_id"] = df_product["product_variant_id"].astype(int)
            
            df_product["product_id"] = df_product["product_id"].astype(str) + "-" + df_product["product_variant_id"].astype(str)
            df_product.drop("product_variant_id", axis=1, inplace=True)
            grouped_product = df_product.groupby("product_id", as_index=False)[["total_qty", "total_amount", "total_cost_amount"]].sum()
            grouped_product["laba"] = grouped_product["total_amount"] - grouped_product["total_cost_amount"]
            grouped_product.drop(["total_amount", "total_cost_amount"], axis=1, inplace=True)

            max_laba = float(grouped_product["laba"].max())
            min_laba = float(grouped_product["laba"].min())
            median_laba = float(grouped_product.loc[grouped_product["laba"] != 0.0, "laba"].median())
            median_laba = 0.0 if np.isnan(median_laba) else median_laba
            
            highest_sold = float(grouped_product["total_qty"].max())
            lowest_sold = float(grouped_product["total_qty"].min())

            median_transaction = float(grouped_product.loc[grouped_product["total_qty"] != 0.0, "total_qty"].median())
            median_transaction = 0.0 if np.isnan(median_transaction) else median_transaction
            
            grouped_product["category"] = 0
            grouped_product.loc[(grouped_product["laba"] > median_laba) & (grouped_product["total_qty"] > median_transaction), "category"] = 1
            grouped_product.loc[(grouped_product["laba"] > median_laba) & (grouped_product["total_qty"] <= median_transaction), "category"] = 2
            grouped_product.loc[(grouped_product["laba"] <= median_laba) & (grouped_product["total_qty"] > median_transaction), "category"] = 3
            grouped_product.loc[(grouped_product["laba"] <= median_laba) & (grouped_product["total_qty"] <= median_transaction), "category"] = 4
            
            grouped_product["total_qty"] = grouped_product["total_qty"].astype(float)
            grouped_product["laba"] = grouped_product["laba"].astype(float)

            threshold_qty = grouped_product.total_qty.quantile(0.90)
            grouped_product["is_extreme_qty"] = grouped_product.total_qty > threshold_qty
            grouped_product["is_extreme_qty"] = grouped_product["is_extreme_qty"].astype(int)
            
            threshold_laba = grouped_product.laba.quantile(0.90)
            grouped_product["is_extreme_laba"] = (grouped_product.laba > threshold_laba).astype(int)
            
            grouped_product["is_extreme"] = (grouped_product["is_extreme_laba"] | grouped_product["is_extreme_qty"]).astype(int)

            total_star = grouped_product.loc[grouped_product["category"] == 1, "category"].shape[0]
            total_puzzle = grouped_product.loc[grouped_product["category"] == 2, "category"].shape[0]
            total_horse = grouped_product.loc[grouped_product["category"] == 3, "category"].shape[0]
            total_dog = grouped_product.loc[grouped_product["category"] == 4, "category"].shape[0]

            top5 = grouped_product.sort_values("total_qty", ascending=False).head(5)
            top5_product_id = top5["product_id"].astype(str).values.tolist()
            top5_product_qty  = top5["total_qty"].astype(float).values.tolist()

            bottom5 = grouped_product.sort_values("total_qty", ascending=True).head(5)
            bottom5_product_id = bottom5["product_id"].astype(str).values.tolist()
            bottom5_product_qty  = bottom5["total_qty"].astype(float).values.tolist()

            product_json_data = {
                "data": {
                    "product_sold_this_period": total_product_this_period,
                    "product_growth_rate": growth_product,

                    "sales_amount_this_period": sales_amount_this_period,
                    "sales_growth_rate": growth_sales,

                    "product_sold_all_time": total_product_sold_all_time,
                    "average_product_sold_monthly": average_sold_per_month,
                    
                    "highest_total_date": max_product_date,
                    "highest_total_by_date": max_product,

                    "lowest_total_date": min_product_date,
                    "lowest_total_by_date": min_product,
                    "mean_total_by_date": mean_product,

                    "highest_sales_date": max_sales_date,
                    "highest_sales_by_date": max_sales,

                    "lowest_sales_date": min_sales_date,
                    "lowest_sales_by_date": min_sales,
                    "mean_sales_by_date": mean_sales,

                    "highest_laba": max_laba,
                    "lowest_laba": min_laba,
                    "highest_sold": highest_sold,
                    "lowest_sold": lowest_sold,

                    "top5_product_id": top5_product_id,
                    "top5_product_qty": top5_product_qty,

                    "bottom5_product_id": bottom5_product_id,
                    "bottom5_product_qty": bottom5_product_qty,

                    "graph_data_by_date": df_product_graph.to_dict(orient="records"),
                    "graph_sales_by_date": df_sales_graph.to_dict(orient="records"),

                    "median_laba": median_laba,
                    "median_transaction": median_transaction,
                    "total_star": total_star,
                    "total_puzzle": total_puzzle,
                    "total_horse": total_horse,
                    "total_dog": total_dog,
                    "product_menu_matrix": grouped_product.to_dict(orient="records"),
                },
                "status": 200,
                "error": 0
            }
            return product_json_data
        else:
            return empty_data_response

    @staticmethod
    def GetProductTrend(store_id: int):

        file_path = f"{EndPoint.directory}/ml/product-trend/{store_id}_product_trend.json"
        response = download_json_file_from_s3(file_path)
        
        # Check if response is empty or error occurred
        if not response:
            raise HTTPException(status_code=404, detail="Product trend data not found")
        
        return response
