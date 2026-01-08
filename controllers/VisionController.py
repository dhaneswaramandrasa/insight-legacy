from fastapi import Request, Depends, HTTPException
from schemas.schemas import ModulChoices
from services.end_point import EndPoint

class VisionController:
    
    @staticmethod
    async def DataInsight(store_id, start_date, end_date, modul):
        if modul == ModulChoices.sales:
            response = EndPoint.GetSales(store_id, start_date, end_date)
        elif modul == ModulChoices.product:
            response = EndPoint.GetProduct(store_id, start_date, end_date)
        elif modul == ModulChoices.customer:
            response = EndPoint.GetCustomer(store_id, start_date, end_date)
        elif modul == ModulChoices.sales_ml:
            response = EndPoint.GetSalesML(store_id)
        elif modul == ModulChoices.laba_ml:
            response = EndPoint.GetLabaML(store_id)
        elif modul == ModulChoices.product_detail:
            response = EndPoint.GetProductDetail(store_id, start_date, end_date)
        elif modul == ModulChoices.product_trend:
            response = EndPoint.GetProductTrend(store_id)
        else:
             raise HTTPException(status_code=404, detail="Module should in one of the sales, product or customer")
        return response