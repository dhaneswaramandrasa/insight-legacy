from enum import Enum


class ModulChoices(str, Enum):
    sales = 'sales'
    product = 'product'
    customer = 'customer'
    sales_ml = "sales_ml"
    laba_ml = "laba_ml"
    product_detail = "product_detail"
    product_trend = "product_trend"