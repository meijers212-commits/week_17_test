from fastapi import APIRouter
from dal import DataAccessLayer
router = APIRouter()

@router.get("/analytics/top-customers")
def get_top_customers():
    return DataAccessLayer.Get_the_ten_customers_with_the_highest_order_volume()

@router.get("/analytics/customers-without-orders")
def get_costumers_whit_no_orders():
    return DataAccessLayer.Get_customers_who_havent_placed_an_order()

@router.get("/analytics/zero-credit-active-customers")
def customers_with_0_credit_limit_thet_placed_orders():
    return DataAccessLayer.Get_customers_with_zero_credit_limit_who_placed_orders()