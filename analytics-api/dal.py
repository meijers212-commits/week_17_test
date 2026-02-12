from connection import conn, cursor

class DataAccessLayer:

    @staticmethod
    def Get_the_ten_customers_with_the_highest_order_volume():
        query = """
            SELECT c.customer_name, SUM(o.orderNumber) as total_volume
            FROM customers c
            JOIN orders o ON c.customerNumber = o.customerNumber
            GROUP BY c.customerName
            ORDER BY total_volume DESC
            LIMIT 10;
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    @staticmethod
    def Get_customers_who_havent_placed_an_order():
        query = """
            SELECT c.customerName
            FROM customers c
            LEFT JOIN orders o ON c.customerNumber = o.customerNumber
            WHERE o.customerNumber IS NULL;
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    @staticmethod
    def Get_customers_with_zero_credit_limit_who_placed_orders():
        query = """ 
            SELECT c.customerName 
            FROM customers c 
            JOIN orders o ON c.customerNumber = o.customerNumber 
            WHERE c.creditLimit = 0;
        """ 
        cursor.execute(query) 
        return cursor.fetchall() 

