import pandas as pd
import random
from faker import Faker
def generate_initial_data(num_customers=15, num_orders=50):
    customers = [{"customer_id": i + 1, 
                  "customer_name": fake.name(), 
                  "city": fake.city(), 
                  "state": fake.state(),
                  "email": fake.email()} for i in range(num_customers)]
    
    orders = []
    for i in range(num_orders):
        customer = random.choice(customers)
        orders.append({
            "transaction_id": f"T{i+1:04}",
            "sales_id": f"S{i+1:04}",
            "customer_id": customer["customer_id"],
            "customer_name": customer["customer_name"],
            "city": customer["city"],
            "state": customer["state"],
            "email": customer["email"],
            "order_date": fake.date_this_year(),
            "order_product": fake.word(),
            "quantity": random.randint(1, 10),
            "price": round(random.uniform(10.0, 500.0), 2),
        })

    return pd.DataFrame(orders)

df=generate_initial_data()
df.to_csv("",index=False)
