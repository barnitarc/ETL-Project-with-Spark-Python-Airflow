import pandas as pd
import random
from faker import Faker
import os
fake=Faker()
def generate_incremental_data(initial_data_path, num_orders=10, new_customer_prob=0.2, update_customer_prob=0.3):
    initial_data = pd.read_csv(initial_data_path)
    existing_customers = initial_data[["customer_id", "customer_name", "city", "state", "email"]].drop_duplicates().to_dict("records")

    # Generate new customers
    def create_new_customer():
        return {
            "customer_id": max([c["customer_id"] for c in existing_customers]) + len(existing_customers) + 1,
            "customer_name": fake.name(),
            "city": fake.city(),
            "state": fake.state(),
            "email": fake.email(),
        }

    orders = []
    for i in range(num_orders):
        if random.random() < new_customer_prob:  # Create new customer with some probability
            new_customer = create_new_customer()
            existing_customers.append(new_customer)
            customer = new_customer
        else:  # Use existing customers
            customer = random.choice(existing_customers)
            # Simulate a chance of customer info changing
            if random.random() < update_customer_prob:
                customer["city"] = fake.city()
                customer["state"] = fake.state()
                customer["email"]= fake.email()

        orders.append({
            "transaction_id": f"T{len(initial_data) + i + 1:04}",
            "sales_id": f"S{len(initial_data) + i + 1:04}",
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
df=generate_incremental_data("/home/barnita/work/airflow-projects/dags/project-3/initial-data/initial_load_history.csv")
df.to_csv("/home/barnita/work/airflow-projects/dags/project-3/incremental-data/incremental_load.csv",index=False, header=True)
df.to_csv("/home/barnita/work/airflow-projects/dags/project-3/initial-data/initial_load_history.csv",index=False,mode='w',header=True)
