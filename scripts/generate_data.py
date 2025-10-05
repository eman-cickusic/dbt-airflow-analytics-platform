import csv
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# --- Configuration ---
NUM_USERS = 1000
NUM_ORDERS = 5000
NUM_TICKETS = 250
OUTPUT_DIR = "raw_data"

import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- Generate Users ---

users = []
for _ in range(NUM_USERS):
    user_id = fake.uuid4()
    users.append({
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address().replace('\n', ', '),
        "created_at": fake.date_time_between(start_date="-2y", end_date="now").isoformat()
    })

with open(f'{OUTPUT_DIR}/users.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=users[0].keys())
    writer.writeheader()
    writer.writerows(users)

print(f"Generated {len(users)} users.")


# --- Generate Orders ---

orders = []
user_ids = [u['user_id'] for u in users]
for _ in range(NUM_ORDERS):
    order_date = fake.date_time_between(start_date="-2y", end_date="now")
    orders.append({
        "order_id": fake.uuid4(),
        "user_id": random.choice(user_ids),
        "order_date": order_date.isoformat(),
        "amount": round(random.uniform(10.5, 500.99), 2),
        "status": random.choice(["pending", "completed", "shipped", "cancelled"])
    })

with open(f'{OUTPUT_DIR}/orders.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

print(f"Generated {len(orders)} orders.")


# --- Generate Support Tickets ---

tickets = []
for _ in range(NUM_TICKETS):
    created_date = fake.date_time_between(start_date="-1y", end_date="now")
    resolved_date = created_date + timedelta(days=random.randint(1, 30))
    tickets.append({
        "ticket_id": fake.uuid4(),
        "user_id": random.choice(user_ids),
        "created_at": created_date.isoformat(),
        "resolved_at": resolved_date.isoformat(),
        "status": random.choice(["open", "closed", "in_progress"]),
        "priority": random.choice(["low", "medium", "high", "urgent"])
    })

with open(f'{OUTPUT_DIR}/support_tickets.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=tickets[0].keys())
    writer.writeheader()
    writer.writerows(tickets)

print(f"Generated {len(tickets)} support tickets.")