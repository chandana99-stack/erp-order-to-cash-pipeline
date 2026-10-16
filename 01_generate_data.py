# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate source data (the "pretend ERP")
# MAGIC Writes one folder of CSV files per batch into the landing volume, like a nightly extract from an ERP system:
# MAGIC
# MAGIC `/Volumes/<catalog>/erp_raw/landing/<batch_date>/customers.csv, products.csv, sales_orders.csv, order_items.csv, deliveries.csv, invoices.csv, payments.csv`
# MAGIC
# MAGIC - **First run (initial load):** 500 customers, 40 products and ~90 days of order history.
# MAGIC - **Later runs (incremental):** one day of new orders, payments against open invoices, a few new customers and a few customer changes (address / credit limit) to exercise SCD2.
# MAGIC
# MAGIC The data is **messy on purpose**: duplicate rows, blank emails, extra spaces, mixed-case text, two date formats, negative quantities and orders for customers that don't exist. Cleaning that up is the silver layer's job.

# COMMAND ----------

# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_date", "", "Batch date (blank = today)")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

batch_day = datetime.strptime(BATCH_DATE, "%Y-%m-%d").date()
seed = int(batch_day.strftime("%Y%m%d"))
random.seed(seed)
np.random.seed(seed % (2**32))
fake = Faker("en_US")
Faker.seed(seed)

os.makedirs(LANDING_PATH, exist_ok=True)
if os.path.exists(f"{LANDING_PATH}/{BATCH_DATE}/payments.csv"):
    # a source system extracts each day only once; re-running keeps the same files
    dbutils.notebook.exit(f"Batch {BATCH_DATE} already generated - skipping")

earlier_batches = [d for d in os.listdir(LANDING_PATH) if d < BATCH_DATE]
IS_INITIAL = len(earlier_batches) == 0
print("load type:", "INITIAL" if IS_INITIAL else "INCREMENTAL")

CITIES = [("St. Louis", "MO"), ("Kansas City", "MO"), ("Chicago", "IL"), ("Houston", "TX"), ("Dallas", "TX"),
          ("Memphis", "TN"), ("Nashville", "TN"), ("Atlanta", "GA"), ("Denver", "CO"), ("Columbus", "OH"),
          ("Indianapolis", "IN"), ("Minneapolis", "MN"), ("Phoenix", "AZ"), ("Charlotte", "NC"), ("Newark", "NJ")]
SEGMENTS = {"Retail": 30, "Wholesale": 45, "Distributor": 60}          # segment -> payment terms (days)
CATEGORIES = ["Building Blocks", "Dolls", "Board Games", "Puzzles", "Outdoor", "Plush", "STEM Kits", "Vehicles"]
CARRIERS = ["UPS", "FedEx", "USPS", "XPO Logistics"]
METHODS = ["ACH", "Wire", "Check", "Credit Card"]

# COMMAND ----------

# MAGIC %md ### Build clean records

# COMMAND ----------

def make_customer(cid, as_of):
    city, state = random.choice(CITIES)
    seg = random.choice(list(SEGMENTS))
    return {"customer_id": cid, "customer_name": fake.company(), "email": fake.company_email(),
            "city": city, "state": state, "segment": seg,
            "credit_limit": random.choice([5000, 10000, 25000, 50000, 100000]),
            "updated_at": as_of.isoformat()}


def make_orders(order_days, customer_ids, products, as_of):
    orders, items, deliveries, invoices = [], [], [], []
    price = dict(zip(products["product_id"], products["unit_price"]))
    seg_of = customers_segment_lookup
    for day in order_days:
        for n in range(random.randint(25, 40)):
            oid = f"SO{day:%Y%m%d}{n:04d}"
            cid = random.choice(customer_ids)
            status = random.choices(["OPEN", "SHIPPED", "CANCELLED"], [0.1, 0.85, 0.05])[0]
            orders.append({"order_id": oid, "customer_id": cid, "order_date": day.isoformat(),
                           "sales_rep": fake.first_name(), "status": status})
            total = 0.0
            for line in range(1, random.randint(1, 4) + 1):
                pid = random.choice(list(price))
                qty = random.randint(1, 50)
                items.append({"order_id": oid, "line_no": line, "product_id": pid,
                              "quantity": qty, "unit_price": price[pid]})
                total += qty * price[pid]
            if status != "SHIPPED":
                continue
            ship = day + timedelta(days=random.randint(0, 3))
            if ship > as_of:
                ship = as_of
            delivered = ship + timedelta(days=random.randint(2, 6))
            deliveries.append({"delivery_id": f"DL{oid[2:]}", "order_id": oid, "ship_date": ship.isoformat(),
                               "delivered_date": delivered.isoformat() if delivered <= as_of else "",
                               "carrier": random.choice(CARRIERS)})
            terms = SEGMENTS.get(seg_of.get(cid, "Retail"), 30)
            invoices.append({"invoice_id": f"INV{oid[2:]}", "order_id": oid, "invoice_date": ship.isoformat(),
                             "due_date": (ship + timedelta(days=terms)).isoformat(),
                             "amount": round(total, 2), "currency": "USD"})
    return orders, items, deliveries, invoices


def make_payments(open_invoices, as_of, pay_prob):
    """open_invoices: rows with invoice_id, invoice_date, due_date, amount (still owed)."""
    pays = []
    for inv in open_invoices:
        if random.random() > pay_prob:
            continue
        inv_date = pd.to_datetime(inv["invoice_date"]).date()
        if IS_INITIAL:
            pay_day = inv_date + timedelta(days=max(1, int(np.random.normal(32, 12))))
            if pay_day > as_of:
                continue
        else:
            pay_day = as_of
        amount = float(inv["amount"])
        parts = [amount] if random.random() > 0.07 else [round(amount * 0.6, 2), round(amount - round(amount * 0.6, 2), 2)]
        for i, part in enumerate(parts):
            pays.append({"payment_id": f"PAY{inv['invoice_id'][3:]}{i}{pay_day:%m%d}", "invoice_id": inv["invoice_id"],
                         "payment_date": pay_day.isoformat(), "amount": part, "method": random.choice(METHODS)})
    return pays

# COMMAND ----------

if IS_INITIAL:
    customers = pd.DataFrame([make_customer(f"C{i:05d}", batch_day) for i in range(1, 501)])
    product_rows = []
    for i in range(1, 41):
        cat = random.choice(CATEGORIES)
        product_rows.append({"product_id": f"P{i:03d}",
                             "product_name": f"{fake.color_name()} {random.choice(['Deluxe', 'Mini', 'Classic', 'Pro', 'Junior'])} {cat}",
                             "category": cat,
                             "unit_price": round(random.uniform(4, 120), 2)})
    products = pd.DataFrame(product_rows)
    customers_segment_lookup = dict(zip(customers["customer_id"], customers["segment"]))
    days = [batch_day - timedelta(days=d) for d in range(90, 0, -1)]
    orders, items, deliveries, invoices = make_orders(days, list(customers["customer_id"]), products, batch_day)
    payments = make_payments(invoices, batch_day, pay_prob=0.92)
else:
    cust_tbl = tbl("erp_silver", "customers")
    existing = spark.table(cust_tbl).toPandas()
    products = spark.table(tbl("erp_silver", "products")).select("product_id", "product_name", "category", "unit_price").toPandas()
    products["unit_price"] = products["unit_price"].astype(float)

    # a few brand-new customers
    next_id = int(existing["customer_id"].str[1:].astype(int).max()) + 1
    new_rows = [make_customer(f"C{i:05d}", batch_day) for i in range(next_id, next_id + random.randint(2, 5))]
    # a few existing customers move city or get a new credit limit (-> SCD2 history)
    changed = existing.sample(random.randint(3, 6), random_state=seed % (2**32)).copy()
    for idx in changed.index:
        if random.random() < 0.5:
            changed.at[idx, "city"], changed.at[idx, "state"] = random.choice(CITIES)
        else:
            changed.at[idx, "credit_limit"] = random.choice([5000, 10000, 25000, 50000, 100000, 150000])
    changed["updated_at"] = batch_day.isoformat()
    keep = ["customer_id", "customer_name", "email", "city", "state", "segment", "credit_limit", "updated_at"]
    customers = pd.concat([changed[keep], pd.DataFrame(new_rows)], ignore_index=True)
    customers["credit_limit"] = customers["credit_limit"].astype(float).astype(int)

    customers_segment_lookup = {**dict(zip(existing["customer_id"], existing["segment"])),
                                **dict(zip(customers["customer_id"], customers["segment"]))}
    all_ids = list(customers_segment_lookup)
    orders, items, deliveries, invoices = make_orders([batch_day], all_ids, products, batch_day)

    # pay some of the invoices that are still open
    open_inv = spark.sql(f"""
        SELECT i.invoice_id, i.invoice_date, i.due_date, i.amount - COALESCE(p.paid, 0) AS amount
        FROM {tbl('erp_silver', 'invoices')} i
        LEFT JOIN (SELECT invoice_id, SUM(amount) AS paid FROM {tbl('erp_silver', 'payments')} GROUP BY invoice_id) p
          ON i.invoice_id = p.invoice_id
        WHERE i.amount - COALESCE(p.paid, 0) > 0.01
    """).toPandas().to_dict("records")
    payments = make_payments(open_inv, batch_day, pay_prob=0.06)

frames = {"customers": customers, "products": products, "sales_orders": pd.DataFrame(orders),
          "order_items": pd.DataFrame(items), "deliveries": pd.DataFrame(deliveries),
          "invoices": pd.DataFrame(invoices), "payments": pd.DataFrame(payments)}
{name: len(df) for name, df in frames.items()}

# COMMAND ----------

# MAGIC %md ### Make it messy (like real source data)

# COMMAND ----------

def add_duplicates(df, rate=0.02):
    if df.empty:
        return df
    return pd.concat([df, df.sample(frac=rate, random_state=1)], ignore_index=True)


def roughen(name, df):
    df = df.copy().astype(object)
    if df.empty:
        return df
    pick = lambda rate: np.random.rand(len(df)) < rate
    if name == "customers":
        df.loc[pick(0.05), "email"] = ""
        df.loc[pick(0.10), "customer_name"] = "  " + df["customer_name"].astype(str) + " "
        df.loc[pick(0.15), "state"] = df["state"].astype(str).str.lower()
        df.loc[pick(0.10), "segment"] = df["segment"].astype(str).str.upper()
    if name == "sales_orders":
        m = pick(0.05)
        df.loc[m, "order_date"] = pd.to_datetime(df.loc[m, "order_date"]).dt.strftime("%m/%d/%Y")
        df.loc[pick(0.10), "status"] = df["status"].astype(str).str.lower()
        orphans = pick(0.01)                                        # customer that doesn't exist
        df.loc[orphans, "customer_id"] = "C99999"
    if name == "order_items":
        df.loc[pick(0.005), "quantity"] = -1 * df["quantity"].astype(int)
    if name == "invoices":
        m = pick(0.03)
        df.loc[m, "invoice_date"] = pd.to_datetime(df.loc[m, "invoice_date"]).dt.strftime("%m/%d/%Y")
    if name == "payments":
        df.loc[pick(0.20), "method"] = df["method"].astype(str).str.lower()
    return add_duplicates(df)


out_dir = f"{LANDING_PATH}/{BATCH_DATE}"
os.makedirs(out_dir, exist_ok=True)
for name, df in frames.items():
    messy = roughen(name, df)
    messy.to_csv(f"{out_dir}/{name}.csv", index=False)
    print(f"{name:<13} {len(messy):>6} rows -> {out_dir}/{name}.csv")
