import json
from confluent_kafka import Consumer
import os
from mysql_connection import conn, cursor

kafka_topic = os.getenv("KAFKA_TOPIC")


consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic])

print("🟢 Consumer is running")


cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customerNumber INT PRIMARY KEY,
    customerName VARCHAR(255),
    contactLastName VARCHAR(255),
    contactFirstName VARCHAR(255),
    phone VARCHAR(50),
    addressLine1 VARCHAR(255),
    addressLine2 VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postalCode VARCHAR(20),
    country VARCHAR(255),
    salesRepEmployeeNumber INT,
    creditLimit FLOAT
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    orderNumber INT PRIMARY KEY,
    orderDate VARCHAR(255),
    requiredDate VARCHAR(255),
    shippedDate VARCHAR(255),
    status VARCHAR(255),
    comments TEXT,
    customerNumber INT,
    FOREIGN KEY (customerNumber)
        REFERENCES customers(customerNumber)
        ON DELETE CASCADE
)
""")

conn.commit()
print("✅ MySQL tables ready")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        docs = json.loads(value)
        data_to_process = docs if isinstance(docs, list) else [docs]

        for data in data_to_process:

            if data["type"] == "customer":

                sql = """
                INSERT INTO customers (
                    customerNumber,
                    customerName,
                    contactLastName,
                    contactFirstName,
                    phone,
                    addressLine1,
                    addressLine2,
                    city,
                    state,
                    postalCode,
                    country,
                    salesRepEmployeeNumber,
                    creditLimit
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    customerName = VALUES(customerName)
                """

                val = (
                    data["customerNumber"],
                    data["customerName"],
                    data["contactLastName"],
                    data["contactFirstName"],
                    data["phone"],
                    data["addressLine1"],
                    data["addressLine2"],
                    data["city"],
                    data["state"],
                    data["postalCode"],
                    data["country"],
                    data["salesRepEmployeeNumber"],
                    data["creditLimit"]
                )

                cursor.execute(sql, val)

            elif data["type"] == "order":

                sql = """
                INSERT INTO orders (
                    orderNumber,
                    orderDate,
                    requiredDate,
                    shippedDate,
                    status,
                    comments,
                    customerNumber
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status)
                """

                val = (
                    data["orderNumber"],
                    data["orderDate"],
                    data["requiredDate"],
                    data["shippedDate"],
                    data["status"],
                    data["comments"],
                    data["customerNumber"]
                )

                cursor.execute(sql, val)

        conn.commit()
        print("✅ Data written to MySQL")

except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
    cursor.close()
    conn.close()


