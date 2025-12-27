import time
import uuid
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


session.execute("""
    CREATE KEYSPACE IF NOT EXISTS der_monitoring 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace('der_monitoring')


tables = {
    "operational_metrics": """
        CREATE TABLE IF NOT EXISTS operational_metrics (
            generator_id UUID,
            timestamp TIMESTAMP,
            power_kw DECIMAL,
            voltage int,
            PRIMARY KEY (generator_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """,
    "generator_info": """
        CREATE TABLE IF NOT EXISTS generator_info (
            generator_type text,
            generator_id UUID,
            location text,
            PRIMARY KEY (generator_type, generator_id)
        );
    """,
    "regional_balances": """
        CREATE TABLE IF NOT EXISTS regional_balances (
            region text,
            date date,
            total_output_mwh DECIMAL,
            PRIMARY KEY (region, date)
        );
    """,
    "daily_reports": """
        CREATE TABLE IF NOT EXISTS daily_reports (
            date date,
            generator_id UUID,
            avg_daily_power DECIMAL,
            PRIMARY KEY (date, generator_id)
        );
    """
}

for name, query in tables.items():
    session.execute(query)
    print(f"Таблиця {name} готова.")


gen_ids = [uuid.uuid4() for _ in range(2)] # Два генератори для порівняння (Підваріант Б)
types = ['Solar', 'Wind']
regions = ['Kyiv', 'Lviv']

start_time = time.time()
total_records = 0


for i, g_id in enumerate(gen_ids):
    session.execute(
        "INSERT INTO generator_info (generator_type, generator_id, location) VALUES (%s, %s, %s)",
        (types[i], g_id, regions[i])
    )
    
    for j in range(20):
        ts = datetime.now() - timedelta(minutes=j*30)
        power = random.uniform(5.0, 15.0)
        session.execute(
            "INSERT INTO operational_metrics (generator_id, timestamp, power_kw, voltage) VALUES (%s, %s, %s, %s)",
            (g_id, ts, power, random.randint(220, 240))
        )
        total_records += 1

end_time = time.time()
duration = end_time - start_time


print("\n--- РЕЗУЛЬТАТИ АНАЛІЗУ ---")
avg_powers = []
for g_id in gen_ids:
    rows = session.execute("SELECT power_kw FROM operational_metrics WHERE generator_id = %s", (g_id,))
    powers = [row.power_kw for row in rows]
    avg_p = sum(powers) / len(powers)
    avg_powers.append(avg_p)
    print(f"Генератор {g_id}: Середня потужність = {avg_p:.2f} кВт")


diff = abs(avg_powers[0] - avg_powers[1])
winner = "Генератор 1" if avg_powers[0] > avg_powers[1] else "Генератор 2"
print(f"Результат: {winner} ефективніший на {diff:.2f} кВт.")

print(f"\n--- СТАТИСТИКА Cassandra ---")
print(f"Вставлено записів: {total_records}")
print(f"Загальний час: {duration:.4f} сек")
print(f"Середня швидкість (Latency): {(duration/total_records)*1000:.2f} мс/запис")

cluster.shutdown()