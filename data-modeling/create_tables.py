import psycopg2
from typing import NewType

PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

# Drop tables queries
table_drop_bonus = "DROP TABLE IF EXISTS bonus"
table_drop_title = "DROP TABLE IF EXISTS title"
table_drop_worker = "DROP TABLE IF EXISTS worker"

# Create tables queries
table_create_worker = """
    CREATE TABLE IF NOT EXISTS worker (
        worker_id INT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        salary INT,
        joining_date TIMESTAMP,
        department TEXT
    )
"""

table_create_bonus = """
    CREATE TABLE IF NOT EXISTS bonus (
        worker_ref_id INT,
        bonus_amount INT,
        bonus_date TIMESTAMP,
        CONSTRAINT fk_worker_bonus FOREIGN KEY(worker_ref_id) REFERENCES worker(worker_id)
    )
"""

table_create_title = """
    CREATE TABLE IF NOT EXISTS title (
        worker_ref_id INT,
        worker_title TEXT,
        affected_from TIMESTAMP,
        CONSTRAINT fk_worker_title FOREIGN KEY(worker_ref_id) REFERENCES worker(worker_id)
    )
"""

create_table_queries = [table_create_worker, table_create_bonus, table_create_title]
drop_table_queries = [table_drop_bonus, table_drop_title, table_drop_worker]

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    # เชื่อมต่อกับ Postgres ตามที่ตั้งค่าใน docker-compose
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()

    print("Dropping tables...")
    drop_tables(cur, conn)
    
    print("Creating tables...")
    create_tables(cur, conn)

    print("Tables created successfully.")
    conn.close()

if __name__ == "__main__":
    main()