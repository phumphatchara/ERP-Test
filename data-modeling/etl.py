import pandas as pd
import psycopg2
import os

def process_data(cur, conn):
    # 1. โหลดข้อมูลจาก CSV
    worker_df = pd.read_csv('worker.csv')
    bonus_df = pd.read_csv('bonus.csv')
    title_df = pd.read_csv('title.csv')

    # 2. นำเข้าข้อมูลตาราง Worker
    print("Inserting worker data...")
    for _, row in worker_df.iterrows():
        cur.execute("""
            INSERT INTO worker (worker_id, first_name, last_name, salary, joining_date, department)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (worker_id) DO NOTHING
        """, (row.WORKER_ID, row.FIRST_NAME, row.LAST_NAME, row.SALARY, row.JOINING_DATE, row.DEPARTMENT))

    # 3. นำเข้าข้อมูลตาราง Bonus
    print("Inserting bonus data...")
    for _, row in bonus_df.iterrows():
        cur.execute("""
            INSERT INTO bonus (worker_ref_id, bonus_amount, bonus_date)
            VALUES (%s, %s, %s)
        """, (row.WORKER_REF_ID, row.BONUS_AMOUNT, row.BONUS_DATE))

    # 4. นำเข้าข้อมูลตาราง Title (พร้อมจัดการเงื่อนไข Executive)
    print("Inserting title data...")
    # หา ID พนักงานที่ไม่มีในตาราง Title เพื่อใส่เป็น Executive
    all_worker_ids = set(worker_df['WORKER_ID'])
    titled_worker_ids = set(title_df['WORKER_REF_ID'])
    missing_title_ids = all_worker_ids - titled_worker_ids

    # ใส่ข้อมูลที่มีอยู่
    for _, row in title_df.iterrows():
        cur.execute("""
            INSERT INTO title (worker_ref_id, worker_title, affected_from)
            VALUES (%s, %s, %s)
        """, (row.WORKER_REF_ID, row.WORKER_TITLE, row.AFFECTED_FROM))
    
    # ใส่ 'Executive' สำหรับคนที่ไม่มีตำแหน่ง
    for w_id in missing_title_ids:
        cur.execute("""
            INSERT INTO title (worker_ref_id, worker_title, affected_from)
            VALUES (%s, %s, %s)
        """, (w_id, 'Executive', None))

    conn.commit()
    print("Data ETL completed successfully.")

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()

    process_data(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()