import pandas as pd
import psycopg2
import os

def process_data(cur, conn):
    # 1. โหลดข้อมูลจาก CSV
    base_path = '/workspaces/ERP-Test/data'
    worker_df = pd.read_csv(os.path.join(base_path, 'worker.csv'))
    bonus_df = pd.read_csv(os.path.join(base_path, 'bonus.csv'))
    title_df = pd.read_csv(os.path.join(base_path, 'title.csv'))

    # --- แปลงวันที่ให้ถูกต้องก่อนเข้า Loop ---
    worker_df['JOINING_DATE'] = pd.to_datetime(worker_df['JOINING_DATE'], dayfirst=True)
    bonus_df['BONUS_DATE'] = pd.to_datetime(bonus_df['BONUS_DATE'], dayfirst=True)
    title_df['AFFECTED_FROM'] = pd.to_datetime(title_df['AFFECTED_FROM'], dayfirst=True)

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
        # จัดการค่า NaN ในกรณีที่บางแถวไม่มีวันที่
        b_date = row.BONUS_DATE if pd.notnull(row.BONUS_DATE) else None
        cur.execute("""
            INSERT INTO bonus (worker_ref_id, bonus_amount, bonus_date)
            VALUES (%s, %s, %s)
        """, (row.WORKER_REF_ID, row.BONUS_AMOUNT, b_date))

    # 4. นำเข้าข้อมูลตาราง Title
    print("Inserting title data...")
    all_worker_ids = set(worker_df['WORKER_ID'])
    titled_worker_ids = set(title_df['WORKER_REF_ID'])
    missing_title_ids = all_worker_ids - titled_worker_ids

    # ใส่ข้อมูลที่มีอยู่จากไฟล์ Title
    for _, row in title_df.iterrows():
        t_date = row.AFFECTED_FROM if pd.notnull(row.AFFECTED_FROM) else None
        cur.execute("""
            INSERT INTO title (worker_ref_id, worker_title, affected_from)
            VALUES (%s, %s, %s)
        """, (row.WORKER_REF_ID, row.WORKER_TITLE, t_date))
    
    # ใส่ 'Executive' สำหรับคนที่ไม่มีตำแหน่งในไฟล์ Title
    for w_id in missing_title_ids:
        cur.execute("""
            INSERT INTO title (worker_ref_id, worker_title, affected_from)
            VALUES (%s, %s, %s)
        """, (w_id, 'Executive', None))

    conn.commit()
    print("Data ETL completed successfully.")

def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
        cur = conn.cursor()
        process_data(cur, conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()