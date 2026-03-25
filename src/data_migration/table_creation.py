import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("CONNECTION_STRING")

def connect_to_DB():
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to the database successfully!")
    return conn

def create_table(conn):
     cur = conn.cursor()
     cur.execute("""
             CREATE TABLE IF NOT EXISTS job_postings (
                 id SERIAL PRIMARY KEY,
                 site TEXT,
                 title TEXT,
                 company TEXT,
                 location TEXT,
                 date_posted DATE,
                 job_type TEXT,
                 job_url TEXT,
                 description TEXT
             )
             """)
     conn.commit()
     print("Table created successfully!")
     
def alter_table(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE job_postings
        ADD CONSTRAINT unique_job_url UNIQUE (job_url);
    """)
    conn.commit()
    print("Table altered successfully!")
    
    
if __name__ == "__main__":
    conn = connect_to_DB()
    create_table(conn)
    alter_table(conn)
    conn.close()