import email
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
     
#def alter_table(conn):
#    cur = conn.cursor()
#    cur.execute("""
#        ALTER TABLE job_postings
#        ADD CONSTRAINT unique_job_url UNIQUE (job_url);
 #   """)
  #  conn.commit()
   # print("Table altered successfully!")


def create_subscribers_table(conn):
     cur = conn.cursor()
     cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
            id SERIAL PRIMARY KEY,
            preferred_channel TEXT DEFAULT 'telegram'
                CHECK (preferred_channel IN ('telegram','email','sms')),
            email TEXT,
            phone TEXT,
            telegram_chat_id TEXT UNIQUE,
            channel_verified BOOLEAN DEFAULT TRUE,
            is_active BOOLEAN DEFAULT TRUE,
            source TEXT DEFAULT 'telegram_bot',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
             """)
     conn.commit()
     print("Table created successfully!")



def create_message_logs_table(conn):
     cur = conn.cursor()
     cur.execute("""
            CREATE TABLE IF NOT EXISTS message_logs (
            id SERIAL PRIMARY KEY,
            subscriber_id INTEGER REFERENCES subscribers(id),
            update_id INTEGER REFERENCES job_postings(id),
            channel TEXT,
            message TEXT,
            status TEXT CHECK (status IN ('sent','failed','pending')),
            error_message TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
             """)
     conn.commit()
     print("Table created successfully!")


def alter_subscribers_table(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE subscribers ENABLE ROW LEVEL SECURITY;
    """)
    conn.commit()
    print("Table altered successfully!")

def create_policy_subscribers(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE POLICY "allow anon insert"
        ON subscribers FOR INSERT
        WITH CHECK (true);
    """)
    conn.commit()
    print("Table altered successfully!")


def create_indexes_telegram_idx(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_subscribers_telegram ON subscribers(telegram_chat_id);
    """)
    conn.commit()
    print("Table altered successfully!")

def create_indexes_email_idx(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_subscribers_email ON subscribers(email);
    """)
    conn.commit()
    print("Table altered successfully!")

def alter_subscribers_table_add_last_sent_date(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS last_sent_date DATE;;
    """)
    conn.commit()
    print("Table altered successfully!")

 
if __name__ == "__main__":
    conn = connect_to_DB()
    create_table(conn)
    alter_table(conn)
    create_subscribers_table(conn)
    create_message_logs_table(conn)
    alter_subscribers_table(conn)
    create_policy_subscribers(conn)
    create_indexes_telegram_idx(conn)
    create_indexes_email_idx(conn)
    alter_subscribers_table_add_last_sent_date(conn)
    conn.close()