import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

suppabase_url = os.getenv("API_URL")
supabase_key = os.getenv("SUPABASE_KEY")
connecting_string = os.getenv("CONNECTION_STRING")
bucket_name = os.getenv("SUPABASE_BUCKET_NAME")

print("Supabase URL:", suppabase_url)
print("Supabase Key:", supabase_key) 
print("Connecting String:", connecting_string)
print("Bucket Name:", bucket_name)

