from dotenv import load_dotenv
import os 
import snowflake.connector



load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema'),
    role=os.getenv('role')
    )