# from dotenv import load_dotenv
import os 
import snowflake.connector



# load_dotenv()
# conn = snowflake.connector.connect(
#     user=os.getenv('user'),
#     password=os.getenv('password'),
#     account=os.getenv('account'),
#     warehouse=os.getenv('warehouse'),
#     database=os.getenv('database'),
#     schema=os.getenv('schema'),
#     role=os.getenv('role')
#     )

conn = snowflake.connector.connect(
    user = "TuanSeedin",
    password = "J!2af!G1j",
    account = "tannder-lz30435",
    warehouse = "COMPUTE_WH",
    database = "DATA_VERIFICATION",
    schema = "PUBLIC",
    role = "ACCOUNTADMIN"
)