# from dotenv import load_dotenv
# import os 
# import snowflake.connector



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

import toml
import os

# Load the .toml file
config = toml.load("config.toml")

# Set environment variables
os.environ["user"] = config["snowflake"]["user"]
os.environ["password"] = config["snowflake"]["password"]
os.environ["account"] = config["snowflake"]["account"]
os.environ["warehouse"] = config["snowflake"]["warehouse"]
os.environ["database"] = config["snowflake"]["database"]
os.environ["schema"] = config["snowflake"]["schema"]
os.environ["role"] = config["snowflake"]["role"]


import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema'),
    role = os.getenv('role')
)

conn_params = {
    'user': os.getenv('user'),
    'password': os.getenv('password'),
    'account': os.getenv('account'),
    'warehouse': os.getenv('warehouse'),
    'database': os.getenv('database'),
    'schema': os.getenv('schema'),
    'role': os.getenv('role')
}

test_user = {
    "username": "testuser",
    "password": "affixcon1234"
}