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
os.environ["user"] = config["database"]["user"]
os.environ["password"] = config["database"]["password"]
os.environ["account"] = config["database"]["account"]
os.environ["warehouse"] = config["database"]["warehouse"]
os.environ["database"] = config["database"]["database"]
os.environ["schema"] = config["database"]["schema"]
os.environ["role"] = config["database"]["role"]


import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema'),
    role = os.getenv('role'),
)
