# import textdistance
# import pandas as pd
# # from template import conn
# from utils import apply_name_matching, address_parsing  # Import necessary functions
# import snowflake.connector
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import uvicorn


# app = FastAPI()

# conn = snowflake.connector.connect(
#     user = "TuanSeedin",
#     password = "J!2af!G1j",
#     account = "tannder-lz30435",
#     warehouse = "COMPUTE_WH",
#     database = "DATA_VERIFICATION",
#     schema = "PUBLIC",
#     role = "ACCOUNTADMIN"
# )

# class UserData(BaseModel):
#     first_name: str
#     middle_name: str
#     sur_name: str
#     dob: str
#     address_line1: str
#     suburb: str
#     state: str
#     postcode: str
#     mobile: str
#     email: str

# def verify_user(data):
#     try:
#         cursor = conn.cursor()

#         # query = f"""
#         #     WITH InputData AS (
#         #         SELECT
#         #             '{data['first_name']}' AS first_name_input,
#         #             '{data['middle_name']}' AS middle_name_input,
#         #             '{data['sur_name']}' AS sur_name_input,
#         #             '{data['dob']}' AS dob_input
#         #     )
#         #     SELECT
#         #         First_name, middle_name, sur_name, dob, ad1, suburb, state, postcode, PHONE2_MOBILE, EMAILADDRESS
#         #     FROM
#         #         DATA_VERIFICATION.PUBLIC.AU_RESIDENTIAL AS resident,
#         #         InputData AS input
#         #     WHERE
#         #         (
#         #             (LOWER(input.sur_name_input) IS NOT NULL AND LOWER(input.sur_name_input) != '' AND LOWER(resident.sur_name) LIKE LOWER(input.sur_name_input))
#         #             OR (LOWER(input.middle_name_input) IS NOT NULL AND LOWER(input.middle_name_input) != '' AND LOWER(resident.middle_name) = LOWER(input.middle_name_input))
#         #             OR (LOWER(input.first_name_input) IS NOT NULL AND LOWER(input.first_name_input) != '' AND LOWER(resident.first_name) = LOWER(input.first_name_input))
#         #             AND (input.dob_input IS NOT NULL AND input.dob_input != '' AND resident.DOB = input.dob_input)
#         #         )
#         #     LIMIT 1
#         # """

#         query = "select * from DATA_VERIFICATION.PUBLIC.AU_RESIDENTIAL AS resident limit 1"

#         cursor.execute(query)
#         df = cursor.fetch_pandas_all()

#         if df.empty:
#             raise HTTPException(status_code=404, detail="No match found")

#         # Perform similarity matching
#         df['full_name_similarity'] = (textdistance.jaro_winkler(df['FIRST_NAME'][0].lower(), data['first_name'].lower()) * 100)
#         df['address_line_similarity'] = (textdistance.jaro_winkler(df['AD1'][0].lower(), data['address_line1'].lower()) * 100)

#         # More matching logic...

#         # Return the verification results
#         return {
#             "name_similarity": df['full_name_similarity'][0],
#             "address_similarity": df['address_line_similarity'][0],
#             "results": df.to_dict()
#         }
    
#     except snowflake.connector.errors.ProgrammingError as e:
#         raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

#     finally:
#         cursor.close()

# @app.get("/")
# def read_root():
#     return {"message": "Welcome to the Data Verification API"}

# if __name__ == "__main__":
#     uvicorn.run("main:app", host="0.0.0.0", port=8000)




from fastapi import FastAPI, HTTPException
import snowflake.connector
import uvicorn

app = FastAPI()

# Snowflake connection setup
conn = None

def get_snowflake_connection():
    """Establish and return a Snowflake connection."""
    global conn
    if conn is None:
        conn = snowflake.connector.connect(
            user = "TuanSeedin",
            password = "J!2af!G1j",
            account = "tannder-lz30435",
            warehouse = "COMPUTE_WH",
            database = "DATA_VERIFICATION",
            schema = "PUBLIC",
            role = "ACCOUNTADMIN"
        )
    return conn

@app.on_event("startup")
async def startup():
    """Open the connection to Snowflake when the app starts."""
    global conn
    try:
        conn = get_snowflake_connection()
        print("Connected to Snowflake")
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Could not connect to Snowflake")

@app.on_event("shutdown")
async def shutdown():
    """Close the connection to Snowflake when the app shuts down."""
    global conn
    if conn:
        conn.close()
        print("Snowflake connection closed")

# Health check endpoint to verify if the Snowflake connection is successful
@app.get("/check-connection/")
def check_connection():
    try:
        # Attempt to connect and execute a simple query
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        return {"message": "Connection to Snowflake successful", "version": version}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to Snowflake: {e}")
    finally:
        cursor.close()

# Root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to the Data Verification API"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
