import textdistance
import pandas as pd
from template import conn
from utils import apply_name_matching, address_parsing  # Import necessary functions
import snowflake.connector
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


app = FastAPI()


class UserData(BaseModel):
    first_name: str
    middle_name: str
    sur_name: str
    dob: str
    address_line1: str
    suburb: str
    state: str
    postcode: str
    mobile: str
    email: str


@app.post("/verify_user/")
def verify_user(data: UserData):
    try:
        cursor = conn.cursor()

        query = f"""
            WITH InputData AS (
                SELECT
                    '{data.first_name}' AS first_name_input,
                    '{data.middle_name}' AS middle_name_input,
                    '{data.sur_name}' AS sur_name_input,
                    '{data.dob}' AS dob_input
            )
            SELECT
                First_name, middle_name, sur_name, dob, ad1, suburb, state, postcode, PHONE2_MOBILE, EMAILADDRESS
            FROM
                DATA_VERIFICATION.PUBLIC.AU_RESIDENTIAL AS resident,
                InputData AS input
            WHERE
                (
                    (LOWER(input.sur_name_input) IS NOT NULL AND LOWER(input.sur_name_input) != '' AND LOWER(resident.sur_name) LIKE LOWER(input.sur_name_input))
                    OR (LOWER(input.middle_name_input) IS NOT NULL AND LOWER(input.middle_name_input) != '' AND LOWER(resident.middle_name) = LOWER(input.middle_name_input))
                    OR (LOWER(input.first_name_input) IS NOT NULL AND LOWER(input.first_name_input) != '' AND LOWER(resident.first_name) = LOWER(input.first_name_input))
                    AND (input.dob_input IS NOT NULL AND input.dob_input != '' AND resident.DOB = input.dob_input)
                )
            LIMIT 1
        """

        cursor.execute(query)
        df = cursor.fetch_pandas_all()

        if df.empty:
            raise HTTPException(status_code=404, detail="No match found")



        # # Perform similarity matching
        df['first_name_similarity'] = (textdistance.jaro_winkler(df.FIRST_NAME[0].lower(), data.first_name.lower()) * 100)
        df['address_line1_similarity'] = (textdistance.jaro_winkler(df.AD1[0].lower(), data.address_line1.lower()) * 100)

        # More matching logic...

        # Return the verification results
        return {
            "name_similarity": df['first_name_similarity'][0],
            "address_similarity": df['address_line1_similarity'][0]
            # "results": df.to_dict()
        }

        # return {
        #     'first':df.FIRST_NAME,

        # }
    
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

    finally:
        cursor.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Data Verification API"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)

