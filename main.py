import textdistance
import pandas as pd
from template import conn
from utils import *
import snowflake.connector
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from fuzzywuzzy import fuzz


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


        # Your existing logic starts here
        fields = [
            ('FIRST_NAME', data.first_name, 0),
            ('MIDDLE_NAME', data.middle_name, 1),
            ('SUR_NAME', data.sur_name, 2)
        ]

        def update_name_str(row):
            name_Str = "XXX"
            for db_column, input_field, str_index in fields:
                name_Str = apply_name_matching(row, name_Str, db_column, input_field, str_index)
            return name_Str

        # name_match_str = df.apply(update_name_str, axis=1)[0]

        df['name_match_str'] = df.apply(update_name_str, axis=1)
        first_name_similarity = max(textdistance.jaro_winkler(df.FIRST_NAME[0].lower(), data.first_name.lower()) * 100, 0) if textdistance.jaro_winkler(df.FIRST_NAME[0].lower(), data.first_name.lower()) * 100 > 65 else 0
        middle_name_similarity = max(textdistance.jaro_winkler(df.MIDDLE_NAME[0].lower(), data.middle_name.lower()) * 100, 0) if textdistance.jaro_winkler(df.MIDDLE_NAME[0].lower(), data.middle_name.lower()) * 100 > 65 else 0
        sur_name_similarity = max(textdistance.jaro_winkler(df.SUR_NAME[0].lower(), data.sur_name.lower()) * 100, 0) if textdistance.jaro_winkler(df.SUR_NAME[0].lower(), data.sur_name.lower()) * 100 > 65 else 0

        if df['name_match_str'][0][0] == 'T':
            first_name_similarity = 100
        if df['name_match_str'][0][1] == 'T':
            middle_name_similarity = 100
        if df['name_match_str'][0][2] == 'T':
           sur_name_similarity = 100

        full_name_request = (data.first_name.strip() + " " + data.middle_name.strip() + " "+ data.sur_name.strip()).strip().lower()
        full_name_matched = (df.FIRST_NAME[0].strip()+ " "+df.MIDDLE_NAME[0].strip()+ " "+df.SUR_NAME[0].strip()).lower()
        name_obj = Name(full_name_request)
        
        # # Apply the different matching methods from the Name class
        match_results = {
            "Exact Match": (df['name_match_str'] == 'EEE').any(),
            "Hyphenated Match": name_obj.hyphenated(full_name_matched),
            "Transposed Match": name_obj.transposed(full_name_matched),
            "Middle Name Mismatch": df['name_match_str'].str.contains('E.*E$', regex=True).any(),
            "Initial Match": name_obj.initial(full_name_matched),
            "SurName only Match": df['name_match_str'].str.contains('^[ETMD].*E$', regex=True).any(),
            "Fuzzy Match": name_obj.fuzzy(full_name_matched),
            "Nickname Match": name_obj.nickname(full_name_matched),
            "Missing Part Match": name_obj.missing(full_name_matched),
            "Different Name": name_obj.different(full_name_matched)
        }
        
        # # Filter out any matches that returned False
        match_results = {k: v for k, v in match_results.items() if v}
        top_match = next(iter(match_results.items()), ("No Match Found", ""))

        df['Name_Match_Level'] = top_match[0]
        
        full_name_similarity = max(textdistance.jaro_winkler(full_name_request,full_name_matched) * 100, 0) if textdistance.jaro_winkler(full_name_request,full_name_matched) * 100 > 65 else 0

        # full_name_similarity = (textdistance.jaro_winkler(full_name_request,full_name_matched)*100) 
        # df['full_name_similarity'] = df['full_name_similarity'].apply(lambda score: int(score) if score > 65 else 0)
        if fuzz.token_sort_ratio(full_name_request,full_name_matched)==100 and top_match[0] !='Exact Match':
            full_name_similarity = 100
            df['Name_Match_Level'] = 'Transposed Match'
        
        df['dob_match'] = df['DOB'].apply(lambda x: Dob(data.dob).exact(x))
        address_str = "XXXXXX"

        source = {
            # 'Gnaf_Pid': address_id,
            'Ad1': df["AD1"][0],
            'Suburb': df["SUBURB"][0],
            'State': df["STATE"][0],
            'Postcode': str(df["POSTCODE"][0])
        }
        source_output = address_parsing(df['AD1'][0])
        source = {**source, **source_output}
        # # # st.write(source)


        parsed_address = {
            # 'Gnaf_Pid': address_id,
            'Ad1': data.address_line1,
            'Suburb': data.suburb,
            'State': data.state,
            'Postcode': str(data.postcode)
        }
        parsed_output = address_parsing(data.address_line1)
        parsed_address = {**parsed_address, **parsed_output}
        # # # st.write(parsed_address)

        address_checker = Address(parsed_address=parsed_address,source_address=source)
        address_str=address_checker.address_line1_match(address_str)
        df['Address_Matching_String'] = address_str

        address_line_similarity = max(textdistance.jaro_winkler(df.AD1[0],data.address_line1[0]) * 100, 0) if textdistance.jaro_winkler(df.AD1[0],data.address_line1[0]) * 100 > 65 else 0
        weight1 = 40 if 90<=address_line_similarity <=100 else 30 if 85<=address_line_similarity <90 else 0 
        
        suburb_similarity = max(textdistance.jaro_winkler(df.SUBURB[0],data.suburb[0]) * 100, 0) if textdistance.jaro_winkler(df.SUBURB[0],data.suburb[0]) * 100 > 65 else 0
        weight2 = 30 if 90<=suburb_similarity <=100 else 25 if 85<=suburb_similarity <90 else 0 
        
        state_similarity = max(textdistance.jaro_winkler(df.STATE[0],data.state[0]) * 100, 0) if textdistance.jaro_winkler(df.STATE[0],data.state[0]) * 100 > 65 else 0
        weight3 = 10 if 90<=state_similarity <=100 else  0

        postcde_similarity = max(textdistance.jaro_winkler(df.POSTCODE[0],data.postcode[0]) * 100, 0)
        weight4 = 20 if postcde_similarity ==100 else 0 
        
        total_weight = weight1+weight2+weight3+weight4
        if total_weight > 90:
            match_level = f'Full Match, {total_weight}'
        elif 80 <= total_weight <= 90:
            match_level = f'Partial Match, {total_weight}'
        else:
            match_level = 'No Match'
        df['Address_Match_Level'] = match_level

        matching_levels = get_matching_level(df,data.dob,data.mobile,data.email,full_name_similarity,total_weight)
        df['Overall_Matching_Level'] = ', '.join(matching_levels)
        df["Overall_Verified_Level"] = append_based_on_verification(df,verified_by=True)

        # # st.write("source",source)
        # # st.write("parsed_address",parsed_address)
        # # st.write("address_str",address_str)
        # df_transposed = df.T
        # df_transposed.columns = ['Results']

        # return {
        #     "name_match_str":df.name_match_str[0],
        #     "first_name_similarity":first_name_similarity,
        #     "middle_name_similarity":middle_name_similarity,
        #     "sur_name_similarity":sur_name_similarity

        # }
    
        return {
            'FIRST_NAME':df.FIRST_NAME[0],            
            'MIDDLE_NAME':df.MIDDLE_NAME[0],             
            'SUR_NAME':df.SUR_NAME[0],          
            'DOB':str(df.DOB[0]),
            'AD1':df.AD1[0],           
            "SUBURB":df.SUBURB[0],
            'STATE':df.STATE[0],
            'POSTCODE':str(df.POSTCODE[0]),
            'PHONE2_MOBILE':str(df.PHONE2_MOBILE[0]),
            'EMAILADDRESS':df.EMAILADDRESS[0],
            "name_match_str":df.name_match_str[0],          
            "first_name_similarity":first_name_similarity,           
            "middle_name_similarity":middle_name_similarity,          
            "sur_name_similarity":sur_name_similarity,
            "Name Match Level": df.Name_Match_Level[0],
            "full_name_similarity":  full_name_similarity,
            "dob_match": df['dob_match'][0],
            "Address Matching String" : df.Address_Matching_String[0],
            "address_line_similarity"  : address_line_similarity,
            "suburb_similarity"  : suburb_similarity,
            "state_similarity"  :  state_similarity,
            "postcde_similarity" : postcde_similarity,
            "Address_Match_Level": df.Address_Match_Level[0],
            "Overall Matching Level"  : df.Overall_Matching_Level[0],
            "Overall Verified Level "  : df.Overall_Verified_Level[0]

        }
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

    finally:
        cursor.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Data Verification API"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)

