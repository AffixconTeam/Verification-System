from typing import Optional
from fastapi import FastAPI
import textdistance
import pandas as pd
from template import conn,test_user,conn_params
from utils import *
import uvicorn
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, HTTPException, status
from pydantic import BaseModel
import snowflake.connector
from fuzzywuzzy import fuzz
from input import country_sources
from datetime import date
import time
import sqlite3
from snowflake.snowpark import Session


app = FastAPI()

security = HTTPBasic()

class UserData(BaseModel):
    country_prefix: str
    id_number: Optional[str] = None
    first_name: str
    middle_name: str
    sur_name: str
    dob: str
    addressElement1: str
    addressElement2: str
    addressElement3: str
    addressElement4: str
    mobile: str
    email: str

def verify_credentials(credentials: HTTPBasicCredentials):
    if credentials.username == test_user["username"] and credentials.password == test_user["password"]:
        return {"username": credentials.username}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Basic"},
    )

@app.post("/verify_user/")
async def verify_user(data: UserData, credentials: HTTPBasicCredentials = Depends(security)):
    verify_credentials(credentials)
    
    start_time = time.time()

    try:
        session = Session.builder.configs(conn_params).create()
        first_name_condition = build_match_conditions(data.first_name.upper(), 'GIVEN_NAME_1','FULL_NAME') if data.first_name else "0"
        middle_name_condition = build_match_conditions(data.middle_name.upper(), 'GIVEN_NAME_2','FULL_NAME') if data.middle_name else "0"
        sur_name_condition = build_match_conditions(data.sur_name.upper(), 'SURNAME','FULL_NAME') if data.sur_name else "0"

        if data.id_number:
            query = f"""
                select * from PUBLIC.{country_sources[data.country_prefix]['table_name']} where lower(ID_CARD) = '{data.id_number.lower()}'
                """
        else:
            query = f"""
            WITH matched_records AS (
            SELECT
                FULL_NAME,
                GIVEN_NAME_1,
                GIVEN_NAME_2,
                GIVEN_NAME_3,
                SURNAME,
                DOB_YYYYMMDD,
                DOB_YYYYMMDD_DATE,
                FULL_ADDRESS,
                AD1,
                SUB_DISTRICT,
                DISTRICT,
                CITY,
                REGENCY,
                PROVINCE,
                POSTCODE,
                MOBILE,
                EMAIL,
                {first_name_condition} AS first_name_score,
                {middle_name_condition} AS middle_name_score,
                {sur_name_condition} AS sur_name_score,


                CASE
                    WHEN '{data.dob}' != '' AND DOB_YYYYMMDD_DATE = '{data.dob}'::DATE
                    THEN 1.0
                    ELSE 0
                END AS dob_score
            FROM
                PUBLIC.{country_sources[data.country_prefix]['table_name']}
            )
            SELECT *
            FROM matched_records
            WHERE (first_name_score + middle_name_score + sur_name_score + dob_score ) >= 2
            ORDER BY (first_name_score + middle_name_score + sur_name_score + dob_score ) DESC
            LIMIT 10;
            """


        df = session.sql(query).to_pandas()

        end_time = time.time()

        df['time']=int((end_time - start_time) * 1000)

        df_selected = df.head(1)
    
        if df.empty:
            raise HTTPException(status_code=404, detail="No match found")
    

        full_name_input = data.first_name.lower() + " " + data.middle_name.lower() + " " + data.sur_name.lower()
        threshold = 30 

        def fuzzy_match(row):
            full_name = row['FULL_NAME'].lower() if row['FULL_NAME'] else ""
            return fuzz.ratio(full_name, full_name_input) >= threshold


        available_columns = df.columns.tolist()

        requested_columns = ["GIVEN_NAME_1", "GIVEN_NAME_2","GIVEN_NAME_3","SURNAME","DOB_YYYYMMDD","DOB_YYYYMMDD_DATE","AD1","SUB_DISTRICT","DISTRICT","CITY","REGENCY","PROVINCE","POSTCODE","MOBILE","EMAIL"]
        
        if requested_columns:
            valid_columns = [col for col in requested_columns if col in available_columns]
        else:
            valid_columns = available_columns  
        
        df = df[df.apply(fuzzy_match, axis=1)]

        df['match_score'] = (
            df['FULL_NAME'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.first_name.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.first_name.lower()) * 100)) +\
            df['FULL_NAME'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.middle_name.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.middle_name.lower()) * 100)) +\
            df['FULL_NAME'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.sur_name.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.sur_name.lower()) * 100)) +\
            df['DOB_YYYYMMDD_DATE'].apply(lambda x: 75 if x and data.dob and x == date.fromisoformat(data.dob) else 0) +\
            df['FULL_ADDRESS'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.addressElement1.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.addressElement1.lower()) * 100)) +\
            df['FULL_ADDRESS'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.addressElement2.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.addressElement2.lower()) * 100)) +\
            df['FULL_ADDRESS'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.addressElement3.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.addressElement3.lower()) * 100)) +\
            df['FULL_ADDRESS'].apply(lambda x: max(fuzz.token_set_ratio(x.lower() if x else "", data.addressElement4.lower()), textdistance.jaro_winkler(x.lower() if x else "", data.addressElement4.lower()) * 100))
        )
        
        df_sorted = df.sort_values(by='match_score', ascending=False).reset_index(drop=True)
        df = df.sort_values(by='match_score', ascending=False).head(1)

        df['DOB'] = pd.to_datetime(df['DOB_YYYYMMDD'].str.split('.').str[0], format='%Y%m%d').dt.date
        df = df.fillna("").replace('<NA>','').reset_index(drop=True)
        fields = [
        ('GIVEN_NAME_1', data.first_name, 0),
        ('GIVEN_NAME_2', data.middle_name.split()[0] if data.middle_name else "", 1),
        ('SURNAME', data.sur_name, 3)
            ]
        if data.middle_name and len(data.middle_name.split()) > 1:
            fields.append(('GIVEN_NAME_3', data.middle_name.split()[1], 2))
        def update_name_str(row):
            name_Str = "XXXX" 
            for db_column, input_field, str_index in fields:
                name_Str = apply_name_matching(row, name_Str, db_column, input_field, str_index)
            return name_Str
        df['Name Match Str'] = df.apply(update_name_str, axis=1)
        df['First Name Similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), data.first_name.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), data.first_name.lower())), axis=1).astype(int)
        df['Middle Name Similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), data.middle_name.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), data.middle_name.lower())), axis=1).astype(int)
        df['Surname Similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), data.sur_name.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), data.sur_name.lower())), axis=1).astype(int)


        if df['Name Match Str'][0][0] == 'T':
            df['Given Name 1 Similarity'] = 100
        if df['Name Match Str'][0][1] == 'T':
            df['Given Name 2 Similarity'] = 100
        if df['Name Match Str'][0][2] == 'T':
            df['SurName Similarity'] = 100

        full_name_request = (data.first_name.strip() + " " + data.middle_name.strip() + " "+ data.sur_name.strip()).strip().lower()
        full_name_matched = (df['FULL_NAME'][0].strip()).lower()
        name_obj = Name(full_name_request)
        match_results = {
            "Exact Match": ((df['Name Match Str'] == 'EEEE') |(df['Name Match Str'] == 'EEXE') ).any(),
            "Hyphenated Match": name_obj.hyphenated(full_name_matched),
            "Transposed Match": name_obj.transposed(full_name_matched),
            "Middle Name Mismatch": df['Name Match Str'][0].startswith('E') and df['Name Match Str'][0].endswith('E'),
            "Initial Match": name_obj.initial(full_name_matched),
            "SurName only Match": df['Name Match Str'].str.contains('^[ETMD].*E$', regex=True).any(),
            "Fuzzy Match": name_obj.fuzzy(full_name_matched),
            "Nickname Match": name_obj.nickname(full_name_matched),
            "Missing Part Match": name_obj.missing(full_name_matched),
            "Different Name": name_obj.different(full_name_matched)
        }
        match_results = {k: v for k, v in match_results.items() if v}
        top_match = next(iter(match_results.items()), ("No Match Found", ""))

        df['Name Match Level'] = top_match[0]

        df['full_name_similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), full_name_input.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), full_name_input.lower())), axis=1)
        df['full_name_similarity'] = df['full_name_similarity'].apply(lambda score: int(score) if score > 65 else 0)
        if fuzz.token_sort_ratio(full_name_request,full_name_matched)==100 and top_match[0] !='Exact Match':
            df['full_name_similarity'] = 100

        if 'DOB' in df.columns:
            df['dob_match'] = True if df['DOB'].apply(lambda x: Dob(data.dob).exact(x))[0]=='Exact Match' else False
        

        if data.country_prefix in ('indonisia','mx'):
            df['addressElement1_similarity'] = df[['FULL_ADDRESS', 'AD1']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement1.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement1.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0) 
            weight1 = 50 if 85<=df['addressElement1_similarity'][0] <=100 else 30 if 70<=df['addressElement1_similarity'][0] <85 else 0 
            
            df['addressElement2_similarity'] = df[['FULL_ADDRESS', 'SUB_DISTRICT']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement2.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement2.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0) 
            weight2 = 20 if 85<=df['addressElement2_similarity'][0] <=100 else 25 if 70<=df['addressElement2_similarity'][0] <85 else 0 
            
            df['addressElement3_similarity'] = df[['FULL_ADDRESS', 'REGENCY']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement3.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement3.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0)  
            weight3 = 10 if 85<=df['addressElement3_similarity'][0] <=100 else  0

            df['addressElement4_similarity'] = df[['FULL_ADDRESS', 'PROVINCE']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement4.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement4.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0)
            weight4 = 20 if 85<=df['addressElement4_similarity'][0] <=100 else 0 
            

            total_weight = weight1+weight2+weight3+weight4

        else:
            total_weight = textdistance.jaro_winkler(df['ADDRESS'][0].lower().strip(), data.address_line1.lower().strip()) * 100
            df['address_line_similarity'] = total_weight



        if total_weight > 90:
            match_level = "Full Match"
            Full_Address_Score = total_weight

        elif 70 <= total_weight <= 90:
            match_level = 'Partial Match'
            Full_Address_Score = total_weight
        

        else:
            match_level = 'No Match'
            Full_Address_Score = total_weight

        df['Address Match Level'] = match_level
        df['Full Address Score'] = Full_Address_Score
        matching_levels = get_matching_level(df,data.dob,data.mobile,data.email,df['full_name_similarity'][0],total_weight)
        df['Overall Matching Level'] = ', '.join(matching_levels)
        matching_levels1 = get_mobile_email_matching_level(df,data.dob,data.mobile,data.email,df['full_name_similarity'][0],total_weight)
        df['Overall Matching Level1'] = ', '.join(matching_levels1)

        df["Overall Verified Level"] = append_based_on_verification(df,verified_by=True)
        df["Overall Contact Verified Level"] = append_mobile_email_verification(df,verified_by=True)

        if (df['Overall Verified Level'][0]  != 'No Match' ):
            df['IDV Record Verified'] = True
            df['IDV Multi Level Verification'] = False
            

        else:
            

            df['IDV Record Verified'] = False
            df['IDV Multi Level Verification'] = False



        df_transposed = df.T
        df_transposed.columns = ['Results']

 
        index_col = ['Overall Verified Level','Overall Contact Verified Level','IDV Record Verified','IDV Multi Level Verification']
        
        df_transposed_new = df_transposed.loc[index_col].rename({"Overall Verified Level":"IDV Verified Level","Overall Contact Verified Level":"IDV Contact Verified Level"})
        IDV_Verified_Level = {
            "M1": "Full Name Full Address DOB Match",
            "N1": "Full Name Full Address Match",
            "M2": "Full Name DOB Match",
            "P1": "Full Name, Mobile, and Email",
            "P2": "Full Name and Mobile",
            "P3": "Full Name and Email"}
        
        MultiSourceLevel = {
            True: "Verified by two or more independent sources",
            False: "Failed MultiSources verification"
        }
        SingleSourceLevel = {
            True: "A Verified Record with multiple attributes",
            False: "Non Verified Record"}
        ID_Level ={
            True: "ID Number Verified",
            False: "ID Number Not Verified"
        }

        df_transposed_new['Description'] = df_transposed_new['Results'].apply(lambda x: IDV_Verified_Level.get(x, ''))
        

        end_time = time.time()

        if data.id_number:

            if df_transposed.loc['ID_CARD','Results']==data.id_number:
                df_transposed_new.loc['NIK Verified', 'Results'] = True
            else :
                df_transposed_new.loc['NIK Verified', 'Results'] = False
        #     # st.write("df_transposed.loc['ID_CARD']", df_transposed.loc['ID_CARD','Results'])
            df_transposed_new.loc['NIK Verified', 'Description'] = ID_Level.get(df_transposed_new.loc['NIK Verified', 'Results'], '')
     
        df_transposed_new.loc['IDV Record Verified', 'Description'] = SingleSourceLevel.get(df_transposed_new.loc['IDV Record Verified', 'Results'], '')
        df_transposed_new.loc['IDV Multi Level Verification', 'Description'] = MultiSourceLevel.get(df_transposed_new.loc['IDV Multi Level Verification', 'Results'], '')
        
        if data.id_number:
            df_transposed_new = df_transposed_new.reindex(index=['NIK Verified','IDV Record Verified','IDV Verified Level', 'IDV Contact Verified Level',  'IDV Multi Level Verification'])
        else:
            df_transposed_new = df_transposed_new.reindex(index=['IDV Record Verified','IDV Verified Level', 'IDV Contact Verified Level',  'IDV Multi Level Verification'])

        

        if data.country_prefix in ('au','nz'):
            df_transposed.loc['POSTCODE', 'Results'] = str(int(df_transposed.loc['POSTCODE', 'Results']))

        if data.country_prefix in ('au','nz'):
            system_returned_df = df_transposed.loc[["FIRSTNAME","MIDDLENAME","LASTNAME","DOB","ADDRESS","SUBURB",
                                                "STATE","POSTCODE","MOBILE","EMAIL"]]
        else:
            # system_returned_df = df_transposed.loc[["FIRSTNAME","MIDDLENAME","LASTNAME","DOB","ADDRESS","MOBILE","EMAIL"]]    
            system_returned_df = df_transposed.loc[valid_columns]
            system_returned_df.loc['MiddleName'] = system_returned_df.loc['GIVEN_NAME_2'].fillna('') + ' ' + system_returned_df.loc['GIVEN_NAME_3'].fillna('') 

            if 'DOB_YYYYMMDD' in system_returned_df.index and 'GIVEN_NAME_2' in system_returned_df.index and 'GIVEN_NAME_3' in system_returned_df.index:
                system_returned_df = system_returned_df.drop(['DOB_YYYYMMDD','GIVEN_NAME_2','GIVEN_NAME_3'])
            if 'DOB' not in system_returned_df.index:
                system_returned_df.loc['DOB'] = df_transposed.loc['DOB']

 
            system_returned_df.rename(index={
                'GIVEN_NAME_1': 'FirstName',
                'SURNAME': 'Surname'
            }, inplace=True)
            index_order = ['FirstName', 'MiddleName', 'Surname', 'AD1', 'SUB_DISTRICT', "DISTRICT","CITY","REGENCY","PROVINCE","POSTCODE","MOBILE","EMAIL", 'DOB']

            # ------------------------------------------------------------------------------
            system_returned_df = system_returned_df.reindex(index=index_order)

        
        if data.country_prefix in ('au','nz'):
            similarity_returned_df = df_transposed.loc[["Given Name 1 Similarity","Given Name 2 Similarity","SurName Similarity",
                "full_name_similarity","Name Match Level","dob_match","address_line_similarity",
                "suburb_similarity","state_similarity","postcde_similarity","Address Match Level","Full Address Score"]]
            col_order = ["Name Match Level", "Full Name Score", "Given Name 1 Score", "Given Name 2 Score",
                    "SurName Score", "Address Match Level", "Full Address Score","Address Line Score",
                    "Suburb Score","State Score", "Postcde Score","DOB Match"]
        if data.country_prefix in ('indonisia','mx'):
            similarity_returned_df = df_transposed.loc[["First Name Similarity","Middle Name Similarity","Surname Similarity",
                "full_name_similarity","Name Match Level","dob_match","addressElement1_similarity",
                "addressElement2_similarity","addressElement3_similarity","addressElement4_similarity","Address Match Level","Full Address Score"]]
            col_order = ["Name Match Level", "Full Name Score", "First Name Score", "Middle Name Score",
                    "Surname Score", "Address Match Level", "Full Address Score","AddressElement1 Score",
                    "AddressElement2 Score","AddressElement3 Score","AddressElement4 Score","DOB Match"]
        else:
            similarity_returned_df = df_transposed.loc[["Given Name 1 Similarity","Given Name 2 Similarity","SurName Similarity",
                "full_name_similarity","Name Match Level","dob_match","address_line_similarity",
                "Address Match Level","Full Address Score"]]
            col_order = ["Name Match Level", "Full Name Score", "Given Name 1 Score", "Given Name 2 Score",
                    "SurName Score", "Address Match Level", "Full Address Score","Address Line Score",
                    "DOB Match"]

        # st.markdown(':green[**Scoring**]')
        if data.country_prefix not in ('indonisia','mx'):
            similarity_returned_df.rename({"Given Name 1 Similarity":"Given Name 1 Score", "Given Name 2 Similarity":"Given Name 2 Score",
                                    "SurName Similarity":"SurName Score","full_name_similarity":"Full Name Score",
                                    "dob_match":"DOB Match","address_line_similarity":"Address Line Score","suburb_similarity":"Suburb Score",
                                    "state_similarity":"State Score","postcde_similarity":"Postcde Score"},inplace=True)
        
        else:
            similarity_returned_df.rename({"First Name Similarity":"First Name Score", "Middle Name Similarity":"Middle Name Score",
                                    "Surname Similarity":"Surname Score","full_name_similarity":"Full Name Score",
                                    "dob_match":"DOB Match","addressElement1_similarity":"AddressElement1 Score",
                                    "addressElement2_similarity":"AddressElement2 Score","addressElement3_similarity":"AddressElement3 Score",
                                    "addressElement4_similarity":"AddressElement4 Score"},inplace=True)
 
            similarity_returned_df = similarity_returned_df.reindex(col_order)  
        execution_time = time.time() - start_time
        print(f"Execution time: {execution_time} seconds")
        return {
            "Time": execution_time,
            "Data": df_transposed_new.to_dict(),
            "System Returned Data": system_returned_df.rename(columns={'Results':'System Returned Data'}).to_dict(),
            "Similarity Returned Data": similarity_returned_df.rename(columns={'Results':'Similarity Returned Data'}).to_dict()
        }
        
    

 
        
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

    finally:
        pass


@app.get("/")
async def read_root(credentials: HTTPBasicCredentials = Depends(security)):
    user = verify_credentials(credentials)
    return {"message": "Welcome to the User Verification API"}

if __name__ == "__main__":
    uvicorn.run("test:app", host="0.0.0.0", port=8000)