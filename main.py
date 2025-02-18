from fastapi import FastAPI
import textdistance
import pandas as pd
from template import conn,test_user
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

app = FastAPI()

security = HTTPBasic()

class UserData(BaseModel):
    country_prefix: str
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
def verify_user(data: UserData, credentials: HTTPBasicCredentials = Depends(security)):
    verify_credentials(credentials)
    
    # data = UserData(**data)
    # if data["country_prefix"] == 'au':
    #     table = "AU_RESIDENTIAL"
    # elif data["country_prefix"] == 'nz':
    #     table = "NZ_RESIDENTIAL"
    
    start_time = time.time()

    try:
        cursor = conn.cursor()
        first_name_condition = build_match_conditions(data.first_name.upper(), 'GIVEN_NAME_1','FULL_NAME') if data.first_name else "0"
        middle_name_condition = build_match_conditions(data.middle_name.upper(), 'GIVEN_NAME_2','FULL_NAME') if data.middle_name else "0"
        sur_name_condition = build_match_conditions(data.sur_name.upper(), 'SURNAME','FULL_NAME') if data.sur_name else "0"

        # addressElement1_condition = build_match_conditions(data.addressElement1.upper(), 'AD1','FULL_ADDRESS') if data.addressElement1 else "0"
        # addressElement2_condition = build_match_conditions(data.addressElement2.upper(), 'SUB_DISTRICT','FULL_ADDRESS') if data.addressElement2 else "0"
        # addressElement3_condition = build_match_conditions(data.addressElement3.upper(), 'REGENCY','FULL_ADDRESS') if data.addressElement3 else "0"
        # addressElement4_condition = build_match_conditions(data.addressElement4.upper(), 'PROVINCE','FULL_ADDRESS') if data.addressElement4 else "0"

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
                SHARED_DATA.{country_sources[data.country_prefix]['table_name']}
                -- SHARED_DATA.INDONISIA_SAMPLE_RECORDS
                -- code_schema.kyc_view
        )
        SELECT *
        FROM matched_records
        WHERE (first_name_score + middle_name_score + sur_name_score + dob_score ) >= 2
        --   + addressElement1_score + addressElement2_score + addressElement3_score \
        --   + addressElement4_score) >= 2
        ORDER BY (first_name_score + middle_name_score + sur_name_score + dob_score ) DESC
        --   + addressElement1_score + addressElement2_score + addressElement3_score \
        --   + addressElement4_score) DESC
        LIMIT 1000;
        """

                
        cursor.execute(query)
        # df = cursor.fetch_pandas_all()
        batches = cursor.fetch_pandas_batches()

        df = pd.concat(batches, ignore_index=True)

        # return df.head(1).to_dict(orient='records')[0]
        end_time = time.time()

        df['time']=int((end_time - start_time) * 1000)

        # df_selected = df.head(1)
    
        if df.empty:
            raise HTTPException(status_code=404, detail="No match found")
        
        # return {
        #     "first_name": df_selected.GIVEN_NAME_1[0],
        #     "middle_name": df_selected.GIVEN_NAME_2[0],
        #     "sur_name": df_selected.SURNAME[0],
        #     "dob": df_selected.DOB_YYYYMMDD_DATE[0],
        #     "address_line1": df_selected.AD1[0],
        #     "suburb": df_selected.SUB_DISTRICT[0],
        #     "state": df_selected.REGENCY[0],
        #     "postcode": df_selected.PROVINCE[0],
        #     "mobile": df_selected.MOBILE[0],
        #     "email": df_selected.EMAIL[0]
        # }

        full_name_input = data.first_name.lower() + " " + data.middle_name.lower() + " " + data.sur_name.lower()
        # st.write(first_name+" "+middle_name+" "+sur_name)
        threshold = 30  # Set the threshold for fuzzy match (0-100)

        # Define a function to compute fuzzy match ratio and filter rows
        def fuzzy_match(row):
            full_name = row['FULL_NAME'].lower() if row['FULL_NAME'] else ""
            return fuzz.ratio(full_name, full_name_input) >= threshold


        # resident_data = cursor.fetch_pandas_all()
        # resident_data = pd.read_sql_query(table_query, conn)
        available_columns = df.columns.tolist()
        # st.write(available_columns)

        requested_columns = ["GIVEN_NAME_1", "GIVEN_NAME_2","GIVEN_NAME_3","SURNAME","DOB_YYYYMMDD","DOB_YYYYMMDD_DATE","AD1","SUB_DISTRICT","DISTRICT","CITY","REGENCY","PROVINCE","POSTCODE","MOBILE","EMAIL"]
        
        if requested_columns:
            valid_columns = [col for col in requested_columns if col in available_columns]
        else:
            valid_columns = available_columns  
        
        # st.write(valid_columns)
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
        # # # Add DOB match score if the "DOB" column exists
        # if "DOB" in df.columns:
        #     df['match_score'] += (
        #         ((df['DOB_YYYYMMDD'].notna()) & (df['DOB_YYYYMMDD'] == dob.lower())).astype(int) * 3  # Match DOB (weight 3) if not null
        #     )


        # # # # Step 3: Sort the DataFrame by match_score in descending order
        df_sorted = df.sort_values(by='match_score', ascending=False).reset_index(drop=True)
        # st.write("df_sorted",df_sorted.head(3))
        # st.write("df_sorted",df.sort_values(by='match_score', ascending=False))
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
        # df['Given Name 3 Similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), middle_name.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), middle_name.lower())), axis=1).astype(int)
        df['Surname Similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), data.sur_name.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), data.sur_name.lower())), axis=1).astype(int)


        if df['Name Match Str'][0][0] == 'T':
            df['Given Name 1 Similarity'] = 100
        if df['Name Match Str'][0][1] == 'T':
            df['Given Name 2 Similarity'] = 100
        if df['Name Match Str'][0][2] == 'T':
            df['SurName Similarity'] = 100
        # st.write("df final",df)

        full_name_request = (data.first_name.strip() + " " + data.middle_name.strip() + " "+ data.sur_name.strip()).strip().lower()
        # full_name_matched = (df['GIVEN_NAME_1'][0].strip()+ " "+df['GIVEN_NAME_2'][0].strip()+ " "+df['SURNAME'][0].strip()).lower()
        full_name_matched = (df['FULL_NAME'][0].strip()).lower()
        name_obj = Name(full_name_request)
        match_results = {
            "Exact Match": ((df['Name Match Str'] == 'EEEE') |(df['Name Match Str'] == 'EEXE') ).any(),
            "Hyphenated Match": name_obj.hyphenated(full_name_matched),
            "Transposed Match": name_obj.transposed(full_name_matched),
            # "Middle Name Mismatch": df['Name Match Str'].str.contains('E.*E$', regex=True).any(),
            "Middle Name Mismatch": df['Name Match Str'][0].startswith('E') and df['Name Match Str'][0].endswith('E'),
            "Initial Match": name_obj.initial(full_name_matched),
            "SurName only Match": df['Name Match Str'].str.contains('^[ETMD].*E$', regex=True).any(),
            # "Middle Name Mismatch": df['Name Match Str'].str.contains('E.*E$', regex=True).any(),
            "Fuzzy Match": name_obj.fuzzy(full_name_matched),
            "Nickname Match": name_obj.nickname(full_name_matched),
            "Missing Part Match": name_obj.missing(full_name_matched),
            "Different Name": name_obj.different(full_name_matched)
        }
        # Filter out any matches that returned False
        match_results = {k: v for k, v in match_results.items() if v}
        top_match = next(iter(match_results.items()), ("No Match Found", ""))

        df['Name Match Level'] = top_match[0]
        # st.write(df.T)
        # st.write("full_name_request",full_name_request)
        # st.write("full_name_matched",full_name_matched)

        # st.write(fuzz.token_sort_ratio(full_name_request.lower(),full_name_matched.lower()))

        # df['full_name_similarity'] = (fuzz.token_set_ratio(full_name_request,full_name_matched)) 
        # df['full_name_similarity'] = df.apply(lambda row: textdistance.jaro_winkler(row['FULL_NAME'].lower(), full_name_input.lower()) * 100, axis=1)
        df['full_name_similarity'] = df.apply(lambda row: max(textdistance.jaro_winkler(row['FULL_NAME'].lower(), full_name_input.lower()) * 100, fuzz.partial_token_sort_ratio(row['FULL_NAME'].lower(), full_name_input.lower())), axis=1)
        df['full_name_similarity'] = df['full_name_similarity'].apply(lambda score: int(score) if score > 65 else 0)
        if fuzz.token_sort_ratio(full_name_request,full_name_matched)==100 and top_match[0] !='Exact Match':
            # st.write('test')
            df['full_name_similarity'] = 100

        if 'DOB' in df.columns:
            df['dob_match'] = True if df['DOB'].apply(lambda x: Dob(data.dob).exact(x))[0]=='Exact Match' else False
        # st.write("df final",df)
        

        if data.country_prefix in ('indonisia','mx'):
            df['addressElement1_similarity'] = df[['FULL_ADDRESS', 'AD1']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement1.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement1.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0) 
            weight1 = 50 if 85<=df['addressElement1_similarity'][0] <=100 else 30 if 70<=df['addressElement1_similarity'][0] <85 else 0 
            
            df['addressElement2_similarity'] = df[['FULL_ADDRESS', 'SUB_DISTRICT']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement2.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement2.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0) 
            weight2 = 20 if 85<=df['addressElement2_similarity'][0] <=100 else 25 if 70<=df['addressElement2_similarity'][0] <85 else 0 
            
            df['addressElement3_similarity'] = df[['FULL_ADDRESS', 'REGENCY']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement3.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement3.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0)  
            weight3 = 10 if 85<=df['addressElement3_similarity'][0] <=100 else  0

            # df['addressElement4_similarity'] = df['PROVINCE'].apply(lambda x: fuzz.partial_token_sort_ratio(x.lower(), addressElement4.lower())).apply(lambda score: int(score) if score > 65 else 0) 
            df['addressElement4_similarity'] = df[['FULL_ADDRESS', 'PROVINCE']].apply(lambda x: max(fuzz.token_set_ratio(x[0].lower(), data.addressElement4.lower()), fuzz.partial_token_sort_ratio(x[1].lower(), data.addressElement4.lower())), axis=1).apply(lambda score: int(score) if score > 65 else 0)
            weight4 = 20 if 85<=df['addressElement4_similarity'][0] <=100 else 0 
            

            total_weight = weight1+weight2+weight3+weight4

        else:
            total_weight = textdistance.jaro_winkler(df['ADDRESS'][0].lower().strip(), data.address_line1.lower().strip()) * 100
            df['address_line_similarity'] = total_weight



        if total_weight > 90:
            # match_level = f'Full Match, {total_weight}'
            match_level = "Full Match"
            Full_Address_Score = total_weight

        elif 70 <= total_weight <= 90:
            # match_level = f'Partial Match, {total_weight}'
            match_level = 'Partial Match'
            Full_Address_Score = total_weight
        

        else:
            match_level = 'No Match'
            Full_Address_Score = total_weight

        df['Address Match Level'] = match_level
        df['Full Address Score'] = Full_Address_Score
        # st.write('df final2',df)
        matching_levels = get_matching_level(df,data.dob,data.mobile,data.email,df['full_name_similarity'][0],total_weight)
        df['Overall Matching Level'] = ', '.join(matching_levels)
        # st.write('test',(df['MOBILE'].iloc[0])=="")
        matching_levels1 = get_mobile_email_matching_level(df,data.dob,data.mobile,data.email,df['full_name_similarity'][0],total_weight)
        df['Overall Matching Level1'] = ', '.join(matching_levels1)

        df["Overall Verified Level"] = append_based_on_verification(df,verified_by=True)
        df["Overall Contact Verified Level"] = append_mobile_email_verification(df,verified_by=True)

        if (df['Overall Verified Level'][0]  != 'No Match' ):
            df['IDV Record Verified'] = True
            # df['MultiSource'] = False
            df['IDV Multi Level Verification'] = False
            # multi_sources_score.append(df['Overall Verified Level'][0])
            # multi_mobile_email_score.append(df['Overall Contact Verified Level'][0])

            # st.write(df)

        else:
            
            # st.warning("No Matching Records: ❌")

            df['IDV Record Verified'] = False
            # df['MultiSource'] = False
            df['IDV Multi Level Verification'] = False



        df_transposed = df.T
        df_transposed.columns = ['Results']
        # dfs[table_name] = df_transposed

        """
        if df_transposed.loc['Overall Matching Level']['Results'] == '' or df_transposed.loc['Overall Verified Level']['Results'] == 'No Match':
            # st.warning("No Matching Records: ❌")
            # multi_sources_score.append('No Match')
            # multi_mobile_email_score.append('No Match')
            # if not multi_sources:
                # st.warning("No Matching Records: ❌")
                # break
        # st.write(df_transposed.loc['Overall Matching Level'])
        else:
        """
        index_col = ['Overall Verified Level','Overall Contact Verified Level','IDV Record Verified','IDV Multi Level Verification']
        
            # st.dataframe(df_transposed.loc[index_col].rename({"Overall Verified Level":"IDV Verified Level","Overall Contact Verified Level":"IDV Contact Verified Level"}), width=550, height=200)
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
        
        # df_transposed_new.loc[['IDV Record Verified','Description']] = df_transposed_new['Results'].apply(lambda x: MultiSourceLevel.get(x, ''))

        # Add tooltips for IDV Verified Level and IDV Contact Verified Level based on the mapping
        # df_transposed_new.loc['Description'] = df_transposed_new.loc['IDV Multi Level Verification', 'Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
        df_transposed_new['Description'] = df_transposed_new['Results'].apply(lambda x: IDV_Verified_Level.get(x, ''))
        

        end_time = time.time()
        # print(f"Time taken to process and return result: {end_time - start_time} seconds")

        # df_transposed_new['Description'] = df_transposed_new['Results'].apply(
                # lambda x: ', '.join([description for key, description in IDV_Verified_Level.items() if key in str(x)])
            # )
        # df_transposed_new['MultiSource_Description'] = df_transposed_new['Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
        # df_transposed_new['Description'] = df_transposed_new.loc['IDV Multi Level Verification', 'Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
        # df_transposed_new.loc['MultiSource', 'Description'] = df_transposed_new.loc['MultiSource', 'Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
        df_transposed_new.loc['IDV Record Verified', 'Description'] = SingleSourceLevel.get(df_transposed_new.loc['IDV Record Verified', 'Results'], '')
        df_transposed_new.loc['IDV Multi Level Verification', 'Description'] = MultiSourceLevel.get(df_transposed_new.loc['IDV Multi Level Verification', 'Results'], '')

            
        # if not multi_sources and df['IDV Record Verified'][0] == True:
        #     if json_view:
        #         json_data = df_transposed_new["Results"].to_dict()
        #         st.json(json_data)
        #         single_json['Summary'] = json_data
        #         if table_name not in single_json['Sources']:
        #             single_json['Sources'][table_name] = {
        #                 "Profile": [],
        #                 "Scoring": []
        #             }
        #     else:
        # st.dataframe(df_transposed_new.reindex(
            # index=['IDV Record Verified','IDV Verified Level', 'IDV Contact Verified Level',  'IDV Multi Level Verification']), width=550, height=200)
        df_transposed_new = df_transposed_new.reindex(index=['IDV Record Verified','IDV Verified Level', 'IDV Contact Verified Level',  'IDV Multi Level Verification'])
    # st.write(df_transposed)
    
        
    # if not multi_sources and not df.empty and df['IDV Record Verified'][0] == True:
    # with st.expander(":red[**Detailed Data:**]"):
    #     st.markdown(':green[**Profile**]')
        if data.country_prefix in ('au','nz'):
            df_transposed.loc['POSTCODE', 'Results'] = str(int(df_transposed.loc['POSTCODE', 'Results']))
        # st.write(df_transposed)
        # if df_transposed['Mobile'][0] != "":
        # df_transposed.loc['MOBILE', 'Results'] = (int(df_transposed.loc['MOBILE', 'Results']))
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

        # system_returned_df.rename({"FIRST_NAME":"First Name","MIDDLE_NAME":"Middle Name", "SUR_NAME":"Last Name","AD1":"Ad1",
        #                         "SUBURB":"Suburb","STATE":"State","POSTCODE":"Postcode","Phone2_Mobile":"Mobile",
        #                         "EMAILADDRESS":"EmailAddress"},inplace=True)
        # if json_view:
        # system_returned_df_json_data = system_returned_df["Results"].to_dict()
        # st.json(system_returned_df_json_data)
        # single_json['Sources'][table_name]['Profile'] = system_returned_df_json_data

        # else:
            
            # ------------------------------------ Test ------------------------------------

            system_returned_df_test = system_returned_df.copy()
            # system_returned_df_test = system_returned_df_test.loc[['GIVEN_NAME_1', 'GIVEN_NAME_2', 'GIVEN_NAME_3','GIVEN_NAME_4','GIVEN_NAME_5','GIVEN_NAME_6','GIVEN_NAME_7', 'SURNAME']]
            # system_returned_df_test = system_returned_df_test.loc[['GIVEN_NAME_1', 'GIVEN_NAME_2', 'GIVEN_NAME_3', 'SURNAME']]
            # system_returned_df_test.loc['MiddleName'] = system_returned_df_test.loc['GIVEN_NAME_2'].fillna('') + ' ' + system_returned_df_test.loc['GIVEN_NAME_3'].fillna('') 
                # ' ' + system_returned_df_test.loc['GIVEN_NAME_4'].fillna('') +
                # '' + system_returned_df_test.loc['GIVEN_NAME_5'].fillna('') + ' ' + system_returned_df_test.loc['GIVEN_NAME_6'].fillna('') + ' ' + system_returned_df_test.loc['GIVEN_NAME_7'].fillna('')
            system_returned_df.rename(index={
                'GIVEN_NAME_1': 'FirstName',
                # 'GIVEN_NAME_2': 'MiddleName',
                # 'GIVEN_NAME_3': 'MiddleName',
                'SURNAME': 'Surname'
            }, inplace=True)
            index_order = ['FirstName', 'MiddleName', 'Surname', 'AD1', 'SUB_DISTRICT', "DISTRICT","CITY","REGENCY","PROVINCE","POSTCODE","MOBILE","EMAIL", 'DOB']
            # st.write('test',system_returned_df_test.loc[['FirstName', 'MiddleName', 'SurName']])

            # ------------------------------------------------------------------------------
            # st.dataframe(system_returned_df.reindex(index=index_order), width=550, height=400) 
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

        # if json_view:
        #     similarity_returned_df_json_data = similarity_returned_df["Results"].to_dict()
        #     st.json(similarity_returned_df_json_data)
        #     single_json['Sources'][table_name]['Scoring'] = similarity_returned_df_json_data
        # else:
            # st.dataframe(similarity_returned_df.reindex(col_order), width=550, height=480)  
            similarity_returned_df = similarity_returned_df.reindex(col_order)  
        execution_time = time.time() - start_time
        print(f"Execution time: {execution_time} seconds")
        return {
            "Time": execution_time,
            "Data": df_transposed_new.to_dict(),
            "System Returned Data": system_returned_df.rename(columns={'Results':'System Returned Data'}).to_dict(),
            "Similarity Returned Data": similarity_returned_df.rename(columns={'Results':'Similarity Returned Data'}).to_dict()
        }
        # return df_transposed_new.to_dict(),system_returned_df.rename(columns={'Results':'System Returned Data'}).to_dict(), similarity_returned_df.rename(columns={'Results':'Similarity Returned Data'}).to_dict()
        
        """
        break
    with col11:        
    display_match_explanation_new() 

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
    # st.write(dfs.values())                        
    if multi_sources:  
    d = Counter(multi_sources_score)
    is_multi_source = (
    (d.get("M1", 0) >= 2) or
    (d.get("M1", 0) >= 1 and d.get("M2", 0) >= 1) or
    (d.get("M1", 0) >= 1 and d.get("N1", 0) >= 1) or
    (d.get("M2", 0) >= 1 and d.get("N1", 0) >= 1)
    )

    # Prepare DataFrame
    multi_df = pd.DataFrame(
    {
    # "IDV Multi Verified Levels": [", ".join(d.keys())],
    "IDV Verified Level" : [", ".join(multi_sources_score)],
    "IDV Contact Verified Level" : [", ".join(multi_mobile_email_score)],
    "IDV Record Verified" : [True if df['IDV Record Verified'][0]==True else False],
    # "IDV Multi Verified Levels": [", ".join([f"{key}: {value}" for key, value in d.items()])],
    "IDV Multi Level Verification": [True if is_multi_source else False] 
    }
    )
    multi_df = multi_df.T
    multi_df.columns = ['Results']
    multi_df['Description'] = multi_df['Results'].apply(lambda x: IDV_Verified_Level.get(x, ''))
    # df_transposed_new['MultiSource_Description'] = df_transposed_new['Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
    # df_transposed_new.loc['MultiSource', 'Description'] = df_transposed_new.loc['MultiSource', 'Results'].apply(lambda x: MultiSourceLevel.get(x, ''))
    multi_df.loc['IDV Record Verified', 'Description'] = SingleSourceLevel.get(multi_df.loc['IDV Record Verified', 'Results'], '')
    multi_df.loc['IDV Multi Level Verification', 'Description'] = MultiSourceLevel.get(multi_df.loc['IDV Multi Level Verification', 'Results'], '')

    with st.expander("🔖 :red[**Summary Data:**]",expanded=True):
    if json_view:
    json_data = multi_df["Results"].to_dict()
    st.json(json_data)  
    single_json['Summary'] = json_data
    # if table_name not in single_json['Detailed Data']:
    #     single_json['Detailed Data'][table_name] = {
    #         "Profile": [],
    #         "Scoring": []
    #     }
    else:                  
    st.dataframe(multi_df, width=550, height=200)

    for source,df in dfs.items():
    if source not in single_json['Sources']:
        single_json['Sources'][source] = {
            "Profile": [],
            "Scoring": []
        }
    with st.expander(f":red[**{source} - Detailed Data:**]"):
    st.markdown(':green[**Profile**]')
    if 'POSTCODE' in df.index:
        df.loc['POSTCODE', 'Results'] = str(int(df.loc['POSTCODE', 'Results']))
    if 'MOBILE' in df.index:
        df.loc['MOBILE', 'Results'] = str(int(df.loc['MOBILE', 'Results']))
    # system_returned_df = df.loc[["FIRSTNAME","MIDDLENAME","LASTNAME","DOB","ADDRESS","SUBURB",
    #                                         "STATE","POSTCODE","MOBILE","EMAIL"]]
    columns_to_select = ["FIRSTNAME", "MIDDLENAME", "LASTNAME", "DOB", "ADDRESS", 
                    "SUBURB", "STATE", "POSTCODE", "MOBILE", "EMAIL"]

    existing_columns = df.index.intersection(columns_to_select)
    system_returned_df = df.loc[existing_columns]
    # system_returned_df.rename({"FIRST_NAME":"First Name","MIDDLE_NAME":"Middle Name", "SUR_NAME":"Last Name","AD1":"Ad1",
    #                         "SUBURB":"Suburb","STATE":"State","POSTCODE":"Postcode","Phone2_Mobile":"Mobile",
    #                         "EMAILADDRESS":"EmailAddress"},inplace=True)
    if json_view:
        system_returned_df_json_data = system_returned_df["Results"].to_dict()
        st.json(system_returned_df_json_data)
        single_json['Sources'][source]['Profile'] = system_returned_df_json_data

    else:
        st.dataframe(system_returned_df, width=550, height=400) 

    similarity_columns_to_select = ["Given Name 1 Similarity","Given Name 2 Similarity","SurName Similarity",
            "full_name_similarity","Name Match Level","dob_match","address_line_similarity",
            "suburb_similarity","state_similarity","postcde_similarity","Address Match Level","Full Address Score", "Overall Verified Level",
            "Overall Contact Verified Level"]

    similarity_existing_columns = df.index.intersection(similarity_columns_to_select)
    similarity_returned_df = df.loc[similarity_existing_columns]
    # similarity_returned_df = df.loc[["Given Name 1 Similarity","Given Name 2 Similarity","SurName Similarity",
    #         "full_name_similarity","Name Match Level","dob_match","address_line_similarity",
    #         "suburb_similarity","state_similarity","postcde_similarity","Address Match Level","Full Address Score"]]

    st.markdown(':green[**Scoring**]')
    similarity_returned_df.rename({"Given Name 1 Similarity":"Given Name 1 Score", "Given Name 2 Similarity":"Given Name 2 Score",
                                "Surname Similarity":"SurName Score","full_name_similarity":"Full Name Score",
                                "dob_match":"DOB Match","address_line_similarity":"Address Line Score","suburb_similarity":"Suburb Score",
                                "state_similarity":"State Score","postcde_similarity":"Postcde Score"},inplace=True)

    col_order = ["Name Match Level", "Full Name Score", "Given Name 1 Score", "Given Name 2 Score",
                "SurName Score", "Address Match Level", "Full Address Score","Address Line Score",
                "Suburb Score","State Score", "Postcde Score","DOB Match","Overall Verified Level","Overall Contact Verified Level"]
        
    if json_view:
        similarity_returned_df_json_data = similarity_returned_df["Results"].to_dict()
        st.json(similarity_returned_df_json_data)
        single_json['Sources'][source]['Scoring'] = similarity_returned_df_json_data

    else:
        st.dataframe(similarity_returned_df.reindex(col_order), width=550, height=500)  
    with col11:
    if json_view:        
    st.json(single_json)
                                    
        return df.head(1).to_dict(orient='records')[0]

        # return {
        #     "first_name": df.GIVEN_NAME_1[0],
        #     "middle_name": df.GIVEN_NAME_2[0],
        #     "sur_name": df.SURNAME[0],
        #     "dob": df.DOB_YYYYMMDD_DATE[0],
        #     "address_line1": df.AD1[0],
        #     "suburb": df.SUB_DISTRICT[0],
        #     "state": df.REGENCY[0],
        #     "postcode": df.PROVINCE[0],
        #     "mobile": df.MOBILE[0],
        #     "email": df.EMAIL[0],
        #     'match_score': df.match_score[0]
        # }
        


        # return {
        #     'FIRST_NAME':df.FIRST_NAME[0],            
        #     'MIDDLE_NAME':df.MIDDLE_NAME[0],             
        #     'SUR_NAME':df.SUR_NAME[0],          
        #     'DOB':str(df.DOB[0]),
        #     'AD1':df.AD1[0],           
        #     "SUBURB":df.SUBURB[0],
        #     'STATE':df.STATE[0],
        #     'POSTCODE':str(df.POSTCODE[0]),
        #     'PHONE2_MOBILE':str(df.PHONE2_MOBILE[0]),
        #     'EMAILADDRESS':df.EMAILADDRESS[0],
        #     "name_match_str":df.name_match_str[0],          
        #     "first_name_similarity":"{}%".format(int(first_name_similarity)),           
        #     "middle_name_similarity":"{}%".format(int(middle_name_similarity)),          
        #     "sur_name_similarity":"{}%".format(int(sur_name_similarity)),
        #     "Name Match Level": df.Name_Match_Level[0],
        #     "full_name_similarity":  "{}%".format(int(full_name_similarity)),
        #     "dob_match": df['dob_match'][0],
        #     "Address Matching String" : df.Address_Matching_String[0],
        #     "address_line_similarity"  : "{}%".format(int(address_line_similarity)),
        #     "suburb_similarity"  : "{}%".format(int(suburb_similarity)),
        #     "state_similarity"  :  "{}%".format(int(state_similarity)),
        #     "postcde_similarity" : "{}%".format(int(postcde_similarity)),
        #     "Address_Match_Level": df.Address_Match_Level[0],
        #     "Overall Matching Level"  : Overall_Matching_Level,
        #     "Overall Verified Level "  : Overall_Verified_Level

        # }
    """
        
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

    finally:
        cursor.close()


@app.get("/")
def read_root(credentials: HTTPBasicCredentials = Depends(security)):
    user = verify_credentials(credentials)
    return {"message": "Welcome to the Data Verification API"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)