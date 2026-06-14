import pandas as pd

# The exact output from your Athena query
raw_header_string = "unique_entity_id,blank_(deprecated),entity_eft_indicator,cage_code,dodaac,sam_extract_code,purpose_of_registration,initial_registration_date,registration_expiration_date,last_update_date,activation_date,legal_business_name,dba_name,entity_division_name,entity_division_number,physical_address_line_1,physical_address_line_2,physical_address_city,physical_address_province_or_state,physical_address_zip_postal_code,physical_address_zip_code_+4,physical_address_country_code,physical_address_congressional_district,d&b_open_data_flag,entity_start_date,fiscal_year_end_close_date,entity_url,entity_structure,state_of_incorporation,country_of_incorporation,business_type_counter,bus_type_string,primary_naics,naics_code_counter,naics_code_string,psc_code_counter,psc_code_string,credit_card_usage,correspondence_flag,mailing_address_line_1,mailing_address_line_2,mailing_address_city,mailing_address_zip_postal_code,mailing_address_zip_code_+4,mailing_address_country,mailing_address_state_or_province,govt_bus_poc_first_name,govt_bus_poc_middle_initial,govt_bus_poc_last_name,govt_bus_poc_title,govt_bus_poc_st_add_1,govt_bus_poc_st_add_2,govt_bus_poc_city,govt_bus_poc_zip_postal_code,govt_bus_poc_zip_code_+4,govt_bus_poc_country_code,govt_bus_poc_state_or_province,alt_govt_bus_poc_first_name,alt_govt_bus_poc_middle_initial,alt_govt_bus_poc_last_name,alt_govt_bus_poc_title,alt_govt_bus_poc_st_add_1,alt_govt_bus_poc_st_add_2,alt_govt_bus_poc_city,alt_govt_bus_poc_zip_postal_code,alt_govt_bus_poc_zip_code_+4,alt_govt_bus_poc_country_code,alt_govt_bus_poc_state_or_province,past_perf_poc_poc__first_name,past_perf_poc_poc__middle_initial,past_perf_poc_poc__last_name,past_perf_poc_poc__title,past_perf_poc_st_add_1,past_perf_poc_st_add_2,past_perf_poc_city,past_perf_poc_zip_postal_code,past_perf_poc_zip_code_+4,past_perf_poc_country_code,past_perf_poc_state_or_province,alt_past_perf_poc_first_name,alt_past_perf_poc_middle_initial,alt_past_perf_poc_last_name,alt_past_perf_poc_title,alt_past_perf_poc_st_add_1,alt_past_perf_poc_st_add_2,alt_past_perf_poc_city,alt_past_perf_poc_zip_postal_code,alt_past_perf_poc_zip_code_+4,alt_past_perf_poc_country_code,alt_past_perf_poc_state_or_province,elec_bus_poc_first_name,elec_bus_poc_middle_initial,elec_bus_poc_last_name,elec_bus_poc_title,elec_bus_poc_st_add_1,elec_bus_poc_st_add_2,elec_bus_poc_city,elec_bus_poc_zip_postal_code,elec_bus_poc_zip_code_+4,elec_bus_poc_country_code,elec_bus_poc_state_or_province,alt_elec_poc_bus_poc_first_name,alt_elec_poc_bus_poc_middle_initial,alt_elec_poc_bus_poc_last_name,alt_elec_poc_bus_poc_title,alt_elec_poc_bus_st_add_1,alt_elec_poc_bus_st_add_2,alt_elec_poc_bus_city,alt_elec_poc_bus_zip_postal_code,alt_elec_poc_bus_zip_code_+4,alt_elec_poc_bus_country_code,alt_elec_poc_bus_state_or_province,_c112,_c113,_c114,_c115,_c116,_c117,_c118,_c119,_c120,_c121,_c122,_c123,_c124,_c125,_c126,_c127,_c128,_c129,_c130,_c131,_c132,_c133,_c134,_c135,_c136,_c137,_c138,_c139,_c140,_c141,_c142"

# Automatically converts the string into a list for pandas
sam_headers = raw_header_string.split(',')

# File paths
input_file = '/Users/tompetterson/Downloads/SAM_PUBLIC_MONTHLY_V2_20260503.dat'
output_file = '/Users/tompetterson/Downloads/sam_fixed_zeros_2026.csv'

print(f"Reading raw file from: {input_file}...")

# Read the raw .dat file, forcing all fields to remain as text
df = pd.read_csv(
    input_file, 
    sep='|',              
    header=None, 
    names=sam_headers, 
    dtype=str,            # CRITICAL: Preserves leading zeros
    engine='python',
    on_bad_lines='skip'   
)

print("Processing complete. Saving clean CSV...")

# Export as comma-separated CSV with headers
df.to_csv(output_file, index=False)

print(f"Success! Clean file ready for S3 upload: {output_file}")