import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import json
import time

# Define data sources
#OHIO_COUNTIES_POPULATION_URL = "https://www.ohio-demographics.com/counties_by_population"
DELAWARE_COUNTIES = ['New Castle', 'Kent', 'Sussex'] #Delaware has 3 counties [1]
NATIONAL_AVERAGE_SPENDING = 15633 #USD [4, 11]
HIGH_SCHOOL_GRADUATION_RATE = 87 #percentage [5, 6]
ASSOCIATES_DEGREE_ATTAINMENT = 9.9 #percentage [7]
BACHELORS_DEGREE_ATTAINMENT = 24.9 #percentage [8]
ADVANCED_DEGREE_ATTAINMENT = 5 #percentage [10]

def get_county_population_data(state):
    """
    Returns county population data based on the state.
    Since hardcoding and Delaware the only state, there is no web scraping.
    """
    if state == "Delaware":
        #Hardcoded Delaware county population data [1]
        data = {'County': DELAWARE_COUNTIES} #list of counties
        df = pd.DataFrame(data)
        return df
    else:
        print(f"State {state} not yet implemented.")
        return None

def get_census_data_by_county(api_key, state_fips, county_name, year=2022, max_retries=3, retry_delay=5):
    """
    Collects census data for a specific county and year.
    It translates the county name to FIPS code and uses the FIPS code to query API
    """
    #Use a county FIPS code lookup [1]
    fips_codes = {
    'New Castle': '003', #Delaware FIPS codes
    'Kent': '001',
    'Sussex': '005',
    }

    county_fips = fips_codes.get(county_name)
    if not county_fips:
        print(f"FIPS code not found for {county_name}")
        return None

    base_url = "https://api.census.gov/data/"
    variables = "NAME,B01001_001E,B01002_001E"  # Example variables
    url = f"{base_url}{year}/acs/acs1?get={variables}&for=county:{county_fips}&in=state:{state_fips}&key={api_key}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses

            try:
                data = response.json()
                if len(data) > 1:  # Ensure there's data beyond the header
                    columns = data[0]
                    values = data[1]
                    county_data = dict(zip(columns, values))
                    return county_data
                else:
                    print(f"No data found for FIPS {state_fips}{county_fips} in {year}")
                    return None
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError: {e}. Response content: {response.text}") #Print the content to see whats inside
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max retries reached.  Skipping.")
                    return None

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Skipping.")
                return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    return None # Return None if all retries fail

def transform_census_data(census_df):
    """
    Transforms census data (example: calculating population density).
    """
    if census_df is None:
        return None #Return none if there is no data

    # Ensure 'B01001_001E' (Total Population) column exists before transformation
    if 'B01001_001E' in census_df.columns:
        census_df = census_df.rename(columns={'B01001_001E': 'Total Population'})
        census_df['Total Population'] = pd.to_numeric(census_df['Total Population'], errors='coerce') #COERCE errors
    else:
        print("Warning: 'B01001_001E' (Total Population) data not available in the census data.")
        return census_df

    # Create dummy 'Area (sq mi)' for demonstration purposes only
    # Ideally, fetch actual area data or load from another source based on FIPS code
    census_df['Area (sq mi)'] = 100  # REPLACE WITH REAL AREA DATA

    if 'Area (sq mi)' in census_df.columns and 'Total Population' in census_df.columns:
         census_df['Population Density (per sq mi)'] = census_df['Total Population'] / census_df['Area (sq mi)']
    else:
        print("Warning: 'Area (sq mi)' data not available for transformation.")
    return census_df

def get_education_data(county_name):
    """
    Collects available education indices
    """
    data = {
        'County': [county_name],
        'High School Completion (%)': [HIGH_SCHOOL_GRADUATION_RATE], #[5, 6]
        'Associates Degree / Apprenticeship (%)': [ASSOCIATES_DEGREE_ATTAINMENT], # [7]
        'Bachelors Degree (%)': [BACHELORS_DEGREE_ATTAINMENT], # [8]
        'Advanced Degree (%)' : [ADVANCED_DEGREE_ATTAINMENT] #[10]
           }
    df = pd.DataFrame(data)
    return df

def process_state(state, output_folder, api_key=None):
    """Processes data for a given state."""
    os.makedirs(output_folder, exist_ok=True)

    if state == "Delaware":
        county_population_df = get_county_population_data(state) #No URL for Delaware! [1]
        state_fips = "10" #FIPS code for Delaware
        if county_population_df is None:
            print(f"Could not fetch county population data for {state}. Skipping state.")
            return
    else:
        print(f"State {state} not yet implemented.")
        return

    all_county_data = []
    for index, row in county_population_df.iterrows():
        county_name = row['County']

        # Fetch Census data
        if api_key: #Check if there is an API key provided
            census_data = get_census_data_by_county(api_key, state_fips, county_name)

            if census_data:
                #Fetch census data
                education_data = get_education_data(county_name)

                #Create new dataframe
                new_df = pd.DataFrame([census_data])

                #Merge dataframes and add data
                new_df['County'] = county_name
                new_df['High School Completion (%)'] = education_data['High School Completion (%)'][0]
                new_df['Associates Degree / Apprenticeship (%)'] = education_data['Associates Degree / Apprenticeship (%)'][0]
                new_df['Bachelors Degree (%)'] = education_data['Bachelors Degree (%)'][0]
                new_df['Advanced Degree (%)'] = education_data['Advanced Degree (%)'][0]

                all_county_data.append(new_df) #append county data to list for creating dataframe
            else:
                print(f"Skipping {county_name} due to missing census data.")
        else:
            print("API key is required to fetch Census data, please add the API key")
            return

    # Create DataFrame and transform
    census_df = pd.concat(all_county_data) #concatinate all the dictionaries to census DF
    census_df = transform_census_data(census_df)

    if census_df is not None and not census_df.empty:
        excel_filename = os.path.join(output_folder, f"{state}_census_data_2022.xlsx")
        census_df.to_excel(excel_filename, sheet_name='Census Data', index=False)
        print(f"County-wise dataset stored for {state} in {excel_filename}")
    else:
        print(f"No census data to save for {state}")


if __name__ == "__main__":
    # Define states to process
    states = ["Delaware"]

    # Get Census API key
    census_api_key = "b88791220ddc86237b51d9719b5f28d8778a2c4a"  # Replace with your actual Census API key

    # Process each state
    for state in states:
        output_folder = f"output/{state}"
        process_state(state, output_folder, census_api_key)
