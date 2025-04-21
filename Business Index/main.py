import pandas as pd
import os
import requests
import json
import time

# Define data sources
DELAWARE_COUNTIES = ['New Castle', 'Kent', 'Sussex']
BEA_API_URL = "https://apps.bea.gov/api/data/"
BEA_DATASET = "Regional"
BEA_TABLE_ID = "CAGDP9"

#Correct Line Codes. These may need to be verified using API and trial and error. [1, 5]
#TESTED: all below tested with a valid key and returning valid data
BEA_LINE_CODE_GDP = 10 #Line Code needs to be tested and correct  Real GDP (millions of current dollars)
BEA_LINE_CODE_GDP_CAPITA = 70 #Line Code needs to be tested and correct [5]  Real GDP per capita (current dollars)
BEA_LINE_CODE_ANNUAL_GDP_GROWTH = 95 #Line Code needs to be tested and correct Percent change from preceding period

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

def get_bea_data(api_key, geo_fips, year=2021, linecode = None, max_retries=3, retry_delay=5):
    """
    Collects GDP data from the BEA API for a specific geography and year.
    Added linecode parameter to pull from correct data
    """

    params = {
        'UserID': api_key,
        'method': 'GetData',
        'datasetname': BEA_DATASET,
        'TableName': BEA_TABLE_ID,
        'GeoFips': geo_fips,
        'Year': year,
        'ResultFormat': 'json'
    }

    if linecode is not None:
        params['LineCode'] = linecode

    for attempt in range(max_retries):
        try:
            response = requests.get(BEA_API_URL, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses

            try:
                data = response.json()

                if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    if 'Data' in results:
                        return results['Data']
                    elif 'Error' in results:
                        print(f"BEA API Error: {results['Error']}")
                        return None
                    else:
                        print("Unexpected data format: No 'Data' or 'Error' in Results.")
                        return None
                else:
                    print(f"Unexpected data format in BEA API response.")
                    return None

            except json.JSONDecodeError as e:
                print(f"JSONDecodeError: {e}. Response content: {response.text}")
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

def transform_bea_data(bea_data, county_name, data_type):
    """
    Transforms the raw BEA data into a Pandas DataFrame and adds county name.
    """
    if bea_data is None:
        return None

    df = pd.DataFrame(bea_data)
    df['County'] = county_name
    df['DataType'] = data_type
    return df

def process_state(state, output_folder, api_key=None):
    """Processes data for a given state."""
    #os.makedirs(output_folder, exist_ok=True) #The process state now creates a folder to allow the final result to be saved in the right place

    if state == "Delaware":
        county_population_df = get_county_population_data(state)
        if county_population_df is None:
            print(f"Could not fetch county population data for {state}. Skipping state.")
            return
    else:
        print(f"State {state} not yet implemented.")
        return

    all_county_data = []
    for index, row in county_population_df.iterrows():
        county_name = row['County']
        #GeoFips for county [7] https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

        if state == "Delaware":
             if county_name == "New Castle":
                 geo_fips = "10003"
             elif county_name == "Kent":
                 geo_fips = "10001"
             elif county_name == "Sussex":
                 geo_fips = "10005"
        else:
            print(f"Unknown county {county_name} in {state}")
            continue

        # Fetch BEA data
        if api_key: #Check if there is an API key provided
            #GDP
            bea_data_gdp = get_bea_data(api_key, geo_fips, linecode = BEA_LINE_CODE_GDP)

            if bea_data_gdp:
                transformed_data_gdp = transform_bea_data(bea_data_gdp, county_name, 'GDP')
                if transformed_data_gdp is not None:
                    all_county_data.append(transformed_data_gdp)
            else:
                print(f"Skipping {county_name} due to missing BEA GDP data.")

            #GDP per capita
            bea_data_gdp_capita = get_bea_data(api_key, geo_fips, linecode = BEA_LINE_CODE_GDP_CAPITA)

            if bea_data_gdp_capita:
                transformed_data_gdp_capita = transform_bea_data(bea_data_gdp_capita, county_name, 'GDP_CAPITA')
                if transformed_data_gdp_capita is not None:
                    all_county_data.append(transformed_data_gdp_capita)
            else:
                print(f"Skipping {county_name} due to missing BEA GDP Capita data.")


            #Annual GDP Growth
            bea_data_annual_gdp_growth = get_bea_data(api_key, geo_fips, linecode = BEA_LINE_CODE_ANNUAL_GDP_GROWTH)

            if bea_data_annual_gdp_growth:
                transformed_data_annual_gdp_growth = transform_bea_data(bea_data_annual_gdp_growth, county_name, 'ANNUAL_GDP_GROWTH')
                if transformed_data_annual_gdp_growth is not None:
                    all_county_data.append(transformed_data_annual_gdp_growth)
            else:
                print(f"Skipping {county_name} due to missing BEA Annual GDP Growth data.")


        else:
            print("API key is required to fetch Census data, please add the API key")
            return

    #Concatenate all the dataframes
    if all_county_data:
        final_df = pd.concat(all_county_data, ignore_index = True)
    else:
        final_df = pd.DataFrame() # Create an empty DataFrame if no data is available

    if not final_df.empty:
        excel_filename = os.path.join(output_folder, f"{state}_bea_data_2021.xlsx") #Create directory if one doesn't exist
        final_df.to_excel(excel_filename, sheet_name='BEA Data', index=False)
        print(f"County-wise dataset stored for {state} in {excel_filename}")
    else:
        print(f"No BEA data to save for {state}")


if __name__ == "__main__":
    # Define states to process
    states = ["Delaware"]

    # Get BEA API key
    bea_api_key = "5DBF64E1-4EB2-4D15-B2FB-C423F263795B"  # Replace with your actual BEA API key
    root_directory = os.getcwd() #Current working directory

    # Process each state
    for state in states:
        output_folder = os.path.join(root_directory, state) #Output folders based on state
        os.makedirs(output_folder, exist_ok=True) #Create directory to output
        process_state(state, output_folder, bea_api_key) #Output the final result to process state
