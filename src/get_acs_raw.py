# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%

#The variables come from here:
# https://api.census.gov/data/2019/acs/acs1/variables.html

#importing packages
import pandas as pd
import censusdata
import gc
import re
from unittest.mock import inplace
import json
import os
import itertools
from datetime import date


# %%
def get_geoid_reference_df(geographies: dict) -> pd.DataFrame:

    """Curates the GEOID References from the geographies grab"""

    #creating a dictionary to store the GEOID references

    geoid_dict = {
        'GEOID': [],
        'STATE': [],
        'COUNTY': [], 
        'TRACT' : [], 
        'BLOCK_GROUP': []
    }

    # replace unessecary strings with blank
    replace_str = ['Summary level: 150', 'state', 'county', 'tract', 'block group', ' ', ':', ',']

    # iterate throguh items of returned geogeographies from us census api 
    for item in geographies.items():
        # get the second item in the tuple
        geoid = item[1]
        # remove strings in replace_str list in string
        geoid = re.sub('|'.join(replace_str), '', str(geoid))
        # split by > to get seperated census ids 
        geoids = str(geoid).split('>')
        # append to dictionary lists for each census id 
        geoid_dict['STATE'].append(geoids[0])
        geoid_dict['COUNTY'].append(geoids[1])
        geoid_dict['TRACT'].append(geoids[2])
        geoid_dict['BLOCK_GROUP'].append(geoids[3])

        # curate the GEOID by adding census ids together
        geoid_dict['GEOID'].append(geoids[0] + geoids[1] + geoids[2] + geoids[3])

    geoid_df = pd.DataFrame.from_dict(geoid_dict)

    geoid_df.set_index('GEOID', inplace=True)


    return geoid_df


# %%
def geoid_from_df(df: pd.DataFrame) -> pd.DataFrame:

    "Return the GEOID from the dataframe that is returned from the ACS calls"


    # reset index so we can handle the census object that is returned for the id column
    df = df.reset_index()

    df = df.rename(columns={"index": "id"})
    
    geoid = df[['id']].astype(str)

    # Get split columns on colon

    geoid = geoid['id'].str.split(":",expand=True)

    # get all of the census id columns from the split
    geoid = geoid[[3,4,5,6]]
    
    # force set the geoid columns by position
    geoid.columns = ['STATE', 'COUNTY', 'TRACT', 'BLOCK_GROUP']
    
    # add all columns together to get GEOID

    geoid['GEOID'] = geoid['STATE'] + geoid['COUNTY'] + geoid['TRACT'] + geoid['BLOCK_GROUP']

    # Apply a replacement to all columns to obtain cleaned geoid 

    geoid['GEOID'] = geoid['GEOID'].apply(lambda s: re.sub('|'.join(['> block group', '> tract', '> county']), '', str(s)))

    # drop the unessecary columns

    geoid = geoid.drop(columns=['STATE', 'COUNTY', 'TRACT', 'BLOCK_GROUP'])
    
    # concat new columns and passed dataframe

    df = pd.concat([df,geoid], axis=1)

    # drop id columns and set GEOID index so concat works

    df.drop(columns=['id'], inplace=True)

    df.reset_index()

    df.set_index('GEOID', inplace=True) 


    return df


# %%
# get census boundaries 

def get_acs_data(acs_data_dict: dict, state_code:str, year:str) -> pd.DataFrame:

    """This function will return a dataframe with the ACS data for each chunk"""

    def _chunked(dd, size):
        
        """A generator to break the passed dictionary into chunks that can be iterated over"""

        it = iter(dd)
        while True:
            p = tuple(itertools.islice(it, size))
            if not p:
                break
            yield p

    # get the chunk size from the acs_dd dictionary

    chunks = int(len(list(acs_data_dict.items()))/15)


    # get geoids for pass in state code
    boundaries = censusdata.geographies(censusdata.censusgeo([('state', state_code), ('county', '*'), ('tract', '*'), ('block group', '*')]), 'acs5', year)

    # curate reference and dataframe to concat on as we iterate through the chunks 

    geoid_df = get_geoid_reference_df(boundaries)

    # using reference curate the dataframe and assign the GEOID index to concat on

    census_df_list = []

    # iterate over the dictionary in chunks
    for chunk in _chunked(acs_dd, chunks):

        for var in chunk:
            #Get the census data from the API
            census_data_df = censusdata.download('acs5', year, censusdata.censusgeo([('state', state_code), ('county', '*'), ('tract', '*'), ('block group', '*')]), [var]).rename(columns=acs_dd)
            #Pulling out the tract number from the index
        
            # print(get_geoid(census_data_df).columns.values)

            acs_df = geoid_from_df(census_data_df)

            census_df_list.append(acs_df)
            # acs_df = None

    census_df = pd.concat([geoid_df, pd.concat(census_df_list, axis=1)], axis=1)

   # concat all the dataframes together 
    return census_df
        


# %%
# set absolute path to join on for reading and writing
abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(''))))

filename = 'acs_dd.json'

print(abspath)

# read in ACS data to obtain keys and formatted column names from source config folder

acs_dd = json.load(open(os.path.join(abspath, 'src', 'config', filename)))

# get today's date and year for the ACS data

todays_date = date.today()

# year = todays_date.year

year = '2019' # hard coded year to get since 2019 is the only thing that is available 

# run the get_acs_data function to get the ACS data for each chunk within the config folder
raw_df = get_acs_data(acs_dd, '37', year)

# export data to csv on raw path

raw_df.to_csv(os.path.join(abspath, 'data', 'raw', f'acs_{year}_raw.csv'))


# %%



