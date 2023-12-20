###################################################################################
# A script to load and tidy up LAD-level datasets used in the Levelling Up White
# Paper.
###################################################################################

# Import packages
import pandas as pd
import geopandas as gpd
import pickle
import os
import re
import requests
from matplotlib import pyplot as plt
import datetime
import io
from zipfile import ZipFile
from textwrap import wrap
import psycopg2
from psycopg2 import extras
from sqlalchemy import create_engine
from utils.db_config import config

###################################################################################
# set some preliminaries and helper functions
###################################################################################


# set a data folder
data_folder = 'data downloads'
parent_script = 'main_dataset_uploader.py'
# get the connection parameters
params = config(filename='geoproj_aws_db.ini')
my_nomis_uid = config(filename='nomis.ini', section='nomis')['my_nomis_uid']

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(params['user'], params['password'], params['host'], params['port'], params['database']))

# define the list of Core Cities
cc_list = ['Belfast', 'Birmingham', 'Bristol, City of', 'Cardiff', 'Glasgow City', 'Leeds', 'Liverpool', 'Manchester',
           'Newcastle upon Tyne', 'Nottingham', 'Sheffield']

# get the lad mappings to use with the above function
with psycopg2.connect(**params) as con:
    lad_mappings = pd.read_sql_query(sql='select * from lad_mappings', con=con)
    lad21_lookup = pd.read_sql_query(sql='select * from lad21_lookup', con=con)

# A function to upload data to a database table
def execute_values(df, table, con):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cur = con.cursor()
    try:
        extras.execute_values(cur, query, tuples)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    print("execute_values() done")
    cur.close()

# A function to check vintages of LAD codes
def lad_vintage_checker(test_set, code=True, countries=['England', 'Wales', 'Scotland', 'Northern Ireland']):
    lad_mappings1 = lad_mappings.merge(lad21_lookup.loc[:,['lad21cd', 'ctry21nm']], how='left', left_on='lad21cd', right_on='lad21cd')
    '''A helper function to test a set of LAD codes or names against the historic vintages in my lad_mappings
       dataframe. Note that it requires the lad mappings dataframe to be in the environment.
       test_set is a set or list of LAD codes/names to be tested. Default is codes, but set to FALSE for names.'''
    test_list = []
    if code == True:
        col_list = ['lad11cd', 'lad17cd', 'lad18cd', 'lad19cd', 'lad20cd', 'lad21cd', 'lad23cd']
    else:
        col_list = ['lad11nm', 'lad17nm', 'lad18nm', 'lad19nm', 'lad20nm', 'lad21nm', 'lad23nm']
    for col in col_list:
        missing = set(lad_mappings1[lad_mappings1['ctry21nm'].isin(countries)][col]) - set(test_set)
        extra = set(test_set) - set(lad_mappings1[lad_mappings1['ctry21nm'].isin(countries)][col])
        s = pd.Series([missing, extra], index=['In vintage but missing from test'.format(col),
                                               'In test but missing from vintage'.format(col)])
        test_list.append(s)
    test_df = pd.concat(test_list, axis=1).transpose()
    test_df.index = col_list
    return test_df





###################################################################################
# geographic lookups
###################################################################################

# NB can't download this automatically
lad_to_rgn_england_21 = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'Local_Authority_District_to_Region_(April_2021)_Lookup_in_England.csv')).drop('FID', axis=1)
lad_to_ctry_21 = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'Local_Authority_District_to_Country_(April_2021)_Lookup_in_the_United_Kingdom.csv')).drop('FID', axis=1)
lad_to_cty_21 = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'Local_Authority_District_to_County_(April_2021)_Lookup_in_England.csv')).drop('FID', axis=1)
lad_to_itl3 = pd.read_excel(os.path.join(data_folder, 'local_authority_boundaries', 'LAD21_LAU121_ITL321_ITL221_ITL121_UK_LU.xlsx'), sheet_name='LAD21_LAU121_ITL21_UK_LU', engine='openpyxl')

# merge it all into a mega lad lookup

# This approach leads to a slight problem, in that there are 4 Scottish LADs that are split into more than one ITL3 region.
# So I now remove those Scottish LADs from the ITL mappings
scotlads = lad_to_itl3['LAD21NM'].value_counts()[lad_to_itl3['LAD21NM'].value_counts()>1].index.to_list()

lad21_lookup = lad_to_ctry_21.merge(lad_to_rgn_england_21.loc[:,['LAD21CD', 'RGN21CD', 'RGN21NM']], how='left', left_on='LAD21CD', right_on='LAD21CD')\
    .merge(lad_to_cty_21.loc[:,['LAD21CD', 'CTY21NM']], how='left', left_on='LAD21CD', right_on='LAD21CD')\
    .merge(lad_to_itl3[~lad_to_itl3['LAD21NM'].isin(scotlads)].drop(['LAD21NM', 'LAU121CD', 'LAU121NM'], axis=1), how='left', left_on='LAD21CD', right_on='LAD21CD')
lad21_lookup.columns = [x.lower() for x in lad21_lookup.columns]
lad21_lookup['rgn21nm_filled'] = lad21_lookup.apply(lambda x: x.ctry21nm if pd.isnull(x.rgn21nm) else x.rgn21nm, axis=1)

# create a table to hold it
sql_string = ', '.join([x+' VARCHAR' for x in lad21_lookup.columns])
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS lad21_lookup (
                {},
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd)
                )""".format(sql_string))
    cur.close()
    con.commit()

# add the metadata
lad21_lookup['created'] = datetime.datetime.now()
lad21_lookup['parent_script'] = parent_script

# upload the data
with psycopg2.connect(**params) as con:
    execute_values(df=lad21_lookup, table='lad21_lookup', con=con)

###################################################################################
# pcode lookup
###################################################################################

# NB this has to be downloaded manually from here: https://geoportal.statistics.gov.uk/

# get postcode lookup and merge it in
pcode_lookup = pd.read_csv(os.path.join(data_folder, 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU', 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU.csv'), low_memory=False, encoding='unicode_escape')
# drop some columns and rename others
pcode_lookup = pcode_lookup.loc[:,['pcd7', 'pcd8', 'pcds', 'oa21cd',
       'lsoa21cd', 'msoa21cd', 'ladcd', 'lsoa21nm', 'msoa21nm', 'ladnm']].rename({'ladcd':'lad21cd'}, axis=1)

# add some metadata before adding to database
pcode_lookup['created'] = datetime.datetime.now()
pcode_lookup['parent_script'] = parent_script

# write to the database
with engine.begin() as conn:
    pcode_lookup.to_sql(name='pcode_lookup', con=conn, if_exists='fail', index=False)



###################################################################################
# population dataset
###################################################################################

# Get the latest year's data. NB this looks like a static URL and will need to be updated in the future.
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/mid2020/ukpopestimatesmid2020on2021geography.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2020 Population data already downloaded. Loading it.')
# tidy up the column names in the annual time series
df20 = pd.read_excel(filepath, sheet_name='MYE4', skiprows=7)
df20.columns = [datetime.date(year=int(re.sub('Mid-','',x)),month=1,day=1) if len(re.findall('Mid-',x))>0 else x for x in df20.columns]

# Get the latest year's data. NB this looks like a static URL and will need to be updated in the future.
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/mid2021/ukpopestimatesmid2021on2021geographyfinal.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2021 Population data already downloaded. Loading it.')
# tidy up the column names in the annual time series
df21 = pd.read_excel(filepath, sheet_name='MYE4', skiprows=7)
df21.columns = [datetime.date(year=int(re.sub('Mid-','',x)),month=1,day=1) if len(re.findall('Mid-',x))>0 else x for x in df21.columns]
df21 = df21.iloc[:,:4].merge(df20, how='left', left_on=['Code', 'Name', 'Geography'], right_on=['Code', 'Name', 'Geography'])

df21_long = pd.melt(df21, id_vars=['Code', 'Name', 'Geography'], var_name='year', value_name='population')
df21_long['year'] = df21_long['year'].apply(lambda x: x.year)

# calculate UK population estimates at LAD, ITL3 and ITL2 level for use elsewhere
pop_base = df21_long.merge(lad21_lookup.loc[:,['lad21nm', 'lad21cd', 'itl321cd', 'itl321nm', 'itl221cd', 'itl221nm']], how='left', left_on='Code', right_on='lad21cd')

# NB need to aggregate with skipna=FALSE because of the lack of mappings for some Scottish LADs
pop_itl3 = pop_base.groupby(['itl321cd', 'itl321nm', 'year'])['population'].agg(lambda x: x.sum(skipna=False)).reset_index()
pop_itl2 = pop_base.groupby(['itl221cd', 'itl221nm', 'year'])['population'].agg(lambda x: x.sum(skipna=False)).reset_index()
pop_lad = pop_base[pop_base['lad21cd'].notna()].loc[:,['lad21cd', 'lad21nm', 'year', 'population']]

# create an 'all geog' dataframe to upload in case we want to take raw data for higher level geographies
pop_all_geog = pop_base.loc[:,['Name', 'Geography', 'lad21nm', 'lad21cd',
       'itl321cd', 'itl321nm', 'itl221cd', 'itl221nm', 'year', 'population']].copy()
pop_all_geog.columns = [x.lower() for x in pop_all_geog.columns]


# get midyear estimates at LSOA level
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates/mid2020sape23dt2/sape23dt2mid2020lsoasyoaestimatesunformatted.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2020 Population data already downloaded. Loading it.')
xl = pd.ExcelFile(filepath, engine='openpyxl')
pop_lsoa = xl.parse(sheet_name='Mid-2020 Persons', engine='openpyxl', skiprows=4, usecols='A:G')
pop_lsoa = pop_lsoa.rename({'LSOA Code':'lsoa11cd', 'All Ages':'population', 'LA Code (2021 boundaries)':'lad21nm'}, axis=1)
pop_lsoa['year'] = 2020
pop_lsoa = pop_lsoa.loc[:,['lsoa11cd', 'lad21nm', 'year', 'population']]

# prepare for upload
# add timestamps and parent scripts...
for df in [pop_all_geog, pop_lad, pop_itl3, pop_itl2, pop_lsoa]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
pop_lad = pop_lad.fillna(psycopg2.extensions.AsIs('NULL'))
pop_itl3 = pop_itl3.fillna(psycopg2.extensions.AsIs('NULL'))
pop_itl2 = pop_itl2.fillna(psycopg2.extensions.AsIs('NULL'))
pop_lsoa = pop_lsoa.fillna(psycopg2.extensions.AsIs('NULL'))
pop_all_geog = pop_all_geog.fillna(psycopg2.extensions.AsIs('NULL'))



# create tables to hold these dataframes
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS population_lad (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                year INT,
                population BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd, year));
                
                CREATE TABLE IF NOT EXISTS population_itl2 (
                itl221cd VARCHAR,
                itl221nm VARCHAR,
                year INT,
                population BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl221cd, year));
                
                CREATE TABLE IF NOT EXISTS population_itl3 (
                itl321cd VARCHAR,
                itl321nm VARCHAR,
                year INT,
                population BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl321cd, year));
                
                CREATE TABLE IF NOT EXISTS population_lsoa (
                lsoa11cd VARCHAR,
                lad21nm VARCHAR,
                year INT,
                population BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lsoa11cd, year));
                
                CREATE TABLE IF NOT EXISTS population_all_geog (
                name VARCHAR,
                geography VARCHAR,
                lad21nm VARCHAR,
                lad21cd VARCHAR,
                itl321cd VARCHAR, 
                itl321nm VARCHAR, 
                itl221cd VARCHAR, 
                itl221nm VARCHAR,
                year INT,
                population BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (name, year));
                """)
    cur.close()
    con.commit()

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=pop_lad, table='population_lad', con=con)
    execute_values(df=pop_itl3, table='population_itl3', con=con)
    execute_values(df=pop_itl2, table='population_itl2', con=con)
    execute_values(df=pop_lsoa, table='population_lsoa', con=con)
    execute_values(df=pop_all_geog, table='population_all_geog', con=con)

###################################################################################
# Employment - LADs and ITL3
###################################################################################

# get the latest Business Register and Employment Survey (BRES) employment data from the NOMIS API
nomis_all_lad_geog = ','.join(lad21_lookup.lad21cd.to_list())
nomis_base = 'https://www.nomisweb.co.uk/api/v01/dataset/'
nomis_dataset = 'NM_189_1'
nomis_time = '&date=latest'
nomis_selection = '&industry=37748736&employment_status=1&measure=1&measures=20100'
filename = 'nomis_employment_lads.csv'
filepath = os.path.join(data_folder, filename)
api = nomis_base+nomis_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_selection
req = requests.get(api)
with open(filepath, 'wb') as output_file:
    output_file.write(req.content)
employment_bres = pd.read_csv(filepath)
# rename some columns, set other to lower, and keep only a subset of the metadata columns
employment_bres.columns = [x.lower() for x in employment_bres.columns]
employment_bres = employment_bres.rename({'geography_code':'lad21cd', 'geography_name':'lad21nm', 'date':'year', 'obs_value':'employment'}, axis=1)
employment_bres_lad_long = employment_bres.loc[:,['lad21cd', 'lad21nm', 'employment', 'year']]

# calculate UK population estimates at LAD, ITL3 and ITL2 level for use elsewhere
emp_bres_base = employment_bres_lad_long.merge(lad21_lookup.loc[:,['lad21cd', 'itl321cd', 'itl321nm', 'itl221cd', 'itl221nm']], how='left', left_on='lad21cd', right_on='lad21cd')

# NB need to aggregate with skipna=FALSE because of the lack of mappings for some Scottish LADs
emp_bres_itl3 = emp_bres_base.groupby(['itl321cd', 'itl321nm', 'year'])['employment'].agg(lambda x: x.sum(skipna=False)).reset_index()
emp_bres_itl2 = emp_bres_base.groupby(['itl221cd', 'itl221nm', 'year'])['employment'].agg(lambda x: x.sum(skipna=False)).reset_index()
emp_bres_lad = emp_bres_base[emp_bres_base['lad21cd'].notna()].loc[:,['lad21cd', 'lad21nm', 'year', 'employment']]

# Annual Population Surey / LFS
nomis_url_annual = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_17_5.data.csv?geography=1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730&date=latestMINUS70,latestMINUS66,latestMINUS62,latestMINUS58,latestMINUS54,latestMINUS50,latestMINUS46,latestMINUS42,latestMINUS38,latestMINUS34,latestMINUS30,latestMINUS26,latestMINUS22,latestMINUS18,latestMINUS14,latestMINUS10,latestMINUS6,latestMINUS2&variable=18&measures=20599,21001,21002,21003'
nomis_base = 'https://www.nomisweb.co.uk/api/v01/dataset/'
nomis_dataset = 'NM_17_5'
# NB this is done as a relative download, so if you run this again later, it will get the latest quarter minus 2, 6, 10... quarters, which may no longer be year ends
nomis_time = '&date=latestMINUS70,latestMINUS66,latestMINUS62,latestMINUS58,latestMINUS54,latestMINUS50,latestMINUS46,latestMINUS42,latestMINUS38,latestMINUS34,latestMINUS30,latestMINUS26,latestMINUS22,latestMINUS18,latestMINUS14,latestMINUS10,latestMINUS6,latestMINUS2'
nomis_selection = '&variable=18&measures=21001'
filename = 'nomis_LFS_employment_lads.csv'
filepath = os.path.join(data_folder, filename)
api = nomis_base+nomis_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_time+nomis_selection
req = requests.get(api)
with open(filepath, 'wb') as output_file:
    output_file.write(req.content)
employment_lfs = pd.read_csv(filepath)
# rename some columns, set other to lower, and keep only a subset of the metadata columns
employment_lfs.columns = [x.lower() for x in employment_lfs.columns]
employment_lfs = employment_lfs.rename({'geography_code':'lad21cd', 'geography_name':'lad21nm', 'date':'year', 'obs_value':'employment'}, axis=1)
employment_lfs_lad_long = employment_lfs.loc[:,['lad21cd', 'lad21nm', 'employment', 'year']]
employment_lfs_lad_long['year'] = employment_lfs_lad_long['year'].apply(lambda x: int(x[:4]))

# calculate UK population estimates at LAD, ITL3 and ITL2 level for use elsewhere
emp_lfs_base = employment_lfs_lad_long.merge(lad21_lookup.loc[:,['lad21cd', 'itl321cd', 'itl321nm', 'itl221cd', 'itl221nm']], how='left', left_on='lad21cd', right_on='lad21cd')

# NB need to aggregate with skipna=FALSE because of the lack of mappings for some Scottish LADs
emp_lfs_itl3 = emp_lfs_base.groupby(['itl321cd', 'itl321nm', 'year'])['employment'].agg(lambda x: x.sum(skipna=False)).reset_index()
emp_lfs_itl2 = emp_lfs_base.groupby(['itl221cd', 'itl221nm', 'year'])['employment'].agg(lambda x: x.sum(skipna=False)).reset_index()
emp_lfs_lad = emp_lfs_base[emp_lfs_base['lad21cd'].notna()].loc[:,['lad21cd', 'lad21nm', 'year', 'employment']]

# create tables to hold these dataframes
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS employment_bres_lad (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd, year));
                
                CREATE TABLE IF NOT EXISTS employment_bres_itl3 (
                itl321cd VARCHAR,
                itl321nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl321cd, year));
                
                CREATE TABLE IF NOT EXISTS employment_bres_itl2 (
                itl221cd VARCHAR,
                itl221nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl221cd, year));

                CREATE TABLE IF NOT EXISTS employment_lfs_lad (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd, year));
                
                CREATE TABLE IF NOT EXISTS employment_lfs_itl3 (
                itl321cd VARCHAR,
                itl321nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl321cd, year));
                
                CREATE TABLE IF NOT EXISTS employment_lfs_itl2 (
                itl221cd VARCHAR,
                itl221nm VARCHAR,
                year INT,
                employment BIGINT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl221cd, year));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [employment_bres_lad_long, emp_bres_itl3, emp_bres_itl2, employment_lfs_lad_long, emp_lfs_itl3, emp_lfs_itl2]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
employment_bres_lad_long = employment_lfs_lad_long.fillna(psycopg2.extensions.AsIs('NULL'))
employment_lfs_lad_long = employment_lfs_lad_long.fillna(psycopg2.extensions.AsIs('NULL'))
emp_bres_itl3 = emp_bres_itl3.fillna(psycopg2.extensions.AsIs('NULL'))
emp_bres_itl2 = emp_bres_itl2.fillna(psycopg2.extensions.AsIs('NULL'))
emp_lfs_itl3 = emp_lfs_itl3.fillna(psycopg2.extensions.AsIs('NULL'))
emp_lfs_itl2 = emp_lfs_itl2.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=employment_bres_lad_long, table='employment_bres_lad', con=con)
    execute_values(df=employment_lfs_lad_long, table='employment_lfs_lad', con=con)
    execute_values(df=emp_lfs_itl3, table='employment_lfs_itl3', con=con)
    execute_values(df=emp_lfs_itl2, table='employment_lfs_itl2', con=con)
    execute_values(df=emp_bres_itl3, table='employment_bres_itl3', con=con)
    execute_values(df=emp_bres_itl2, table='employment_bres_itl2', con=con)


###################################################################################
# Subregional productivity - LADs
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/labourproductivity/datasets/subregionalproductivitylabourproductivityindicesbylocalauthoritydistrict/current/ladproductivity.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading subregional productivity data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Subregional productivity data already exists. Loading it.')
# get the GVA per hour sheet, clean up the column names and merge in regions
hr = pd.read_excel(filepath, sheet_name='A3', skiprows=4, nrows=364)
hr.columns = ['geog_code', 'geog_name'] + [x for x in range(2004,2021,1)]
hr = hr.merge(lad21_lookup.loc[:,['lad21cd', 'rgn21nm_filled']], how='left', left_on='geog_code', right_on='lad21cd')
# melt to long
hr = pd.melt(hr, id_vars=['geog_code', 'geog_name', 'lad21cd', 'rgn21nm_filled'], var_name='year', value_name='gva_per_hr')

# get the GVA per job sheet, clean up the column names and merge in regions
job = pd.read_excel(filepath, sheet_name='B3', skiprows=4, nrows=375)
job.columns = ['geog_code', 'geog_name'] + [x for x in range(2002,2021,1)]
job = job.merge(lad21_lookup.loc[:,['lad21cd', 'rgn21nm_filled']], how='left', left_on='geog_code', right_on='lad21cd')
# melt to long
job = pd.melt(job, id_vars=['geog_code', 'geog_name', 'lad21cd', 'rgn21nm_filled'], var_name='year', value_name='gva_per_job')

# create tables to hold these dataframes
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS gva_hr_lad (
                geog_name VARCHAR,
                geog_code VARCHAR,
                lad21cd VARCHAR,
                rgn21nm_filled VARCHAR,
                year INT,
                gva_per_hr FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (geog_code, year));

                CREATE TABLE IF NOT EXISTS gva_job_lad (
                geog_name VARCHAR,
                geog_code VARCHAR,
                lad21cd VARCHAR,
                rgn21nm_filled VARCHAR,
                year INT,
                gva_per_job FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (geog_code, year));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [hr, job]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
hr = hr.fillna(psycopg2.extensions.AsIs('NULL'))
job = job.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=hr, table='gva_hr_lad', con=con)
    execute_values(df=job, table='gva_job_lad', con=con)

###################################################################################
# Subregional productivity - ITL3s
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/labourproductivity/datasets/subregionalproductivitylabourproductivitygvaperhourworkedandgvaperfilledjobindicesbyuknuts2andnuts3subregions/current/itlproductivity.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading subregional productivity data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Subregional productivity data already exists. Loading it.')
# get the GVA per hour sheet, clean up the column names and merge in regions
hr = pd.read_excel(filepath, sheet_name='A1', header=[0,1], skiprows=3, nrows=222)
hr.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2004,2021,1)]
# make it long
hr = pd.melt(hr, id_vars=['ITL level', 'ITL code', 'Region Name'], var_name='year', value_name='gva_per_hr')
hr = hr.rename({'ITL code':'itl_code', 'ITL level':'itl_level', 'Region Name':'region_name'}, axis=1)

# get the GVA per job sheet, clean up the column names and merge in regions
job = pd.read_excel(filepath, sheet_name='B3', header=[0,1], skiprows=3, nrows=222)
job.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2002,2021,1)]
job = pd.melt(job, id_vars=['ITL level', 'ITL code', 'Region Name'], var_name='year', value_name='gva_per_job')
job = job.rename({'ITL code':'itl_code', 'ITL level':'itl_level', 'Region Name':'region_name'}, axis=1)

with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS gva_hr_itl (
                itl_level VARCHAR,
                itl_code VARCHAR,
                region_name VARCHAR,
                year INT,
                gva_per_hr FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl_code, year));

                CREATE TABLE IF NOT EXISTS gva_job_itl (
                itl_level VARCHAR,
                itl_code VARCHAR,
                region_name VARCHAR,
                year INT,
                gva_per_job FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl_code, year));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [hr, job]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
hr = hr.fillna(psycopg2.extensions.AsIs('NULL'))
job = job.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=hr, table='gva_hr_itl', con=con)
    execute_values(df=job, table='gva_job_itl', con=con)

###################################################################################
# Subregional productivity - LSOAs  - NOT UPLOADED
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/uksmallareagvaestimates/1998to2020/uksmallareagvaestimates1998to202023012023150255.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading subregional productivity data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Subregional productivity data already exists. Loading it.')
lsoagva = {}
# get the GVA per hour sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='Table 1', header=[0], skiprows=1, nrows=34753, engine='openpyxl')
#df.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2004,2021,1)]



###################################################################################
# ASHE distribution of earnings, using lad21cd
###################################################################################

nomis_url = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_30_1.data.csv?geography=1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730&date=latestMINUS2-latest&sex=8&item=2,6...15&pay=7&measures=20100,20701'
nomis_base = 'https://www.nomisweb.co.uk/api/v01/dataset/'
nomis_dataset = 'NM_30_1'
# NB this is done as a relative download, so if you run this again later, it will get the latest year
# NB2 we
nomis_time = '&date=latestMINUS16-latest'
# NB I'm ignoring the confidence interval and just downloading the point estimate - this may not always be appropriate
nomis_selection = '&sex=8&item=2,6...15&pay=7&measures=20100'
nomis_uid = '&uid={}'.format(my_nomis_uid)
filename = 'nomis_ASHE_table8.csv'
filepath = os.path.join(data_folder, filename)
api = nomis_base+nomis_dataset+".data.csv"+"?geography="+nomis_all_lad_geog+nomis_time+nomis_selection+nomis_uid
req = requests.get(api)
with open(filepath, 'wb') as output_file:
    output_file.write(req.content)
ashe_t8 = pd.read_csv(filepath)

# get the columns we want
ashe_t8 = ashe_t8.loc[:,['DATE', 'GEOGRAPHY_CODE', 'GEOGRAPHY_NAME', 'ITEM_NAME', 'OBS_VALUE', 'OBS_STATUS_NAME']]
ashe_t8.columns = [x.lower() for x in ashe_t8.columns]
ashe_t8['item_name'] = ashe_t8['item_name'].apply(lambda x: re.sub('Median','50 percentile',x))
ashe_t8 = ashe_t8.rename({'item_name':'percentile', 'obs_value':'annual_gross_wage', 'geography_code':'lad21cd', 'geography_name':'lad21nm', 'date':'year'}, axis=1)
ashe_t8['percentile'] = ashe_t8['percentile'].apply(lambda x: int(x[:2]))

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS ashe_distribution_lad (
                year INT,
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                percentile INT,
                annual_gross_wage FLOAT,
                obs_status_name VARCHAR,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd, percentile, year));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [ashe_t8]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
ashe_t8 = ashe_t8.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=ashe_t8, table='ashe_distribution_lad', con=con)


###################################################################################
# life satisfaction
###################################################################################

# now get the complete, long-form dataset and add that too
url = 'https://download.ons.gov.uk/downloads/datasets/wellbeing-local-authority/editions/time-series/versions/3.csv'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading Life Satisfaction (full dataset) data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Life Satisfaction (full dataset) data already exists. Loading it.')
full_dataset = pd.read_csv(os.path.join(data_folder, filename), na_values=['[c]', '[u]', '[w]', '[x]'])

# merge in the lad lookup to identify types of geography
full_dataset = full_dataset.merge(lad21_lookup.loc[:,['lad21cd','lad21nm']], how='left', right_on='lad21cd', left_on='administrative-geography')
full_dataset = full_dataset.rename({'v4_3':'percent', 'Lower limit':'lower_limit', 'Upper limit':'upper_limit', 'Time':'year',
                                    'administrative-geography':'geog_code', 'Geography':'geog_name', 'MeasureOfWellbeing':'measure_of_wellbeing',
                                    'Estimate':'estimate'}, axis=1)
full_dataset = full_dataset.loc[:,['lad21cd','lad21nm', 'geog_code', 'geog_name', 'year', 'measure_of_wellbeing', 'estimate', 'percent', 'lower_limit', 'upper_limit']]
full_dataset['year'] = full_dataset['year'].apply(lambda x: int(x[:4]))

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS wellbeing_lad (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                geog_code VARCHAR,
                geog_name VARCHAR,
                year INT,
                measure_of_wellbeing VARCHAR,
                estimate VARCHAR,
                percent FLOAT,
                lower_limit FLOAT,
                upper_limit FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (geog_code, year, measure_of_wellbeing, estimate));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [full_dataset]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
full_dataset = full_dataset.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=full_dataset, table='wellbeing_lad', con=con)

###################################################################################
# Skills
###################################################################################

# Skills data come from the APS/LFS, which we can access via the NomisWeb API

# set a filename and filepath to save the CSV we're going to create.
# I'm doing this up front so I can check for it's existence before downloading the data again.
filename = 'nomis_download_skills.csv'
filepath = os.path.join(data_folder, filename)

# Set out some building blocks for the API call
nomis_api = "https://www.nomisweb.co.uk/api/v01/dataset/NM_17_5.data.csv?geography=1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730&date=latest&variable=290,720...722,335,344&measures=20599,21001,21002,21003&uid=0x3dc137623ce948ec3d5cc8b0b203283e1ea30c89"
nomis_base = "https://www.nomisweb.co.uk/api/v01/dataset/"
# use lad21cd form the lad lookup. This seems to drop North Northamptonshire and West Northamptonshire (I guess there was a merge in 2021?)
# But it's good to know I can use standard geographies to call the API - it should make it easy to construct queries.
nomis_all_lad_geog = ','.join(lad21_lookup.lad21cd.to_list())
nomis_dataset = "NM_17_5"
# NB this is a relative call, but the data only appear to be present in certain surveys, so be careful when updating this
nomis_time = '&date=latestMINUS6'
# NB I'm ignoring the confidence interval and just downloading the point estimate - this may not always be appropriate
nomis_selection = "&variable=290,720...722,335,344&measures=20599,21001,21002,21003"
nomis_uid = '&uid={}'.format(my_nomis_uid)

# combine the building blocks
api = nomis_base+nomis_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_time+nomis_selection+nomis_uid
req = requests.get(api)
with open(filepath, 'wb') as output_file:
    output_file.write(req.content)
skills = pd.read_csv(filepath)
skills = skills.loc[:,['DATE', 'GEOGRAPHY_NAME', 'GEOGRAPHY_CODE',
       'VARIABLE_NAME', 'MEASURES_NAME', 'OBS_VALUE', 'OBS_STATUS_NAME',]]
skills.columns = [x.lower() for x in skills.columns]
skills = skills.rename({'geography_name':'geog_name', 'geography_code':'geog_code', 'date':'year'}, axis=1)
skills['year'] = skills['year'].apply(lambda x: int(x[:4]))

# merge in the lookup data
skills = skills.merge(lad21_lookup.loc[:,['lad21cd', 'lad21nm', 'rgn21nm_filled']], how='left', left_on='geog_code', right_on='lad21cd')
skills = skills.loc[:,['lad21cd', 'lad21nm', 'geog_code', 'geog_name', 'year', 'variable_name', 'measures_name', 'obs_value',
       'obs_status_name']]

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS skills_lad (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                geog_code VARCHAR,
                geog_name VARCHAR,
                year INT,
                variable_name VARCHAR,
                measures_name VARCHAR,
                obs_value FLOAT,
                obs_status_name VARCHAR,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (geog_code, year, variable_name, measures_name));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [skills]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
skills = skills.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=skills, table='skills_lad', con=con)


###################################################################################
# VOA rateable values - NOT UPLOADED
###################################################################################

# Get the latest year's Rateable value data. NB this might be a dynamic URL.
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/903228/NDR_Floorspace_Tables__2020_MSOA_LSOA.zip'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading VOA data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('VOA rateable value data already exists. Loading it.')
voa = {}
# get the gross weekly pay spreadsheet and select the 'All' tab, then tidy up column names manually
df_list = []
for file, scat in zip(['FS_OA2.1.csv', 'FS_OA3.1.csv', 'FS_OA4.1.csv', 'FS_OA5.1.csv'],
                      ['Retail', 'Office', 'Industrial', 'Other']):
    df = pd.read_csv(io.BytesIO(ZipFile(filepath).read(file)), na_values=['.', '..']).iloc[:,2:].drop('ba_code', axis=1)
    df['scat'] = scat
    df_list.append(df)
voa_df = pd.concat(df_list, axis=0)
voa['voa_rv'] = voa_df

# repeat for floorspace
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1019757/NDR_Business_Floorspace_Tables_by_region__county__local_authority_district__middle_and_lower_super_output_area__2021.zip'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading VOA data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('VOA floorspace data already exists. Loading it.')
# get the gross weekly pay spreadsheet and select the 'All' tab, then tidy up column names manually
df_list = []
for file, scat in zip(['Table FS_OA2.1.csv', 'Table FS_OA2.1.csv', 'Table FS_OA3.1.csv', 'Table FS_OA4.1.csv', 'Table FS_OA5.1.csv'],
                      ['Total', 'Retail', 'Office', 'Industrial', 'Other']):
    df = pd.read_csv(io.BytesIO(ZipFile(filepath).read(file)), na_values=['.', '..']).drop('ba_code', axis=1)
    df['scat'] = scat
    df_list.append(df)
voa_df = pd.concat(df_list, axis=0)
voa['voa_floorspace'] = voa_df

# repeat for Special Category (SCat) data - first get number of properties...
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1086188/NDR_Stock_SCat_RV_bands_by_area_2022.zip'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading VOA data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('VOA floorspace data already exists. Loading it.')
# get the gross weekly pay spreadsheet and select the 'All' tab, then tidy up column names manually
scat_n = pd.read_csv(io.BytesIO(ZipFile(filepath).read('SCAT_AREAS_N_all.csv')), na_values=['.', '..', '-']).drop('ba_code_for_publications', axis=1)
# drop geographies other than local authority
scat_n = scat_n[scat_n['geographical_level_presented'] == 'LAUA'].copy().drop(['geographical_level_presented', 'voa_name'], axis=1).set_index('ons_area_codes')
scat_n = lad_lookup[lad_lookup['ctry21nm'].isin(['England', 'Wales'])].loc[:,['lad21cd', 'lad21nm', 'rgn21nm_filled']]\
    .merge(scat_n, how='left', left_on='lad21cd', right_index=True)

# ...then get rateable value (RV) of properties
scat_rv = pd.read_csv(io.BytesIO(ZipFile(filepath).read('SCAT_AREAS_RV_all.csv')), na_values=['.', '..', '-']).drop('ba_code_for_publications', axis=1)
# drop geographies other than local authority
scat_rv = scat_rv[scat_rv['geographical_level_presented'] == 'LAUA'].copy().drop(['geographical_level_presented', 'voa_name'], axis=1).set_index('ons_area_codes')
scat_rv = lad_lookup[lad_lookup['ctry21nm'].isin(['England', 'Wales'])].loc[:,['lad21cd', 'lad21nm', 'rgn21nm_filled']]\
    .merge(scat_rv, how='left', left_on='lad21cd', right_index=True)

# get a hierarchy of classifications for VOA Scats
scat_hierarchy = pd.read_excel(os.path.join('input_data', 'NDR_Stock_SCat_2022.xlsx'), engine='openpyxl', sheet_name='Table SC1.1', skiprows=9).iloc[1:,2:5].dropna()
# drop rows that just contained Sector or Sub-sector totals
def dropper(x):
    if len(re.findall('SECTOR', x))>0:
        out=1
    elif len(re.findall('Sub-sector', x))>0:
        out=1
    else:
        out=0
    return out
scat_hierarchy['to_drop'] = [dropper(x) for x in scat_hierarchy['name']]
scat_hierarchy = scat_hierarchy[scat_hierarchy['to_drop']==0].drop('to_drop', axis=1)
# tweak the text of sector and sub-sector to make it less annoying
scat_hierarchy['sector'] = [re.sub(' SECTOR','',x).title() for x in scat_hierarchy['sector']]
scat_hierarchy['Sub-sector'] = [re.sub(' Sub-sector','',x).title() for x in scat_hierarchy['Sub-sector']]
scat_hierarchy = scat_hierarchy.rename({'Sub-sector':'sub-sector'}, axis=1)

voa['scat_n'] = scat_n
voa['scat_rv'] = scat_rv
voa['scat_hierarchy'] = scat_hierarchy

###################################################################################
# Primary Urban Area dict
###################################################################################

# transcribe this table from the Centre for Cities 'https://www.centreforcities.org/wp-content/uploads/2022/08/2022-PUA-Table.pdf'
pua_exlondon = pd.read_fwf(os.path.join(data_folder, 'pua_definitions_exlondon.txt'), header=None)
pua_exlondon = pua_exlondon[0] + ' ' + pua_exlondon[1].fillna('') + ' ' + pua_exlondon[2].fillna('')
temp = [x.strip().split(' ',1) for x in pua_exlondon]
_key = [x[0] for x in temp]
_value = [x[1] for x in temp]
_value = [x.split(', ') for x in _value]

pua_london = pd.read_fwf(os.path.join(data_folder, 'pua_list_london.txt'), header=None)
pua_london = pua_london[0] + ' ' + pua_london[1].fillna('')
pua_london = pua_london.to_list()
pua_london = ' '.join(pua_london)
pua_london = re.sub('  ',' ',pua_london)
pua_london = pua_london.split(', ')

pua_dict = dict(zip(_key, _value))
pua_dict['London'] = pua_london

# write as a table to upload instead
pua_df = pd.DataFrame([_key, _value], index=['pua', 'lad21nm']).transpose()

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS pua_lookup (
                pua VARCHAR,
                lad21nm VARCHAR[],
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (pua));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [pua_df]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
skills = skills.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=pua_df, table='pua_lookup', con=con)

###################################################################################
# Indices of Deprivation
###################################################################################

url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/845345/File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading Indices of Deprivation data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Indices of Deprivation data already exists. Loading it.')
# Read the csv file
iod = pd.read_csv(filepath)

# drop some columns
iod = iod.iloc[:,:-5]
iod = iod.drop(['Local Authority District code (2019)',
       'Local Authority District name (2019)'], axis=1)
# rename columns for postgresql
for s in ['\(where 1 is most deprived 10% of LSOAs\)', '\(where 1 is most deprived\)', '\(', '\)']:
    iod.columns = [re.sub(s,'',x) for x in iod.columns]
iod.columns = [re.sub(' ','_',x.strip().lower()) for x in iod.columns]
iod = iod.rename({'lsoa_code_2011':'lsoa11cd', 'lsoa_name_2011':'lsoa11nm'}, axis=1)

# add timestamp and parent_script
iod['create'] = datetime.datetime.now()
iod['parent_script'] = parent_script

# try dumping this directly into the database
with engine.begin() as conn:
    iod.to_sql(name='iod_2019', con=conn, if_exists='fail', index=False)

####################################################
# Get experimental GFCF by region for ITL3 regions
####################################################
url = 'https://www.ons.gov.uk/file?uri=/economy/regionalaccounts/grossdisposablehouseholdincome/datasets/experimentalregionalgrossfixedcapitalformationgfcfestimatesbyassettype/1997to2020/updatedexperimentalregionalgfcf19972020byassetandindustry.xlsx'
data_name = 'Regional GFCF by asset type'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')

regional_GFCF = []
for sheet in ['1.3', '2.3', '3.3', '4.3', '5.3', '6.3']:
    temp_df = xl.parse(sheet_name=sheet, skiprows=3, header=0, na_values=['[w]', '[low]'], nrows=3960, engine='openpyxl')
    regional_GFCF.append(temp_df)
regional_GFCF = pd.concat(regional_GFCF, axis=0)
del temp_df
# melt to long-form before outputting for use in Jupyter?
regional_GFCF = pd.melt(regional_GFCF, id_vars=['Asset', 'ITL3 name', 'ITL3 code', 'ITL2 name', 'ITL2 code',
       'ITL1 name', 'ITL1 code', 'SIC07 industry code', 'SIC07 industry name'], var_name='Year', value_name='value')
regional_GFCF['Year'] = regional_GFCF['Year'].astype(int)

#### Now calculate regional_GFCF per head, using population data from the data dictionary

# download population_itl3 and employment_itl3
# NB I can't use the dataframe from above in the script because the NAs have been replaced with some niche datatype
# for uploading to postgresql
with psycopg2.connect(**params) as con:
    pop_itl3 = pd.read_sql_query(sql='select * from population_itl3', con=con)
    emp_itl3 = pd.read_sql_query(sql='select * from employment_lfs_itl3', con=con)

# load population (i.e. by residence) and employment (by job location) data and calculate per head and per job values
itl3_GFCF_per_head = regional_GFCF.merge(pop_itl3, how='left', left_on=['ITL3 code', 'Year'], right_on=['itl321cd', 'year'])\
                                    .merge(emp_itl3, how='left', left_on=['ITL3 code', 'Year'], right_on=['itl321cd', 'year'])
itl3_GFCF_per_head['value_per_head'] = 1000000 * itl3_GFCF_per_head['value'].div(itl3_GFCF_per_head['population'])
itl3_GFCF_per_head['value_per_job'] = 1000000 * itl3_GFCF_per_head['value'].div(itl3_GFCF_per_head['employment'])
itl3_GFCF_per_head = itl3_GFCF_per_head.loc[:,['ITL3 code', 'ITL3 name', 'Year', 'Asset', 'SIC07 industry code', 'SIC07 industry name',
                                               'value', 'population', 'employment', 'value_per_head', 'value_per_job']].rename({'ITL3 code':'itl321cd', 'ITL3 name':'itl321nm'}, axis=1)
itl3_GFCF_per_head.columns = [re.sub(' ','_',x.strip().lower()) for x in itl3_GFCF_per_head.columns]

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS itl3_gfcf (
                itl321cd VARCHAR,
                itl321nm VARCHAR,
                year INT,
                asset VARCHAR,
                sic07_industry_code VARCHAR,
                sic07_industry_name VARCHAR,
                value FLOAT,
                population FLOAT,
                employment FLOAT,
                value_per_head FLOAT,
                value_per_job FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl321cd, year, sic07_industry_code, asset));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [itl3_GFCF_per_head]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
itl3_GFCF_per_head = itl3_GFCF_per_head.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=itl3_GFCF_per_head, table='itl3_gfcf', con=con)


####################################################
# Get experimental GFCF by region for ITL2 regions
####################################################

url = 'https://www.ons.gov.uk/file?uri=/economy/regionalaccounts/grossdisposablehouseholdincome/datasets/experimentalregionalgrossfixedcapitalformationgfcfestimatesbyassettype/1997to2020/updatedexperimentalregionalgfcf19972020byassetandindustry.xlsx'
data_name = 'Regional GFCF by asset type'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')

itl2_GFCF = xl.parse(sheet_name='1.2', skiprows=3, header=0, na_values=['[w]', '[low]'], nrows=924, engine='openpyxl')
# melt to long-form before outputting for use in Jupyter?
itl2_GFCF = pd.melt(itl2_GFCF, id_vars=['Asset', 'ITL2 name', 'ITL2 code',
       'ITL1 name', 'ITL1 code', 'SIC07 industry code', 'SIC07 industry name'], var_name='Year', value_name='value')
itl2_GFCF['Year'] = itl2_GFCF['Year'].astype(int)

#### Now calculate itl2_GFCF per head and per job, using population data from the data dictionary

# download pop_itl2
with psycopg2.connect(**params) as con:
    pop_itl2 = pd.read_sql_query(sql='select * from population_itl2', con=con)
    emp_itl2 = pd.read_sql_query(sql='select * from employment_lfs_itl2', con=con)

# load population (i.e. by residence) and employment (by job location) data and calculate per head and per job values
itl2_GFCF_per_head = itl2_GFCF.merge(pop_itl2, how='left', left_on=['ITL2 code', 'Year'], right_on=['itl221cd', 'year'])\
                                    .merge(emp_itl2, how='left', left_on=['ITL2 code', 'Year'], right_on=['itl221cd', 'year'])
itl2_GFCF_per_head['value_per_head'] = 1000000 * itl2_GFCF_per_head['value'].div(itl2_GFCF_per_head['population'])
itl2_GFCF_per_head['value_per_job'] = 1000000 * itl2_GFCF_per_head['value'].div(itl2_GFCF_per_head['employment'])
itl2_GFCF_per_head = itl2_GFCF_per_head.loc[:,['ITL2 code', 'ITL2 name', 'Year', 'Asset', 'SIC07 industry code', 'SIC07 industry name',
                                               'value', 'population', 'employment', 'value_per_head', 'value_per_job']].rename({'ITL2 code':'itl221cd', 'ITL2 name':'itl221nm'}, axis=1)
itl2_GFCF_per_head.columns = [re.sub(' ','_',x.strip().lower()) for x in itl2_GFCF_per_head.columns]

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS itl2_gfcf (
                itl221cd VARCHAR,
                itl221nm VARCHAR,
                year INT,
                asset VARCHAR,
                sic07_industry_code VARCHAR,
                sic07_industry_name VARCHAR,
                value FLOAT,
                population FLOAT,
                employment FLOAT,
                value_per_head FLOAT,
                value_per_job FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (itl221cd, year, sic07_industry_code, asset));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [itl2_GFCF_per_head]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
itl2_GFCF_per_head = itl2_GFCF_per_head.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=itl2_GFCF_per_head, table='itl2_gfcf', con=con)

################################################
# Get the LA capital expenditure data
################################################
#url_22_23_forecast = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1086517/CER_2022-23_A1_capital_expenditure_and_receipts_by_service_and_category.ods'
url_18_19 = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/842038/COR_2018-19_outputs_COR_A1.xlsx'
url_19_20 = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1037878/COR_2019-20_outputs_COR_A1.ods'
url_20_21 = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1037853/COR_2020-21_outputs_COR_A1.ods'
url_21_22 = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1116478/COR_2021-22_outputs_COR_A1.ods'

## NB the files have different formats, so I can't just loop through them. Instead I have to repeat quite
## a lot of code for each individual file.

LA_investment_dict = {}

##### 19-20
url = url_18_19
data_name = 'LA Capital Expenditure'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')
LA_investment = xl.parse(sheet_name='Fixed assets', skiprows=3, header=[0, 1], engine='openpyxl', nrows=443,
                             na_values=[':', '[x]'])
# Add a higher-sector level to the multi-index
temp = pd.Series([x[0] for x in LA_investment.columns])
# work out the number of subsectors in each broad sector
broad_sectors_18_19 = [['']*5, ['Education']*30, ['Highways & Transport']*48, ['Social Care']*6, ['Public Health']*6,
                 ['Housing']*6, ['Culture & Related Services']*36, ['Environmental & Regulatory Services']*90,
                 ['Planning & Development Services']*6, ['Police']*6, ['Fire & Rescue']*6,
                 ['Central Services']*6, ['Industrial & Commercial Services']*48, ['Trading Services']*12, ['All Services']*6]
broad_sectors = [item for sublist in broad_sectors_18_19 for item in sublist]
LA_investment = LA_investment.transpose()
LA_investment.loc[:,'Sector'] = broad_sectors
LA_investment.set_index('Sector', append=True, inplace=True)
LA_investment = LA_investment.transpose()
# melt the non-LAD columns to long form to make more useful
df = LA_investment.iloc[:,5:].stack(level=[0,1,2]).reset_index()
# merge the LADs back in
LA_investment = df.merge(LA_investment.iloc[:,:5].droplevel(level=[0,2], axis=1).loc[:,['ONS Code', 'Name']], how='left', left_on='level_0', right_index=True)\
    .drop('level_0', axis=1)\
    .rename({'level_1':'Sub-sector', 'level_2':'Asset', 0:'value'}, axis=1)
# Re-order the columns
LA_investment = LA_investment.rename({'Name':'LA Name'}, axis=1).loc[:,[ 'ONS Code', 'LA Name', 'Sector', 'Sub-sector', 'Asset', 'value']].sort_values(['LA Name', 'Sector', 'Sub-sector'])

# add it to the overall dictionary
LA_investment_dict['18-19'] = LA_investment

##### 19-20
url = url_19_20
data_name = 'LA Capital Expenditure'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath)
LA_investment = xl.parse(sheet_name='Fixed_assets', skiprows=3, header=[0, 1], engine='odf', nrows=425,
                             na_values=[':', '[x]'])
# Add a higher-sector level to the multi-index
temp = pd.Series([x[0] for x in LA_investment.columns])
# work out the number of subsectors in each broad sector
broad_sectors_19_20 = [['']*5, ['Education']*30, ['Highways & Transport']*48, ['Social Care']*6, ['Public Health']*6,
                 ['Housing']*6, ['Culture & Related Services']*36, ['Environmental & Regulatory Services']*90,
                 ['Planning & Development Services']*6, ['Digital Infrastructure']*6, ['Police']*6, ['Fire & Rescue']*6,
                 ['Central Services']*6, ['Industrial & Commercial Services']*48, ['Trading Services']*12, ['All Services']*6]
broad_sectors = [item for sublist in broad_sectors_19_20 for item in sublist]
LA_investment = LA_investment.transpose()
LA_investment.loc[:,'Sector'] = broad_sectors
LA_investment.set_index('Sector', append=True, inplace=True)
LA_investment = LA_investment.transpose()
# melt the non-LAD columns to long form to make more useful
df = LA_investment.iloc[:,5:].stack(level=[0,1,2]).reset_index()
# merge the LADs back in
LA_investment = df.merge(LA_investment.iloc[:,:5].droplevel(level=[0,2], axis=1).loc[:,['ONS Code', 'Name']], how='left', left_on='level_0', right_index=True)\
    .drop('level_0', axis=1)\
    .rename({'level_1':'Sub-sector', 'level_2':'Asset', 0:'value'}, axis=1)
# Re-order the columns
LA_investment = LA_investment.rename({'Name':'LA Name'}, axis=1).loc[:,[ 'ONS Code', 'LA Name', 'Sector', 'Sub-sector', 'Asset', 'value']].sort_values(['LA Name', 'Sector', 'Sub-sector'])

# add it to the overall dictionary
LA_investment_dict['19-20'] = LA_investment

######### 20-21
url = url_20_21
data_name = 'LA Capital Expenditure'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath)
LA_investment = xl.parse(sheet_name='Fixed_assets', skiprows=3, header=[0, 1], engine='odf', nrows=425,
                             na_values=[':', '[x]'])
# Add a higher-sector level to the multi-index
temp = pd.Series([x[0] for x in LA_investment.columns])
# work out the number of subsectors in each broad sector
broad_sectors_19_20 = [['']*5, ['Education']*30, ['Highways & Transport']*48, ['Social Care']*6, ['Public Health']*6,
                 ['Housing']*6, ['Culture & Related Services']*36, ['Environmental & Regulatory Services']*90,
                 ['Planning & Development Services']*6, ['Digital Infrastructure']*6, ['Police']*6, ['Fire & Rescue']*6,
                 ['Central Services']*6, ['Industrial & Commercial Services']*48, ['Trading Services']*12, ['All Services']*6]
broad_sectors = [item for sublist in broad_sectors_19_20 for item in sublist]
LA_investment = LA_investment.transpose()
LA_investment.loc[:,'Sector'] = broad_sectors
LA_investment.set_index('Sector', append=True, inplace=True)
LA_investment = LA_investment.transpose()
# melt the non-LAD columns to long form to make more useful
df = LA_investment.iloc[:,5:].stack(level=[0,1,2]).reset_index()
# merge the LADs back in
LA_investment = df.merge(LA_investment.iloc[:,:5].droplevel(level=[0,2], axis=1).loc[:,['ONS Code', 'Name']], how='left', left_on='level_0', right_index=True)\
    .drop('level_0', axis=1)\
    .rename({'level_1':'Sub-sector', 'level_2':'Asset', 0:'value'}, axis=1)
# Re-order the columns
LA_investment = LA_investment.rename({'Name':'LA Name'}, axis=1).loc[:,[ 'ONS Code', 'LA Name', 'Sector', 'Sub-sector', 'Asset', 'value']].sort_values(['LA Name', 'Sector', 'Sub-sector'])

# add it to the overall dictionary
LA_investment_dict['20-21'] = LA_investment

######### 21-22
## Not done yet. It's in a different format. See if I can find in the same format before slogging
## through changing the code.
url = url_21_22
data_name = 'LA Capital Expenditure'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath)
LA_investment = xl.parse(sheet_name='Fixed_assets', skiprows=6, header=0, engine='odf', nrows=426,  usecols='A:LK',
                             na_values=[':', '[x]'])
# split the header on ':'
header_split = [re.split(':',x) for x in LA_investment.columns]
header1 = []
header2 = []
for _list in header_split:
    if len(_list) == 1:
        header1.append('')
        header2.append(_list[0])
    else:
        header1.append(_list[0])
        header2.append(_list[1])
# drop the '£ thousand from header2
header2 = [re.sub('£ thousand', '',x) for x in header2]

# reassign the columns
LA_investment.columns = header2
# Create a third level of column
broad_sectors_19_20 = [['']*5, ['Education']*30, ['Highways & Transport']*48, ['Social Care']*6, ['Public Health']*6,
                 ['Housing']*6, ['Culture & Related Services']*36, ['Environmental & Regulatory Services']*90,
                 ['Planning & Development Services']*6, ['Digital Infrastructure']*6, ['Police']*6, ['Fire & Rescue']*6,
                 ['Central Services']*6, ['Industrial & Commercial Services']*48, ['Trading Services']*12, ['All Services']*6]
broad_sectors = [item for sublist in broad_sectors_19_20 for item in sublist]

LA_investment = LA_investment.transpose()
LA_investment.loc[:,'Sector'] = broad_sectors
LA_investment.loc[:,'Sub-sector'] = header1
LA_investment.set_index('Sector', append=True, inplace=True)
LA_investment.set_index('Sub-sector', append=True, inplace=True)
LA_investment = LA_investment.transpose()

# melt the non-LAD columns to long form to make more useful
df = LA_investment.iloc[:,5:].stack(level=[0,1,2]).reset_index()
# merge the LADs back in
LA_investment = df.merge(LA_investment.iloc[:,:5].droplevel(level=[1,2], axis=1).loc[:,['ONS Code', 'Name']], how='left', left_on='level_0', right_index=True)\
    .drop('level_0', axis=1)\
    .rename({'level_1':'Asset', 'level_2':'Asset', 0:'value'}, axis=1)
# Re-order the columns
LA_investment = LA_investment.rename({'Name':'LA Name'}, axis=1).loc[:,[ 'ONS Code', 'LA Name', 'Sector', 'Sub-sector', 'Asset', 'value']].sort_values(['LA Name', 'Sector', 'Sub-sector'])

# add it to the overall dictionary
LA_investment_dict['21-22'] = LA_investment


# keep this in case I want to bring back the 22-23 data at some point
# NB we're not using it for now because it is forecasts rather than actuals
broad_sectors_22_23 = [['']*5, ['Education']*30, ['Highways & Transport']*48, ['Social Care']*6, ['Public Health']*6,
                 ['Housing']*18, ['Culture & Related Services']*36, ['Environmental & Regulatory Services']*90,
                 ['Planning & Development Services']*6, ['Digital Infrastructure']*6, ['Police']*6, ['Fire & Rescue']*6,
                 ['Central Services']*6, ['Industrial & Commercial Services']*48, ['Trading Services']*12, ['All Services']*6]

### Now get LA investment per head

# get the appropriate population figures
with psycopg2.connect(**params) as conn:
    pop_lad = pd.read_sql_query(sql='select * from population_lad', con=conn)

# create a single, long dataframe that includes the year and the right LA code
df_list = []
for _key, year, vintage in zip(LA_investment_dict.keys(), ['2018','2019', '2020', '2021'], ['lad18cd', 'lad19cd', 'lad20cd', 'lad21cd']):
    temp = LA_investment_dict[_key]
    temp['Year'] = year
    if vintage != 'lad21cd':
        temp = temp.merge(lad_mappings.loc[:,[vintage, 'lad21cd']], how='left', left_on='ONS Code', right_on=vintage)
    df_list.append(temp)
LA_investment = pd.concat(df_list, axis=0)
LA_investment['value'] = LA_investment['value'].astype('float')
LA_investment['Year'] = LA_investment['Year'].astype(int)

# now aggregate up to lad21cd regions (this adds up data for LAs that were previously separate)
# and merge back in lad21 names
LA_investment = LA_investment.groupby(['Sector', 'Sub-sector', 'Asset', 'Year',
       'lad21cd'])['value'].sum().reset_index()\
        .merge(lad21_lookup.loc[:,['lad21cd', 'lad21nm']], how='left')

# now merge in the population data
LA_investment_per_head = LA_investment.merge(pop_lad.loc[:,['lad21cd', 'year', 'population']], how='left', left_on=['lad21cd', 'Year'], right_on=['lad21cd', 'year'])
LA_investment_per_head['value_per_head'] = 1000 * LA_investment_per_head['value'].div(LA_investment_per_head['population'])

#  rearrange and rename columns
LA_investment_per_head = LA_investment_per_head.loc[:,['lad21cd', 'lad21nm', 'Sector', 'Sub-sector', 'Asset', 'Year', 'value',
       'population', 'value_per_head']].rename({'Sub-sector':'sub_sector'}, axis=1)
LA_investment_per_head.columns = [x.lower() for x in  LA_investment_per_head.columns]

# create a database table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS la_investment (
                lad21cd VARCHAR,
                lad21nm VARCHAR,
                sector VARCHAR,
                sub_sector VARCHAR,
                asset VARCHAR,
                year INT,
                value FLOAT,
                population FLOAT,
                value_per_head FLOAT,
                created timestamptz,
                parent_script VARCHAR,
                PRIMARY KEY (lad21cd, sector, sub_sector, asset, year));
                """)
    cur.close()
    con.commit()

# prepare for upload
# add timestamps and parent scripts...
for df in [LA_investment_per_head]:
    df['created'] = datetime.datetime.now()
    df['parent_script'] = parent_script
#...convert NAs for storing in postresql [should I wrap this into the insert function?]
LA_investment_per_head = LA_investment_per_head.fillna(psycopg2.extensions.AsIs('NULL'))

# load the data
with psycopg2.connect(**params) as con:
    execute_values(df=LA_investment_per_head, table='la_investment', con=con)


###################################################################################
# GVA by LAD and industry - NOT UPLOADED
###################################################################################

url_list = ['https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukcnortheast/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukcnortheast.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukdnorthwest/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukdnorthwest.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukeyorkshireandthehumber/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukeyorkshireandthehumber.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukfeastmidlands/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukfeastmidlands.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukgwestmidlands/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukgwestmidlands.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukheastofengland/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukheastofengland.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukilondon/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukilondon.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukjsoutheast/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukjsoutheast.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukksouthwest/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukksouthwest.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/uklwales/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesuklwales.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/ukmscotland/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesukmscotland.xlsx',
            'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedlocalauthoritiesbynuts1region/uknnorthernireland/regionalgrossvalueaddedbalancedbyindustrylocalauthoritiesuknnorthernireland.xlsx']
df_list = []
for url in url_list:
    req = requests.get(url)
    filename = url.split('/')[-1]
    filepath = os.path.join(data_folder, filename)
    if os.path.isfile(filepath) == False:
        print('Downloading GVA by LAD for # region data.')
        with open(filepath, 'wb') as output_file:
            output_file.write(req.content)
    else:
        print('GVA by LAD for # region already exists. Loaded it.')
    # Read the Excel file
    #xl = pd.ExcelFile(filepath)
    # parse the Life Satisfaction sheet and add it to the dictionary
    df = pd.read_excel(filepath, sheet_name='CVM index', skiprows=1, na_values=[':'], engine='openpyxl')
    # drop the empty rows from the bottom that contain footnotes in the first column
    df = df.loc[df['LAD code'].notna(),:]
    df_list.append(df)
gva_by_lad = pd.concat(df_list)



###################################################################################
# patents work - NOT UPLOADED
###################################################################################

filename = 'corecities_nuts2_updated.xlsx'
filepath = os.path.join(data_folder, filename)
patents = {}
patents['rta'] = pd.read_excel(filepath, sheet_name='corecities_nuts2', engine='openpyxl')
patents['innovation_distribution'] = pd.read_excel(filepath, sheet_name='Innovation Distribution - UK', engine='openpyxl')
patents['innovation_intensity'] = rta = pd.read_excel(filepath, sheet_name='Innovation Intensity - UK', header=[0,1], engine='openpyxl')
patents['istrax'] = pd.read_excel(filepath, sheet_name='ISTRAX', engine='openpyxl')

###################################################################################
# visitor attractions - NOT UPLOADED
###################################################################################

# First get the 2021 dataset, as it has postcodes. Then merge in the 2022 dataset which doesn't have postcodes for some reason.
url = 'https://www.visitbritain.org/sites/default/files/vb-corporate/Domestic_Research/vva_full_attractions_listings_2021_website.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading Vist Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit Britain data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# parse the Life Satisfaction sheet and add it to the dictionary
df21 = xl.parse(sheet_name='Permission to publish', na_values=['Not available'], usecols='A:U', nrows=888).drop('Unnamed: 19', axis=1)

# Now get the 2022 dataset
url = 'https://www.visitbritain.org/sites/default/files/vb-corporate/Domestic_Research/annual_attractions_full_listings_2022_v2.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join(data_folder, filename)
if os.path.isfile(filepath) == False:
    print('Downloading Vist Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit Britain data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# parse the Life Satisfaction sheet and add it to the dictionary
df22 = xl.parse(sheet_name='With permissions', na_values=['Not available'], usecols='A:S', nrows=1114)

# look at how well they match up. Not that well! Annoying.
matches_id = set(df21['Provider_Product_ID']).intersection(set(df22['providerid']))
matches_name = set(df21['Attraction']).intersection(set(df22['Attraction']))

# get postcode lookup and merge it in
with psycopg2.connect(**params) as conn:
    pcode_lookup = pd.read_sql_query(sql='select pcds, lad21cd, ladnm from pcode_lookup where pcds in {}'.format(tuple(df21['Postcode'].drop_duplicates().to_list())), con=conn)
df21 = df21.merge(pcode_lookup.loc[:,['pcds', 'lad21cd', 'ladnm']], how='left', left_on='Postcode', right_on='pcds')
del pcode_lookup

attractions = {}
attractions['attractions_21'] = df21
attractions['attractions_22'] = df22

###################################################################################
# National Heritage List
###################################################################################

listed_buildings = gpd.read_file(os.path.join('input_data', 'National_Heritage_List_for_England_(NHLE)', 'Listed_Building_polygons.shp'), crs='EPSG:27700')
lad_gpd = gpd.read_file(os.path.join('input_data', 'Local_Authority_Districts_(December_2021)_UK_BFC', 'LAD_DEC_2021_UK_BFC.shp'), crs='EPSG:27700')
listed_buildings = gpd.sjoin(listed_buildings, lad_gpd.loc[:,['OBJECTID', 'LAD21CD', 'LAD21NM', 'geometry']], how='left', op='intersects')

###################################################################################
# motor vehicle traffic by LAD
###################################################################################

vehicle_traffic = {}

# For all traffic
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1169847/tra8901.ods'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading traffic data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit traffic data already exists. Loading it.')
vehicle_traffic = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='odf')
# parse the Life Satisfaction sheet and add it to the dictionary
df1 = xl.parse(sheet_name='TRA8901', skiprows=4, na_values=['[x]'], usecols='A:AJ', nrows=234)
vehicle_traffic['all_traffic'] = df1

# by vehicle type
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1169848/tra8902.ods'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading traffic Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit traffic data already exists. Loading it.')
vehicle_traffic = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='odf')
# parse the Life Satisfaction sheet and add it to the dictionary
df2 = xl.parse(sheet_name='TRA8902', skiprows=4, na_values=['[x]'], usecols='A:Ak', nrows=800)
vehicle_traffic['by_type'] = df2

# all traffic, ex trunk roads
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1169849/tra8903.ods'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading traffic Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit traffic data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='odf')
# parse the Life Satisfaction sheet and add it to the dictionary
df3 = xl.parse(sheet_name='TRA8903', skiprows=4, na_values=['[x]'], usecols='A:AJ', nrows=234)
vehicle_traffic['all_ex_trunk'] = df3

###################################################################################
# rail station usage
###################################################################################

url = 'https://dataportal.orr.gov.uk/media/1907/table-1410-passenger-entries-and-exits-and-interchanges-by-station.ods'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading station traffic data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Station traffic data already exists. Loading it.')
vehicle_traffic = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='odf')
# parse the Life Satisfaction sheet and add it to the dictionary
station_traffic = xl.parse(sheet_name='1410_Entries_Exits_Interchanges', skiprows=3, na_values=['[z]', '[x]'], usecols='A:AD', nrows=2571)

###################################################################################
# FDI
###################################################################################

url = 'https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/foreigndirectinvestmentinvolvingukcompaniesbyukcountryandregiondirectionalinward/current/20230419subnatinwardtables.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading FDI data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('FDI data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
fdi = {}
fdi_itl1 = xl.parse(sheet_name='3.1 ITL1 IIP continent', skiprows=3, na_values=['c'], usecols='A:K', nrows=98)
fdi_city = xl.parse(sheet_name='3.8 City IIP continent', skiprows=3, na_values=['c'], usecols='A:J', nrows=105)
fdi_itl1_industry = xl.parse(sheet_name='3.3 ITL1 IIP industry', skiprows=3, na_values=['c'], usecols='A:K', nrows=266)
fdi_city_industry = xl.parse(sheet_name='3.10 City IIP industry group', skiprows=3, na_values=['c'], usecols='A:J', nrows=120)
fdi_cityregion_lookup = xl.parse(sheet_name='City Regions', skiprows=2)
fdi_cityregion_lookup = dict(zip(fdi_cityregion_lookup['City region'], fdi_cityregion_lookup['Constituent local authorities']))

fdi['itl1'] = fdi_itl1
fdi['city'] = fdi_city
fdi['itl1_industry'] = fdi_itl1_industry
fdi['city_industry'] = fdi_city_industry
fdi['lookup'] = fdi_cityregion_lookup

##############################################
# migration data
##############################################

# migration data
# NB it comes in two files that have to be concatenated
url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/migrationwithintheuk/datasets/internalmigrationbyoriginanddestinationlocalauthoritiessexandsingleyearofagedetailedestimatesdataset/yearendingjune2020part1/detailedestimates2020on2021laspt1.zip"
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading internal data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Internal migration data already exists. Loading it.')
df1 = pd.read_csv(io.BytesIO(ZipFile(filepath).read('Detailed_Estimates_2020_LA_2021_Dataset_1.csv')))
url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/migrationwithintheuk/datasets/internalmigrationbyoriginanddestinationlocalauthoritiessexandsingleyearofagedetailedestimatesdataset/yearendingjune2020part2/detailedestimates2020on2021laspt2.zip"
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading internal data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Internal migration data already exists. Loading it.')
df2 = pd.read_csv(io.BytesIO(ZipFile(filepath).read('Detailed_Estimates_2020_LA_2021_Dataset_2.csv')))
# combine the two parts of the dataset
internal_migration = pd.concat([df1, df2], axis=0)
# merge in LA names
internal_migration = internal_migration.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm']].drop_duplicates(), how='left', left_on='inla', right_on='lad21cd').rename({'lad21nm':'inla_name'}, axis=1).drop('lad21cd', axis=1)
internal_migration = internal_migration.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm']].drop_duplicates(), how='left', left_on='outla', right_on='lad21cd').rename({'lad21nm':'outla_name'}, axis=1).drop('lad21cd', axis=1)

##############################################
# company births and deaths data (annual data - issues with completeness)
##############################################

url = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/businessdemographyreferencetable/current/businessdemographyexceltables2021.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading company births data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Company births data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
company_demographics = {}
sheetnames = [ 'Table 1.1a',
 'Table 1.1b',
 'Table 1.1c',
 'Table 1.1d',
  'Table 2.1a',
 'Table 2.1b',
 'Table 2.1c',
 'Table 2.1d',
 'Table 3.1a',
 'Table 3.1b',
 'Table 3.1c',
 'Table 3.1d']

other_cols = ['Table 4.1',
 'Table 4.2',
 'Table 5.1a',
 'Table 5.1b',
 'Table 5.1c',
 'Table 5.1d',
 'Table 5.1e',
 'Table 5.2a',
 'Table 5.2b',
 'Table 5.2c',
 'Table 5.2d',
 'Table 5.2e',
 'Table 6.1',
 'Table 6.2',
 'Table 7.1a',
 'Table 7.1b',
 'Table 7.1c',
 'Table 7.1d',
 'Table 7.2',
 'Table 7.3a',
 'Table 7.3b',
 'Table 7.3c',
 'Table 7.3d',
 'Table 7.4',
 'Table 8',
 'Table 9']

for sheetname in sheetnames:
    temp = xl.parse(sheet_name=sheetname, skiprows=3, na_values=['c'], engine='openpyxl')
    new_cols = temp.columns.to_list()
    new_cols = ['Geog code', 'Geog name'] + new_cols[2:]
    temp.columns = new_cols
    temp = temp.loc[temp.isna().all(axis=1)==False, temp.isna().all(axis=0)==False]
    # now drop blank columns and rows
    company_demographics[sheetname] = temp

# now make single dataframes of births, deaths and stocks
births = company_demographics['Table 1.1a']
for sheetname in ['Table 1.1b', 'Table 1.1c', 'Table 1.1d']:
    births = births.merge(company_demographics[sheetname], how='left', left_on=['Geog code', 'Geog name'], right_on=['Geog code', 'Geog name'])
deaths = company_demographics['Table 2.1a']
for sheetname in ['Table 2.1b', 'Table 2.1c', 'Table 2.1d']:
    deaths = deaths.merge(company_demographics[sheetname], how='left', left_on=['Geog code', 'Geog name'], right_on=['Geog code', 'Geog name'])
stock = company_demographics['Table 3.1a']
for sheetname in ['Table 3.1b', 'Table 3.1c', 'Table 3.1d']:
    stock = stock.merge(company_demographics[sheetname], how='left', left_on=['Geog code', 'Geog name'], right_on=['Geog code', 'Geog name'])

# remake the dictionary with just the complete dataframes
company_demographics = {}
company_demographics['births'] = births
company_demographics['deaths'] = births
company_demographics['stock'] = stock


############################################################
# Company demographics (quarterly data)
############################################################

url = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/businessdemographyquarterlyexperimentalstatisticslowlevelgeographicbreakdownuk/quarter2apriltojune2023/finalq22023lowlevelgeobreakdown.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading company births data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Company births data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# make a dictionary to hold the individual sheets
company_births = {}
sheetnames = ['Births 2017-2019', 'Births 2020', 'Births 2021', 'Births 2022-2023', 'Deaths 2017-2019', 'Deaths 2020', 'Deaths 2021', 'Deaths 2022-2023']
for sheetname in sheetnames:
    temp = xl.parse(sheet_name=sheetname, skiprows=3, na_values=['c'], nrows=423)
    temp['geog code'] = [x.split(':')[0].strip() for x in temp['Geography']]
    temp['geog name'] = [x.split(':')[1].strip() for x in temp['Geography']]
    company_births[sheetname] = temp

# now make single dataframes of births and deaths
births = company_births['Births 2017-2019']
births = births.loc[:,['Geography', 'geog code', 'geog name', 'Q1 2017', 'Q2 2017', 'Q3 2017', 'Q4 2017', 'Q1 2018',
       'Q2 2018', 'Q3 2018', 'Q4 2018', 'Q1 2019', 'Q2 2019', 'Q3 2019',
       'Q4 2019']]
for sheetname in ['Births 2020', 'Births 2021', 'Births 2022-2023']:
    births = births.merge(company_births[sheetname], how='left', left_on=['Geography', 'geog code', 'geog name'], right_on=['Geography', 'geog code', 'geog name'])
deaths = company_births['Deaths 2017-2019']
deaths = deaths.loc[:,['Geography', 'geog code', 'geog name', 'Q1 2017', 'Q2 2017', 'Q3 2017', 'Q4 2017', 'Q1 2018',
       'Q2 2018', 'Q3 2018', 'Q4 2018', 'Q1 2019', 'Q2 2019', 'Q3 2019',
       'Q4 2019']]
for sheetname in ['Deaths 2020', 'Deaths 2021', 'Deaths 2022-2023']:
    deaths = deaths.merge(company_births[sheetname], how='left', left_on=['Geography', 'geog code', 'geog name'], right_on=['Geography', 'geog code', 'geog name'])

# add annual series to the quarterly series
for year in [2017, 2018, 2019, 2020, 2021, 2022]:
    births[year] = births.loc[:,['Q1 '+str(year), 'Q2 '+str(year), 'Q3 '+str(year), 'Q4 '+str(year)]].sum(axis=1, skipna=False)
    deaths[year] = deaths.loc[:,['Q1 '+str(year), 'Q2 '+str(year), 'Q3 '+str(year), 'Q4 '+str(year)]].sum(axis=1, skipna=False)

# overwrite the dictionary with a simpler dictionary of the combined time series
company_demographics_quarterly = {}
company_demographics_quarterly['births'] = births
company_demographics_quarterly['deaths'] = deaths

############################################################################
# company demographics - annual stocks (to use with quarterly flows
############################################################################

url_2022 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2022/ukbusinessworkbook2022.xlsx'
url_2021 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2021/ukbusinessworkbook2021.xlsx'
url_2020 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2020/ukbusinessworkbook2020.xlsx'
url_2019 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2019/ukbusinessworkbook2019.xlsx'
url_2018 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2018/ukbusinessworkbook2018.xls'
url_2017 = 'https://www.ons.gov.uk/file?uri=/businessindustryandtrade/business/activitysizeandlocation/datasets/ukbusinessactivitysizeandlocation/2017/ukbusinessworkbook2017.xls'

url_list = [url_2017, url_2018, url_2019, url_2020, url_2021, url_2022]
year_list = [2017, 2018, 2019, 2020, 2021, 2022]
stocks = {}
for url, yearname in zip(url_list, year_list):
    req = requests.get(url)
    filename = url.split('/')[-1]
    filepath = os.path.join('input_data', filename)
    if os.path.isfile(filepath) == False:
        print('Downloading company stocks {} data.'.format(str(yearname)))
        with open(filepath, 'wb') as output_file:
            output_file.write(req.content)
    else:
        print('Company stocks {} data already exists. Loading it.'.format(str(yearname)))
    # Read the Excel file (choosing the right engine, depending on whether it is an xlsx or xls file
    if filename[-1]=='x':
        xl = pd.ExcelFile(filepath, engine='openpyxl')
    else:
        xl = pd.ExcelFile(filepath)

    # get the right page and put it into the stocks dictionary
    if yearname in [2017, 2018, 2019, 2020, 2021]:
        temp = xl.parse(sheet_name='Table 1', skiprows=5, na_values=['c'])
        new_cols = temp.columns.to_list()
        new_cols = ['Geog code', 'Geog name'] + new_cols[2:]
        temp.columns = new_cols
        temp = temp.loc[temp.isna().all(axis=1)==False, temp.isna().all(axis=0)==False]
        stocks[yearname] = temp
    else:
        temp = xl.parse(sheet_name='Table 1', skiprows=3, na_values=['c'])
        new_cols = temp.columns.to_list()
        new_cols = ['Geography'] + new_cols[1:]
        temp.columns = new_cols
        temp = temp.loc[temp.isna().all(axis=1) == False, temp.isna().all(axis=0) == False]
        temp['Geog code'] = [x.split(':')[0].strip() for x in temp['Geography']]
        temp['Geog name'] = [x.split(':')[1].strip() for x in temp['Geography']]
        stocks[yearname] = temp

# now make a summary of total stocks by year and add it to the demography quarterly
stock_df = stocks[2017].loc[:,['Geog code', 'Geog name', 'Total']].rename({'Total':'2017'}, axis=1).copy()
for year in range(2018,2023,1):
    stock_df = stock_df.merge(stocks[year].loc[:,['Geog code', 'Total']].rename({'Total':str(year)}, axis=1), how='left', left_on='Geog code', right_on='Geog code')
stock_df = stock_df.iloc[:442,:]
company_demographics_quarterly['stocks'] = stock_df

##############################################
# Core Cities' city region mapping
##############################################

city_region_map = pd.read_excel(os.path.join('input_data', 'Core Cities definitions-20190606.xlsx'), engine='openpyxl', nrows=97)
city_region_map.columns = ['City Region', 'LA', 'NUTS2', 'NUTS3']
# ffill the city region column and drop the NUTS2 and NUTS3 columns as they aren't relevant and could cause confusion
city_region_map['City Region'] = city_region_map['City Region'].fillna(method='ffill')
city_region_map = city_region_map.loc[:,['City Region', 'LA']]

##############################################
# Labour market participation by LA, 2021
##############################################

url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/datasets/locallabourmarketindicatorsforcountieslocalandunitaryauthoritiesli01/current/previous/v35/lmregtabli01april2022.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading participation data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Participation data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
participation_by_lad = xl.parse(sheet_name='LI01', skiprows=4, na_values=['[c]', '[x]', '#N/A'], usecols='A:O', nrows=410)
participation_by_lad.columns = ['Geography', 'Geography code',
       'Population aged 16 to 64, 2020 (thousands)',
       'Employment age 16 and older(thousands)',
       'Employment rate age 16 to 64',
       'Unemployment age 16 to 64 (thousands)',
       'Unemployment rate age 16 to 64',
       'Economic inactivity',
       'Economic inactivity rate',
       'Claimant Count',
       'Claimant Count proportion',
       'Jobs 2020 (thousands)',
       'Jobs Density 2020 %',
       'Earnings by resident 2021 (£)',
       'Earnings by workplace 2021 (£)']
# merge in the lad lookup data to check which units are LADs
participation_by_lad = participation_by_lad.merge(lad_lookup.loc[:,['lad21nm', 'lad21cd']].drop_duplicates(),
                                                    how='left', left_on='Geography code', right_on='lad21cd')
# now load inactivity by reason by LAD
inac_reasons = pd.read_csv(os.path.join('input_data', 'inactivity by reason by lad.csv'), skiprows=7, na_values=['*', '#', '!', ':', '-'])\
                   .loc[:,['local authority: district / unitary (as of April 2021)', '% of economically inactive long-term sick']]
inac_reasons.columns = ['ladnm', '% of economically inactive long-term sick']
participation_by_lad = participation_by_lad.merge(inac_reasons, how='left', left_on='Geography', right_on='ladnm')

##############################################
# Healthy life expectancy
##############################################

url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/healthandsocialcare/healthandlifeexpectancies/datasets/healthstatelifeexpectancyatbirthandatage65bylocalareasuk/current/hsleatbirthandatage65byukla201618.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading HLE data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('HLE data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
hle = {}
for sheetname in ['HE - Male at birth', 'HE - Female at birth', 'HE - Male at 65', 'HE - Female at 65']:
    temp = xl.parse(sheet_name=sheetname, skiprows=3, na_values=['[c]', '[x]', '#N/A'], nrows=486)
    temp = temp.loc[:,['Area Codes', 'LE', 'HLE', 'DfLE']]
    temp = temp.loc[temp.isna().all(axis=1) == False, temp.isna().all(axis=0) == False]
    hle[sheetname] = temp

##############################################
# Natural capital condition indicators
##############################################

url = 'https://www.ons.gov.uk/file?uri=/economy/environmentalaccounts/datasets/habitatconditionnaturalcapitaluksupplementaryinformation/current/habitatconditionnaturalcapitaluksupplementaryinformation1.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Natural Capital data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Natural Capital data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
condition = {}
for sheetname in ['Bees', 'Bats', 'Butterflies', 'Birds', 'Moths']:
    temp = xl.parse(sheet_name=sheetname, skiprows=3, na_values=['[c]', '[x]', '#N/A'])
    #temp = temp.loc[:,['Area Codes', 'LE', 'HLE', 'DfLE']]
    temp = temp.loc[temp.isna().all(axis=1) == False, temp.isna().all(axis=0) == False]
    condition[sheetname] = temp

##############################################
# Libraries dataset from Arts Council England
##############################################

url = 'https://www.artscouncil.org.uk/media/21603/download?attachment'
# had to download manually
libraries = pd.read_excel(os.path.join('input_data', '2022 Dataset Publication as Uploaded (no contact details).xlsx'), engine='openpyxl',
                          sheet_name='Data', na_values=['', ' '])
libraries['Year closed']
closures = libraries.groupby('Year closed')['Year closed'].count()
openings = libraries.groupby('Year opened')['Year opened'].count()

stocks = pd.concat([closures, openings[openings.index>2009]], axis=1)
stocks['current'] = libraries['Year closed'].isna().sum()
stocks['stock'] = stocks['current']
# calculate historic stocks
for year in range(2021,2009,-1):
    stocks.loc[year, 'stock'] = stocks.loc[year+1,'stock'] - stocks.loc[year,'Year opened'] + stocks.loc[year,'Year closed']

'https://www.artscouncil.org.uk/media/21603/download?attachment'

###################################################
# Wikipedia lists of best-selling music and books
###################################################

books_list = ['List_of_best-selling_books_'+str(x)+'.csv' for x in range(1,5,1)]
artists_list = ['List_of_best-selling_music_artists_'+str(x)+'.csv' for x in range(1,7,1)]

book_tables = []
for t in books_list:
    out = pd.read_csv(os.path.join('input_data', 'from Wikipedia', t))
    book_tables.append(out)
books = pd.concat(book_tables, axis=0)
# continued in a separate script because it requires web scraping to get places of birth for authors

artist_tables = []
for a in artists_list:
    out = pd.read_csv(os.path.join('input_data', 'from Wikipedia', a))
    artist_tables.append(out)
artists = pd.concat(artist_tables, axis=0)
artists['Claimed sales cleaned'] = artists['Claimed sales'].apply(lambda x: re.findall('^[0-9]+',x)[0]).astype(int)

############################################################
# carbon emissions data from Global Carbon Budget Project
############################################################

url = 'https://globalcarbonbudget.org/wp-content/uploads/National_Fossil_Carbon_Emissions_2022v1.0.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Carbon data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Carbon Emissions data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')

cons_emiss = xl.parse(sheet_name='Consumption Emissions', skiprows=8, usecols='A:IB', nrows=33, index_col=0)

############################################################
# Social Capital indicators from ONS
############################################################

url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/wellbeing/datasets/socialcapitalheadlineindicators/april2020tomarch2021/referencetablessocialcapital2020.2021corrected2.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading social capital data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Social capital data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')

sheets = ['1.1a Meeting Up',
 '1.1b Calling',
 '1.1c Writing',
 '1.1d Messaging',
 '1.2 Loneliness',
 '1.3 Chatting with neighbours',
 '2.1 Rely on',
 '2.2 Community support',
 '2.3 Special help',
 '2.4a Providing practical help',
 '2.4b Receiving practical help',
 '2.5a Providing financial help',
 '2.5b Receiving financial help',
 '2.6 Borrowing',
 '2.7 Checking on neighbours',
 '3.1a Formal volunteering',
 '3.1b Informal volunteering',
 '3.2 Charity donations',
 '3.3 Social Action',
 '3.4 Influence decisions',
 '3.5 Civic participation',
 '4.1 Generalised trust',
 '4.2 Neighbourhood trust',
 '4.3 Different backgrounds',
 '4.4a Feeling safe - Females',
 '4.4b Feeling safe - Males',
 '4.5 Willing to help neighbours',
 '4.6 Belonging to Neighbourhood']
social_capital = {}
for sheet in sheets:
    try:
        temp_dict = {}
        first_df = xl.parse(sheet_name=sheet, usecols='A:C', nrows=15, header=None)
        question = first_df.iloc[0,0]
        national_level = first_df.iloc[10,0]
        second_df = xl.parse(sheet_name=sheet, usecols='A:C', skiprows=11, nrows=1, index_col=0)
        second_df.index = [national_level]
        third_df = xl.parse(sheet_name=sheet, usecols='E:G', skiprows=11, nrows=60, index_col=0)
        third_df = third_df.loc[['Rural', 'Urban', 'London'], :]
        third_df.columns = second_df.columns
        out_df = pd.concat([second_df, third_df], axis=0)
        temp_dict['data'] = out_df
        temp_dict['question'] = question
        social_capital[sheet] = temp_dict
    except:
        print('{} didn\'t work'.format(sheet))


############################################################
# Energy ratings from ONS
############################################################

url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/housing/datasets/energyefficiencyofhousingenglandandwaleslocalauthoritydistricts/march2022/energyefficiencyofhousingenglandandwaleslocalauthoritydistrictsuptomarch2022.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading energy rating data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Energy rating data already exists. Loading it.')
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
by_lad = xl.parse(sheet_name='1e', usecols='A:E', skiprows=3)


###################################################################################
# combine the dictionaries into a master dictionary and save it as a pickle
###################################################################################
rsa_data_dict = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))
#rsa_data_dict = {'population':uk_population_estimates,
#                 'gva':ladproductivity,
#                 'gva_itl3':itl3productivity,
#                 'wages':wages,
#                 'life_satisfaction':life_satisfaction,
#                 'skills':skills,
#                 'lad_lookup':lad_lookup,
#                 'voa':voa,
#                 'hpi':hpi,
#                 'indices_of_deprivation':indices_of_deprivation,
#                 'lsoagva':lsoagva}
#rsa_data_dict['employment_by_lad'] = employment_lad_long
#rsa_data_dict['employment_by_itl3'] = employment_itl3_long
#rsa_data_dict['employment_by_itl2'] = employment_itl2_long
#rsa_data_dict['population'] = uk_population_estimates
#rsa_data_dict['GVA by LAD'] = gva_by_lad
#rsa_data_dict['lad_lookup'] = lad_lookup
rsa_data_dict['population'] = uk_population_estimates
rsa_data_dict['employment_nut213'] = employment_nuts213
rsa_data_dict['employment_nut216'] = employment_nuts216
rsa_data_dict['lad_itl3_ttwa_lookup'] = lad_itl3_ttwa_lookup
rsa_data_dict['voa'] = voa
rsa_data_dict['pua_dict'] = pua_dict
rsa_data_dict['attractions'] = attractions
rsa_data_dict['listed_buildings'] = listed_buildings
rsa_data_dict['vehicle_traffic'] = vehicle_traffic
rsa_data_dict['station_traffic'] = station_traffic
rsa_data_dict['FDI'] = fdi
rsa_data_dict['internal_migration'] = internal_migration
rsa_data_dict['company births'] = company_births
rsa_data_dict['city_region_mapper'] = city_region_map
rsa_data_dict['participation'] = participation_by_lad
rsa_data_dict['life_satisfaction'] = life_satisfaction
rsa_data_dict['company_demographics'] = company_demographics_quarterly
rsa_data_dict['healthy life expectancy'] = hle
rsa_data_dict['natural capital condition'] = condition
rsa_data_dict['indices_of_deprivation'] = indices_of_deprivation
rsa_data_dict['artists'] = artists
rsa_data_dict['consumption_emissions'] = cons_emiss
rsa_data_dict['social capital'] = social_capital
rsa_data_dict['epc_by_lad'] = by_lad

pickle.dump(rsa_data_dict,open(os.path.join('outputs','rsa_data_dict.p'),'wb'))

#rsa_data_dict = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))
#rsa_data_dict['lad_lookup'] = lad_lookup










































###################################################################################
# plotting
###################################################################################

def dot_plotter(df, lad_col, data_col, title, ylabel, ax):
    '''A plotting function to plot data at a LAD level, by region, highlighting Core Cities'''
    # define a region list, using the same ordering as in the LUWP
    rgn_list = ['East of England', 'East Midlands', 'London', 'North East', 'North West', 'South East', 'South West',
                'West Midlands', 'Yorkshire and The Humber', 'Scotland', 'Wales']
    # loop through the regions and plot dots for non_cc and then cc LADs. Annotate the cc LADs.
    for idx, rgn in enumerate(rgn_list):
        non_cc = df[(df.rgn21nm_filled == rgn) & (~df[lad_col].isin(cc_list))].loc[:, data_col]
        if idx / 2 == round(idx / 2, 0):
            ax.scatter([idx] * len(non_cc), non_cc, edgecolors='red', facecolors='none', alpha=0.3)
        else:
            ax.scatter([idx] * len(non_cc), non_cc, edgecolors='blue', facecolors='none', alpha=0.3)
        # cc = df[(df.rgn21nm_filled == rgn) & (df['LAD Name'].isin(cc_list))].loc[:,2020]
        # ax.scatter([idx]*len(cc), cc, color='green')
        cc = df[(df.rgn21nm_filled == rgn) & (df[lad_col].isin(cc_list))]
        ax.scatter([idx] * cc.shape[0], cc.loc[:, data_col], color='green')
        if cc.shape[0] > 0:
            for row in cc.index:
                ax.text(idx, cc.loc[row, data_col], cc.loc[row, lad_col][:3])
    # tidy up the ax
    ax.set_xticks(range(0, len(rgn_list)))
    labels_display = ['\n'.join(wrap(l, 20)) for l in rgn_list]
    ax.set_xticklabels(labels_display, rotation='45')
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    return ax

# Main economics variables
fig, [[ax1, ax2], [ax3, ax4]] = plt.subplots(2,2,figsize=[12,12])
ax1 = dot_plotter(df=ladproductivity['GVA per hour'].drop(['LAD Code', 'lad21cd'], axis=1).loc[1:,:], lad_col='LAD Name',
                 data_col=2020, title='GVA per hour worked, 2020', ylabel='£', ax=ax1)
ax2 = dot_plotter(df=ladproductivity['GVA per filled job'].drop(['LAD Code', 'lad21cd'], axis=1).loc[1:,:], lad_col='LAD Name',
                 data_col=2020, title='GVA per filled job, 2020', ylabel='£', ax=ax2)
ax3 = dot_plotter(df=wages['median_weekly'], lad_col='lad21nm',
                 data_col='Median pay', title='Median weekly gross pay, 2021', ylabel='£', ax=ax3)
ax4 = dot_plotter(df=skills_df, lad_col='lad21nm',
                 data_col='OBS_VALUE', title='Proportion of workforce with NVQ3+, 2021', ylabel='', ax=ax4)
fig.suptitle('Productivity, pay and skills; local authorities by countries and regions', fontsize=14)
fig.tight_layout()
fig.savefig(os.path.join('outputs','fig1.png'))

# Life satisfaction variables
fig, [[ax1, ax2], [ax3, ax4]] = plt.subplots(2,2,figsize=[12,12])
ax1 = dot_plotter(df=life_satisfaction['Life Satisfaction'], lad_col='lad21nm',
                 data_col='2020-21', title='Life Satisfaction', ylabel='£', ax=ax1)
ax2 = dot_plotter(df=life_satisfaction['Happy'], lad_col='lad21nm',
                 data_col='2020-21', title='Happy', ylabel='£', ax=ax2)
ax3 = dot_plotter(df=life_satisfaction['Worthwhile'], lad_col='lad21nm',
                 data_col='2020-21', title='Worthwhile', ylabel='£', ax=ax3)
ax4 = dot_plotter(df=life_satisfaction['Anxiety'], lad_col='lad21nm',
                 data_col='2020-21', title='Anxiety', ylabel='£', ax=ax4)
fig.suptitle('Life Satisfaction survey results, local authorities by countries and regions, 2020-1', fontsize=14)
fig.tight_layout()
fig.savefig(os.path.join('outputs','fig2.png'))


###################################################################################
# trying time series plots
###################################################################################

rgn_list = ['East of England', 'East Midlands', 'London', 'North East', 'North West', 'South East', 'South West',
            'West Midlands', 'Yorkshire and The Humber', 'Scotland', 'Wales']





###################################################################################
# experiments and debugging
###################################################################################

fig, ax = plt.subplots(1,1,figsize=[6,6])
df = uk_population_estimates['totals_01-20']
df = df[df.Name.isin(cc_list)].drop(['Code', 'Geography'], axis=1).set_index('Name').transpose()
df.plot(ax=ax)

# make a sample dot distribution plot that highlights the core cities
df = ladproductivity['GVA per hour'].drop(['LAD Code', 'lad21cd'], axis=1).loc[1:,:]
fig, ax = plt.subplots()
#rgn_list = df.rgn21nm_filled.drop_duplicates().sort_values().to_list()
rgn_list = ['East of England', 'East Midlands', 'London', 'North East', 'North West', 'South East', 'South West',
 'West Midlands', 'Yorkshire and The Humber', 'Scotland', 'Wales'] # re-ordered to match LUWP

for idx, rgn in enumerate(rgn_list):
    non_cc = df[(df.rgn21nm_filled == rgn) & (~df['LAD Name'].isin(cc_list))].loc[:,2020]
    if idx/2 == round(idx/2,0):
        ax.scatter([idx]*len(non_cc), non_cc, edgecolors='red', facecolors='none', alpha=0.3)
    else:
        ax.scatter([idx] * len(non_cc), non_cc, edgecolors='blue', facecolors='none', alpha=0.3)
    #cc = df[(df.rgn21nm_filled == rgn) & (df['LAD Name'].isin(cc_list))].loc[:,2020]
    #ax.scatter([idx]*len(cc), cc, color='green')
    cc = df[(df.rgn21nm_filled == rgn) & (df['LAD Name'].isin(cc_list))]
    ax.scatter([idx] * cc.shape[0], cc.loc[:, 2020], color='green')
    if cc.shape[0]>0:
        for row in cc.index:
            ax.text(idx, cc.loc[row,2020], cc.loc[row,'LAD Name'][:3])

ax.set_xticks(range(0,len(rgn_list)))
labels_display = [ '\n'.join(wrap(l, 20)) for l in rgn_list]
ax.set_xticklabels(labels_display, rotation='45')
ax.set_title('GVA per hour worked, local authorities\n by countries and regions, 2020')
ax.set_ylabel('£')
fig.tight_layout()
fig.savefig('outputs\\gva_per_hour.png')





