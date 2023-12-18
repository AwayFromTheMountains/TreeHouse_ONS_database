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

# define the list of Core Cities
cc_list = ['Belfast', 'Birmingham', 'Bristol, City of', 'Cardiff', 'Glasgow City', 'Leeds', 'Liverpool', 'Manchester',
           'Newcastle upon Tyne', 'Nottingham', 'Sheffield']

###################################################################################
# geographic lookups
###################################################################################

# NB can't download this automatically
lad_to_rgn_england_21 = pd.read_csv('input_data\\Local_Authority_District_to_Region_(April_2021)_Lookup_in_England.csv').drop('FID', axis=1)
lad_to_ctry_21 = pd.read_csv('input_data\\Local_Authority_District_to_Country_(April_2021)_Lookup_in_the_United_Kingdom.csv').drop('FID', axis=1)
lad_to_cty_21 = pd.read_csv('input_data\\Local_Authority_District_to_County_(April_2021)_Lookup_in_England.csv').drop('FID', axis=1)
lad_to_itl3 = pd.read_excel('input_Data\\LAD21_LAU121_ITL321_ITL221_ITL121_UK_LU.xlsx', sheet_name='LAD21_LAU121_ITL21_UK_LU', engine='openpyxl')
lad_to_nuts = pd.read_csv(os.path.join('input_data', 'Local_Authority_District_(December_2018)_to_NUTS3_to_NUTS2_to_NUTS1_(January_2018)_Lookup_in_United_Kingdom.csv'))

# load the LAD to TTWA lookup and manipulate it into a useful shape
ttwa = pd.read_excel(os.path.join('input_data', '2021la2011ttwalookupv2.xlsx'), engine='openpyxl', sheet_name='2021 LAs by 2011 TTWAs', skiprows=2, usecols='A:G', nrows=1108)
ttwa = ttwa.loc[~ttwa.isna().all(axis=1),:]
ttwa_lookup = ttwa.fillna(method='ffill', axis=0)

# merge it all into a mega lad lookup
lad_lookup = lad_to_ctry_21.merge(lad_to_rgn_england_21.loc[:,['LAD21CD', 'RGN21CD', 'RGN21NM']], how='left', left_on='LAD21CD', right_on='LAD21CD')\
    .merge(lad_to_cty_21.loc[:,['LAD21CD', 'CTY21NM']], how='left', left_on='LAD21CD', right_on='LAD21CD')\
    .merge(lad_to_itl3.drop(['LAD21NM', 'LAU121CD', 'LAU121NM'], axis=1), how='left', left_on='LAD21CD', right_on='LAD21CD')\
    .merge(lad_to_nuts.drop('FID', axis=1), how='left', left_on='LAD21CD', right_on='LAD18CD')
lad_lookup.columns = [x.lower() for x in lad_lookup.columns]
lad_lookup['rgn21nm_filled'] = lad_lookup.apply(lambda x: x.ctry21nm if pd.isnull(x.rgn21nm) else x.rgn21nm, axis=1)

# and make a separate lookup for lad to itl3 to ttwa, as this maps the same lad to multiple TTWAs, with weights
lad_itl3_ttwa_lookup = lad_to_itl3.merge(ttwa_lookup.loc[:,['Local Authority Code', 'TTWA Code', 'TTWA Name', 'Local Authority Population by TTWA', 'Local Authority Population by TTWA (%)']].rename({'Local Authority Code':'LAD21CD'}, axis=1), how='left', left_on='LAD21CD', right_on='LAD21CD')
lad_itl3_ttwa_lookup.columns = [x.lower() for x in lad_itl3_ttwa_lookup.columns]

###################################################################################
# population dataset
###################################################################################

# Get the latest year's data. NB this looks like a static URL and will need to be updated in the future.
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/mid2020/ukpopestimatesmid2020on2021geography.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2020 Population data already downloaded. Loading it.')
uk_population_estimates = {}
uk_population_estimates['decomp_19-20'] = pd.read_excel(filepath, sheet_name='MYE3', skiprows=7)
# tidy up the column names in the annual time series
df = pd.read_excel(filepath, sheet_name='MYE4', skiprows=7)
df.columns = [datetime.date(year=int(re.sub('Mid-','',x)),month=1,day=1) if len(re.findall('Mid-',x))>0 else x for x in df.columns]
uk_population_estimates['totals_01-20'] = df
uk_population_estimates['age_distr_20'] = pd.read_excel(filepath, sheet_name='MYE2 - Persons', skiprows=7)

# Get a zip of time series
url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/mid2001tomid2020detailedtimeseries/ukdetailedtimeseries2001to2020.zip"
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading time series population data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Time series population data already exists. Loading it.')
uk_population_estimates['age_distr_01-20'] = pd.read_csv(io.BytesIO(ZipFile(filepath).read('MYEB1_detailed_population_estimates_series_UK_(2020_geog21).csv')))
uk_population_estimates['summary_decomp_01-20'] = pd.read_csv(io.BytesIO(ZipFile(filepath).read('MYEB3_summary_components_of_change_series_UK_(2020_geog20).csv')))
uk_population_estimates['detailed_decomp_01-20'] = pd.read_csv(io.BytesIO(ZipFile(filepath).read('MYEB2_detailed_components_of_change_series_EW_(2020_geog21).csv')))

# calculate UK population estimates at LAD, ITL3 and ITL2 level for use elsewhere
pop_base = uk_population_estimates['totals_01-20'].merge(lad_lookup.loc[:,['lad21cd', 'itl321cd', 'itl221cd']], how='left', left_on='Code', right_on='lad21cd')
# rename the columns to strings, rather than a mix of strings and datetimes (not sure what I ever wanted that format for!)
pop_base.columns = [     'Code',      'Name', 'Geography',  '2020',  '2019',
        '2018',  '2017',  '2016',  '2015',  '2014',
        '2013',  '2012',  '2011',  '2010',  '2009',
        '2008',  '2007',  '2006',  '2005',  '2004',
        '2003',  '2002',  '2001',   'lad21cd',  'itl321cd', 'itl221cd']
cols_to_sum = ['2020',  '2019',
        '2018',  '2017',  '2016',  '2015',  '2014',
        '2013',  '2012',  '2011',  '2010',  '2009',
        '2008',  '2007',  '2006',  '2005',  '2004',
        '2003',  '2002',  '2001']
pop_itl3 = pop_base.groupby('itl321cd')[cols_to_sum].sum()
pop_itl2 = pop_base.groupby('itl221cd')[cols_to_sum].sum()
pop_lad = pop_base[pop_base['lad21cd'].notna()][cols_to_sum + ['lad21cd']].set_index('lad21cd')
uk_population_estimates['totals_by_itl3'] = pop_itl3
uk_population_estimates['totals_by_itl2'] = pop_itl2
uk_population_estimates['totals_by_lad'] = pop_lad

# get mid-year population estimates at OA level
url_list = ['https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesintheeastmidlandsregionofengland/mid2020sape23dt10f/sape23dt10fmid2020coaunformattedsyoaestimateseastmidlands.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesintheeastregionofengland/mid2020sape23dt10h/sape23dt10hmid2020coaunformattedsyoaestimateseast.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthelondonregionofengland/mid2020sape23dt10a/sape23dt10amid2020coaunformattedsyoaestimateslondon.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthenortheastregionofengland/mid2020sape23dt10d/sape23dt10dmid2020coaunformattedsyoaestimatesnortheast.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthenorthwestregionofengland/mid2020sape23dt10b/sape23dt10bmid2020coaunformattedsyoaestimatesnorthwest.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthesoutheastregionofengland/mid2020sape23dt10i/sape23dt10imid2020coaunformattedsyoaestimatessoutheast.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthesouthwestregionofengland/mid2020sape23dt10g/sape23dt10gmid2020coaunformattedsyoaestimatessouthwest.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinwales/mid2020sape23dt10j/sape23dt10jmid2020coaunformattedsyoaestimateswales.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesinthewestmidlandsregionofengland/mid2020sape23dt10e/sape23dt10emid2020coaunformattedsyoaestimateswestmidlands.xlsx',
            'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/censusoutputareaestimatesintheyorkshireandthehumberregionofengland/mid2020sape23dt10c/sape23dt10cmid2020coaunformattedsyoaestimatesyorkshireandthehumber.xlsx']

# get midyear estimates at LSOA level
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates/mid2020sape23dt2/sape23dt2mid2020lsoasyoaestimatesunformatted.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2020 Population data already downloaded. Loading it.')
xl = pd.ExcelFile(filepath, engine='openpyxl')
df = xl.parse(sheet_name='Mid-2020 Persons', engine='openpyxl', skiprows=4, usecols='A:G')
uk_population_estimates['lsoa'] = df

# by country of birth
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/migrationwithintheuk/datasets/localareamigrationindicatorsunitedkingdom/current/2021lamistables.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading population data')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2020 Population data already downloaded. Loading it.')
xl = pd.ExcelFile(filepath, engine='openpyxl')
df = xl.parse(sheet_name='Non-UK Born Population', engine='openpyxl', skiprows=2, nrows=420, usecols=[0,1,29,30,31], na_values=[':', 's', 'c']).dropna(axis=0)
uk_population_estimates['non-UK'] = df

# by ethnicity (Census 2011)
df = pd.read_csv(os.path.join('input_data', '4956711273781975.csv'), skiprows=8, nrows=348)
uk_population_estimates['ethnicity'] = df

###################################################################################
# Employment - LADs and ITL3
###################################################################################

# get the latest Employment data from the NOMIS API
nomis_all_lad_geog = ','.join(lad_lookup.lad21cd.to_list())
nomis_base = 'https://www.nomisweb.co.uk/api/v01/dataset/'
nomis_dataset = 'NM_189_1'
nomis_time = '&date=latest'
nomis_selection = '&industry=37748736&employment_status=1&measure=1&measures=20100'
filename = 'nomis_employment_lads.csv'
filepath = os.path.join('input_data', filename)
api = nomis_base+nomis_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_selection
req = requests.get(api)
with open(filepath, 'wb') as output_file:
    output_file.write(req.content)
employment = pd.read_csv(filepath)
# rename some columns, set other to lower, and keep only a subset of the metadata columns
employment.columns = [x.lower() for x in employment.columns]
employment = employment.rename({'geography_code':'lad21cd', 'geography_name':'lad21nm', 'date':'year', 'obs_value':'employment'}, axis=1)
employment_lad_long = employment.loc[:,['lad21cd', 'lad21nm', 'employment', 'year']]
employment_lad_long = employment_lad_long1[employment_lad_long1['year']<2022]

# Annual Population Surey / LFS
nomis_url = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_17_5.data.csv?geography=1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730&date=latest&variable=18&measures=20599,21001,21002,21003'

# get NOMIS data on employment by LAD
filepath = os.path.join('input_data', '25422301004241735.csv')
employment = pd.read_csv(filepath, skiprows=8, nrows=363)\
                 .rename({'local authority: district / unitary (as of April 2021)':'lad21nm'}, axis=1)\
                 .set_index('lad21nm')\
                 .iloc[:,[0,2,4,6,8,10,12]]
employment_lad_long = pd.melt(employment.reset_index(), id_vars=['lad21nm'], var_name='year', value_name='employment')
# merge in itl321cd, groupby it and sum to get ITL3 totals, then merge back in the ITL3 names
employment_itl3_long = employment_lad_long.merge(lad_to_itl3.loc[:,['LAD21NM', 'ITL321CD']], how='left', left_on='lad21nm', right_on='LAD21NM')\
    .groupby(['ITL321CD', 'year'])['employment'].sum().reset_index()
employment_itl3_long = employment_itl3_long.merge(lad_to_itl3.loc[:,['ITL321NM', 'ITL321CD']].drop_duplicates(), how='left', left_on='ITL321CD', right_on='ITL321CD')
# repeat for ITL2
employment_itl2_long = employment_lad_long.merge(lad_to_itl3.loc[:,['LAD21NM', 'ITL221CD']], how='left', left_on='lad21nm', right_on='LAD21NM')\
    .groupby(['ITL221CD', 'year'])['employment'].sum().reset_index()
employment_itl2_long = employment_itl2_long.merge(lad_to_itl3.loc[:,['ITL221NM', 'ITL221CD']].drop_duplicates(), how='left', left_on='ITL221CD', right_on='ITL221CD')

# get the NOMIS data on employment by NUTS2 2013 level
filepath = os.path.join('input_data', 'nomis_nuts2.csv')
employment_nuts2 = pd.read_csv(filepath, skiprows=8, nrows=79)\
                    .set_index('Area')\
                 .iloc[:,[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34]]\
                .reset_index()
employment_nuts2['nuts_year'] = [x[:7] for x in employment_nuts2['Area']]
employment_nuts2['nuts_code'] = [x[8:12] for x in employment_nuts2['Area']]
employment_nuts213 = employment_nuts2[employment_nuts2['nuts_year'] == 'nuts213'].drop('nuts_year', axis=1).set_index('nuts_code')
employment_nuts216 = employment_nuts2[employment_nuts2['nuts_year'] == 'nuts216'].drop('nuts_year', axis=1).set_index('nuts_code')

###################################################################################
# Subregional productivity - LADs
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/labourproductivity/datasets/subregionalproductivitylabourproductivityindicesbylocalauthoritydistrict/current/ladproductivity.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading subregional productivity data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Subregional productivity data already exists. Loading it.')
ladproductivity = {}
# get the GVA per hour sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='A3', skiprows=4, nrows=364)
df.columns = ['LAD Code', 'LAD Name'] + [x for x in range(2004,2021,1)]
df = df.merge(lad_lookup.loc[:,['lad21cd', 'cty21nm', 'rgn21nm_filled']], how='left', left_on='LAD Code', right_on='lad21cd')
ladproductivity['GVA per hour'] = df
# get the GVA per job sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='B3', skiprows=4, nrows=375)
df.columns = ['LAD Code', 'LAD Name'] + [x for x in range(2002,2021,1)]
df = df.merge(lad_lookup.loc[:,['lad21cd', 'cty21nm', 'rgn21nm_filled']], how='left', left_on='LAD Code', right_on='lad21cd')
ladproductivity['GVA per filled job'] = df

###################################################################################
# Subregional productivity - ITL3s
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/labourproductivity/datasets/subregionalproductivitylabourproductivitygvaperhourworkedandgvaperfilledjobindicesbyuknuts2andnuts3subregions/current/itlproductivity.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading subregional productivity data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Subregional productivity data already exists. Loading it.')
itl3productivity = {}
# get the GVA per hour sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='A1', header=[0,1], skiprows=3, nrows=222)
df.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2004,2021,1)]
itl3productivity['GVA per hour index'] = df
# get the GVA per job sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='B3', header=[0,1], skiprows=3, nrows=222)
df.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2002,2021,1)]
itl3productivity['GVA per job £'] = df
# get the GVA per job in chained volumes, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='B5', header=[0,1], skiprows=3, nrows=234)
df.columns = ['ITL level', 'ITL code', 'Region Name'] + [x for x in range(2002,2021,1)]
itl3productivity['GVA per job chained'] = df

###################################################################################
# Subregional productivity - LSOAs
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/uksmallareagvaestimates/1998to2020/uksmallareagvaestimates1998to202023012023150255.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
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
lsoagva['Total GVA'] = df
# get the GVA per job?

###################################################################################
# GVA by local authority
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = 'https://www.ons.gov.uk/file?uri=/economy/grossvalueaddedgva/datasets/regionalgrossvalueaddedbalancedbylocalauthorityintheuk/current/regionalgvabbylainuk.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading LA GVA.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('LA GVA data already exists. Loading it.')
lsoagva = {}
# get the GVA per hour sheet, clean up the column names and merge in regions
df = pd.read_excel(filepath, sheet_name='Total GVA', header=[0], skiprows=2, nrows=391, usecols='A:X', engine='openpyxl')

# This only goes to 2015! It seems like they produce really weird cuts of GVA at Local Authority level now, with pretty full accounts at
# NUTS3 level. Maddening.

###################################################################################
# Median wages
###################################################################################

# Get the latest year's data. NB this might be a dynamic URL.
url = "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/placeofresidencebylocalauthorityashetable8/2021provisional/table82021provisional.zip"
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading median wages data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Median wages data already exists. Loading it.')
wages = {}
# get the gross weekly pay spreadsheet and select the 'All' tab, then tidy up column names manually
df = pd.read_excel(io.BytesIO(ZipFile(filepath).read('PROV - Home Geography Table 8.1a   Weekly pay - Gross 2021.xls')), sheet_name='All', skiprows=4, nrows=412, usecols='A:Q', na_values=['x'])
xl = pd.ExcelFile(io.BytesIO(ZipFile(filepath).read('PROV - Home Geography Table 8.1a   Weekly pay - Gross 2021.xls')))
df = xl.parse(sheet_name='All', skiprows=4, nrows=412, usecols='A:Q', na_values=['x'])
df.columns = ['Geography', 'Geography Code', 'Number of jobs (thousand)', 'Median pay',
            'Median (annual percentage change)', 'Mean', 'Median (annual percentage change)', 'perc_10',
                  'perc_20', 'perc_25', 'perc_30', 'perc_40', 'perc_60', 'perc_70', 'perc_75', 'perc_80',
                  'perc_90']
df = df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='inner', left_on='Geography Code', right_on='lad21cd')
df['Median pay'] = df['Median pay'].astype('float')
wages['median_weekly'] = df

###################################################################################
# life satisfaction
###################################################################################

# Get the latest year's data. NB looks like a static URL.
url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/wellbeing/datasets/headlineestimatesofpersonalwellbeing/april2020tomarch2021localauthorityupdate/headlineestimatespersonalwellbeing2020to2021.xlsx"
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Life Satisfaction data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('2021 Life Satisfaction data already exists. Loading it.')
life_satisfaction = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# parse the Life Satisfaction sheet and add it to the dictionary
df = xl.parse(sheet_name='Life Satisfaction - Means', skiprows=6, na_values=['x'], engine='openpyxl', usecols='A:M', nrows=434)
df.columns = ['Area Codes', 'Area Names', 'Geographical Designation', '2011-12', '2012-13', '2013-14', '2014-15',
              '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
df = df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='inner', left_on='Area Codes', right_on='lad21cd')
life_satisfaction['Life Satisfaction'] = df
# Iterate through the other sheets we want
df = xl.parse(sheet_name='Worthwhile - Means', skiprows=6, na_values=['x'], engine='openpyxl', usecols='A:M', nrows=434)
df.columns = ['Area Codes', 'Area Names', 'Geographical Designation', '2011-12', '2012-13', '2013-14', '2014-15',
              '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
df = df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='inner', left_on='Area Codes', right_on='lad21cd')
life_satisfaction['Worthwhile'] = df
df = xl.parse(sheet_name='Happy - Means', skiprows=6, na_values=['x'], engine='openpyxl', usecols='A:M', nrows=434)
df.columns = ['Area Codes', 'Area Names', 'Geographical Designation', '2011-12', '2012-13', '2013-14', '2014-15',
              '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
df = df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='inner', left_on='Area Codes', right_on='lad21cd')
life_satisfaction['Happy'] = df
df = xl.parse(sheet_name='Anxiety - Means', skiprows=6, na_values=['x'], engine='openpyxl', usecols='A:M', nrows=434)
df.columns = ['Area Codes', 'Area Names', 'Geographical Designation', '2011-12', '2012-13', '2013-14', '2014-15',
              '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
df = df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='inner', left_on='Area Codes', right_on='lad21cd')
life_satisfaction['Anxiety'] = df

# now get the complete, long-form dataset and add that too
url = 'https://download.ons.gov.uk/downloads/datasets/wellbeing-local-authority/editions/time-series/versions/3.csv'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Life Satisfaction (full dataset) data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Life Satisfaction (full dataset) data already exists. Loading it.')
full_dataset = pd.read_csv(os.path.join('input_data', filename))
life_satisfaction['full_dataset'] = full_dataset

###################################################################################
# Skills
###################################################################################

# Skills data come from the APS/LFS, which we can access via the NomisWeb API

# set a filename and filepath to save the CSV we're going to create.
# I'm doing this up front so I can check for it's existence before downloading the data again.
filename = 'nomis_download_skills.csv'
filepath = os.path.join('input_data', filename)

# Set out some building blocks for the API call
nomis_api = "https://www.nomisweb.co.uk/api/v01/dataset/NM_17_1.data.csv?geography=1807745025...1807745028,1807745030...1807745032,1807745034...1807745083,1807745085,1807745282,1807745283,1807745086...1807745155,1807745157...1807745164,1807745166...1807745170,1807745172...1807745177,1807745179...1807745194,1807745196,1807745197,1807745199,1807745201...1807745218,1807745221,1807745222,1807745224,1807745226...1807745231,1807745233,1807745234,1807745236...1807745244,1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730&date=latest&cell=403898625...403898631&measures=20100,20701"
nomis_base = "https://www.nomisweb.co.uk/api/v01/dataset/"
# experiment with a string to get all LADs (I got this from using the NomisWeb query builder and selecting both types of local authority
nomis_all_lad_geog_native = "1807745025...1807745028,1807745030...1807745032,1807745034...1807745083,1807745085,1807745282,1807745283,1807745086...1807745155,1807745157...1807745164,1807745166...1807745170,1807745172...1807745177,1807745179...1807745194,1807745196,1807745197,1807745199,1807745201...1807745218,1807745221,1807745222,1807745224,1807745226...1807745231,1807745233,1807745234,1807745236...1807745244,1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730"
# use lad21cd form the lad lookup. This seems to drop North Northamptonshire and West Northamptonshire (I guess there was a merge in 2021?)
# But it's good to know I can use standard geographies to call the API - it should make it easy to construct queries.
nomis_all_lad_geog = ','.join(lad_lookup.lad21cd.to_list())
nomis_skills_dataset = "NM_17_5"
nomis_skills_dataset_params = "&date=latestMINUS2&cell=403898625...403898631&measures=20100"
nomis_skills_dataset_params_v1 = "&date=latestMINUS2&variable=720&measures=20599"
# combine the building blocks
api = nomis_base+nomis_skills_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_skills_dataset_params
api_v1 = nomis_base+nomis_skills_dataset+".data.csv?"+"geography="+nomis_all_lad_geog+nomis_skills_dataset_params_v1

if os.path.isfile(filepath) == False:
    print('Downloading skills dataset from NomisWeb.')
    # call the API from Pandas
    skills_df = pd.read_csv(api_v1)

    # write the data to CSV to save it for later
    filename = 'nomis_download_skills.csv'
    filepath = os.path.join('input_data', filename)
    skills_df.to_csv(filepath, index=False)
else:
    print('Skills data already downloaded from NomisWeb. Loading it.')
    skills_df = pd.read_csv(filepath, header=0)

# merge in the lookup data
skills_df = skills_df.merge(lad_lookup.loc[:,['lad21cd', 'lad21nm', 'cty21nm', 'rgn21nm_filled']], how='left', left_on='GEOGRAPHY_CODE', right_on='lad21cd')

# put it into a dictionary to keep it consistent with the other datasets
skills = {}
skills['NVQ3+'] = skills_df

###################################################################################
# VOA rateable values
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
pua_exlondon = pd.read_fwf(os.path.join('input_data', 'pua_definitions_exlondon.txt'), header=None)
pua_exlondon = pua_exlondon[0] + ' ' + pua_exlondon[1].fillna('') + ' ' + pua_exlondon[2].fillna('')
temp = [x.strip().split(' ',1) for x in pua_exlondon]
_key = [x[0] for x in temp]
_value = [x[1] for x in temp]
_value = [x.split(', ') for x in _value]

pua_london = pd.read_fwf(os.path.join('input_data', 'pua_list_london.txt'), header=None)
pua_london = pua_london[0] + ' ' + pua_london[1].fillna('')
pua_london = pua_london.to_list()
pua_london = ' '.join(pua_london)
pua_london = re.sub('  ',' ',pua_london)
pua_london = pua_london.split(', ')

pua_dict = dict(zip(_key, _value))
pua_dict['London'] = pua_london

###################################################################################
# ONS median HPI at LSOA level
###################################################################################

# Get median prices paid by LSOA from the ONS
# Not really that interesting, but I thought I was downloading a price index at first
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/housing/datasets/medianpricepaidbylowerlayersuperoutputareahpssadataset46/current/hpssadataset46medianpricepaidforresidentialpropertiesbylsoa.xls'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading ONS HPI data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('ONS HPI data already exists. Loading it.')
hpi = {}
# Read the Excel file
xl = pd.ExcelFile(filepath)
# parse the Life Satisfaction sheet and add it to the dictionary
df = xl.parse(sheet_name='Data', skiprows=5, na_values=[':'], usecols='A:DF', engine='xlrd')
df.columns = [re.sub('Year ending ','',x) for x in df.columns]
df = df.merge(lad_lookup.loc[:,['lad21cd','cty21nm','rgn21nm_filled', 'ctry21nm']], how='left', left_on='Local authority code', right_on='lad21cd')
hpi['ons_median_price_paid_lsoa'] = df

###################################################################################
# Indices of Deprivation
###################################################################################

url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/845345/File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Indices of Deprivation data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Indices of Deprivation data already exists. Loading it.')
indices_of_deprivation = {}
# Read the csv file
df = pd.read_csv(filepath)
indices_of_deprivation['indices'] = df

# get some of the underlying, granular indicators (I needed air quality in particular)
url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833992/File_8_-_IoD2019_Underlying_Indicators.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Indices of Deprivation data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Indices of Deprivation data already exists. Loading it.')
# Read the csv file
xl = pd.ExcelFile(filepath, engine='openpyxl')
df = xl.parse('IoD2019 Living Env Domain', engine='openpyxl', usecols='A:L')
indices_of_deprivation['underlying living env indicators'] = df

###################################################################################
# GVA by LAD and industry
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
    filepath = os.path.join('input_data', filename)
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
# patents work
###################################################################################

filename = 'corecities_nuts2_updated.xlsx'
filepath = os.path.join('input_data', filename)
patents = {}
patents['rta'] = pd.read_excel(filepath, sheet_name='corecities_nuts2', engine='openpyxl')
patents['innovation_distribution'] = pd.read_excel(filepath, sheet_name='Innovation Distribution - UK', engine='openpyxl')
patents['innovation_intensity'] = rta = pd.read_excel(filepath, sheet_name='Innovation Intensity - UK', header=[0,1], engine='openpyxl')
patents['istrax'] = pd.read_excel(filepath, sheet_name='ISTRAX', engine='openpyxl')

###################################################################################
# visitor attractions
###################################################################################

# First get the 2021 dataset, as it has postcodes. Then merge in the 2022 dataset which doesn't have postcodes for some reason.
url = 'https://www.visitbritain.org/sites/default/files/vb-corporate/Domestic_Research/vva_full_attractions_listings_2021_website.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Vist Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit Britain data already exists. Loading it.')
hpi = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# parse the Life Satisfaction sheet and add it to the dictionary
df21 = xl.parse(sheet_name='Permission to publish', na_values=['Not available'], usecols='A:U', nrows=888).drop('Unnamed: 19', axis=1)

# Now get the 2022 dataset
url = 'https://www.visitbritain.org/sites/default/files/vb-corporate/Domestic_Research/annual_attractions_full_listings_2022_v2.xlsx'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading Vist Britain data.')
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('Visit Britain data already exists. Loading it.')
hpi = {}
# Read the Excel file
xl = pd.ExcelFile(filepath, engine='openpyxl')
# parse the Life Satisfaction sheet and add it to the dictionary
df22 = xl.parse(sheet_name='With permissions', na_values=['Not available'], usecols='A:S', nrows=1114)

# look at how well they match up. Not that well! Annoying.
matches_id = set(df21['Provider_Product_ID']).intersection(set(df22['providerid']))
matches_name = set(df21['Attraction']).intersection(set(df22['Attraction']))

# get postcode lookup and merge it in
pcode_lookup = pd.read_csv(os.path.join('input_data', 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU', 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU.csv'), low_memory=False, encoding='unicode_escape')
df21 = df21.merge(pcode_lookup.loc[:,['pcds', 'ladcd', 'ladnm']], how='left', left_on='Postcode', right_on='pcds')
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





