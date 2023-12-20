# load packages
import pandas as pd
import numpy as np
import os
import sys
import requests
import re
import pickle

################################################
# Get the GFCF by sector data
################################################
url = 'https://www.ons.gov.uk/file?uri=/economy/grossdomesticproductgdp/compendium/unitedkingdomnationalaccountsthebluebook/2022/supplementarytables/bb22chapter8tables.xlsx'
data_name = 'GFCF by sector'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')
gfcf_by_sector = xl.parse(sheet_name='8.1', index_col=0, usecols=[0,56,57,58,59,60,61,62], skiprows=3, nrows=76).iloc[2:,:]

# now get GDP at market prices to calculate the share of investment
url = 'https://www.ons.gov.uk/file?uri=/economy/grossdomesticproductgdp/compendium/unitedkingdomnationalaccountsthebluebook/2022/supplementarytables/bb2201naataglanceupdated.xlsx'
data_name = 'Blue Book'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')
blue_book = xl.parse(sheet_name='1.1', index_col=0, usecols=[0,18,], skiprows=3, nrows=76).iloc[2:,:]

### I ended up not bothering extracting a sheet for now as there are more urgent priorities

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
filepath = os.path.join('input_data', filename)
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
filepath = os.path.join('input_data', filename)
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
filepath = os.path.join('input_data', filename)
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
filepath = os.path.join('input_data', filename)
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
pop_lad = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))['population']['totals_by_lad']
pop_lad_long = pd.melt(pop_lad.reset_index(), id_vars=['lad21cd'], var_name='year', value_name='population')

# create a single, long dataframe that includes the year
df_list = []
for _key, year in zip(LA_investment_dict.keys(), ['2018','2019', '2020', '2021']):
    temp = LA_investment_dict[_key]
    temp['Year'] = year
    df_list.append(temp)
LA_investment = pd.concat(df_list, axis=0)
LA_investment['value'] = LA_investment['value'].astype('float')

# now merge in the population data
LA_investment_per_head = LA_investment.merge(pop_lad_long, how='left', left_on=['ONS Code', 'Year'], right_on=['lad21cd', 'year'])
LA_investment_per_head['value_per_head'] = 1000 * LA_investment_per_head['value'].div(LA_investment_per_head['population'])


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

#### Now calculate regional_GFCF per head, using population data from the data dictionary

# load the data dict and get population (i.e. by residence) and employment (by job location) data
pop_itl3 = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))['population']['totals_by_itl3']
pop_itl3_long = pd.melt(pop_itl3.reset_index(), id_vars=['itl321cd'], var_name='year', value_name='population')
regional_GFCF_per_head = regional_GFCF.merge(pop_itl3_long, how='left', left_on=['ITL3 code', 'Year'], right_on=['itl321cd', 'year'])
regional_GFCF_per_head['value_per_head'] = 1000000 * regional_GFCF_per_head['value'].div(regional_GFCF_per_head['population'])

employment_itl3 = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))['employment_by_itl3']
regional_GFCF_per_job = regional_GFCF.merge(employment_itl3, how='left', left_on=['ITL3 code', 'Year'], right_on=['ITL321CD', 'year'])
regional_GFCF_per_job['value_per_head'] = 1000000 * regional_GFCF_per_job['value'].div(regional_GFCF_per_job['employment'])

####################################################
# Get experimental GFCF by region for ITL2 regions
####################################################

url = 'https://www.ons.gov.uk/file?uri=/economy/regionalaccounts/grossdisposablehouseholdincome/datasets/experimentalregionalgrossfixedcapitalformationgfcfestimatesbyassettype/1997to2020/updatedexperimentalregionalgfcf19972020byassetandindustry.xlsx'
data_name = 'Regional GFCF by asset type'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
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

#### Now calculate itl2_GFCF per head and per job, using population data from the data dictionary

# load the data dict and get population (i.e. by residence) and employment (by job location) data
pop_itl2 = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))['population']['totals_by_itl2']
pop_itl2_long = pd.melt(pop_itl2.reset_index(), id_vars=['itl221cd'], var_name='year', value_name='population')
itl2_GFCF_per_head = itl2_GFCF.merge(pop_itl2_long, how='left', left_on=['ITL2 code', 'Year'], right_on=['itl221cd', 'year'])
itl2_GFCF_per_head['value_per_head'] = 1000000 * itl2_GFCF_per_head['value'].div(itl2_GFCF_per_head['population'])

employment_itl2 = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))['employment_by_itl2']
itl2_GFCF_per_job = itl2_GFCF.merge(employment_itl2, how='left', left_on=['ITL2 code', 'Year'], right_on=['ITL221CD', 'year'])
itl2_GFCF_per_job['value_per_head'] = 1000000 * itl2_GFCF_per_job['value'].div(itl2_GFCF_per_job['employment'])

####################################################
# Postcode to LAD lookup
####################################################

# load the lookup
pcode_lookup = pd.read_csv(os.path.join('input_data', 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU', 'PCD_OA21_LSOA21_MSOA21_LAD_NOV22_UK_LU.csv'), low_memory=False, encoding='unicode_escape')
# add a column for postcode district
pcode_lookup['district'] = [re.findall('[A-Z]{1,2}[0-9]{1,2} [0-9]{1}|[A-Z]{1,2}[0-9]{1}[A-Z]{1} [0-9]{1}',x)[0] if len(re.findall('[A-Z]{1,2}[0-9]{1,2} [0-9]{1}|[A-Z]{1,2}[0-9]{1}[A-Z]{1} [0-9]{1}',x))>0 else '' for x in pcode_lookup['pcds']]
# derive a lookup from district to LAD, dropping districts with no LAD (not sure why they occur)
district_to_lad = pcode_lookup.groupby('district').agg(ladcd=('ladcd',pd.Series.mode))
district_to_lad = district_to_lad[district_to_lad['ladcd'].str.len()>0]
district_to_lad = district_to_lad[district_to_lad.index.str.len()>0] # gets rid of a blank postcode district
district_to_lad['ladcd'] = district_to_lad['ladcd'].apply(lambda x: x[0] if type(x)==np.ndarray else x)

####################################################
# LAD to TTWA lookup
####################################################

# load the LAD to TTWA lookup and manipulate it into a useful shape
ttwa = pd.read_excel(os.path.join('input_data', '2021la2011ttwalookupv2.xlsx'), engine='openpyxl', sheet_name='2021 LAs by 2011 TTWAs', skiprows=2, usecols='A:G', nrows=1108)
ttwa = ttwa.loc[~ttwa.isna().all(axis=1),:]
ttwa_lookup = ttwa.fillna(method='ffill', axis=0)


####################################################
# Get UK Finance SME lending by postcode district
####################################################
url = 'https://www.ukfinance.org.uk/system/files/2023-01/GB%20SME%20Lending%20%28loans%20%26%20overdrafts%29.xlsx'
data_name = 'SME loans by postcode district'
req = requests.get(url)
filename = url.split('/')[-1]
filepath = os.path.join('input_data', filename)
if os.path.isfile(filepath) == False:
    print('Downloading {} data'.format(data_name))
    with open(filepath, 'wb') as output_file:
        output_file.write(req.content)
else:
    print('{} data already downloaded. Loading it.'.format(data_name))
xl = pd.ExcelFile(filepath, engine='openpyxl')
SME_loans_pcode = xl.parse(sheet_name='All postcode data', skiprows=7, header=0, na_values=['NIL', 'NiL', 'Nil', 'nil', 'terminated'], engine='openpyxl')
# add the LA
SME_loans_pcode = SME_loans_pcode.merge(district_to_lad, how='left', left_on='Sector', right_index=True)

# melt into long form
SME_loans_pcode = pd.melt(SME_loans_pcode, id_vars=['Region', 'Area', 'Area name', 'Sector', 'ladcd'], var_name='Time period', value_name='value')



#####################################################
# Add to the RSA data dictionary
#####################################################
rsa_data_dict = pickle.load(open(os.path.join('outputs','rsa_data_dict.p'),'rb'))
rsa_data_dict['LA investment'] = LA_investment_dict
rsa_data_dict['LA investment per head'] = LA_investment_per_head
rsa_data_dict['Regional GFCF'] = regional_GFCF
rsa_data_dict['Regional GFCF per head'] = regional_GFCF_per_head
rsa_data_dict['Regional GFCF per job'] = regional_GFCF_per_job
rsa_data_dict['ITL2 GFCF per head'] = itl2_GFCF_per_head
rsa_data_dict['ITL2 GFCF per job'] = itl2_GFCF_per_job
rsa_data_dict['SME loans by postcode district'] = SME_loans_pcode
rsa_data_dict['ttwa'] = ttwa_lookup
rsa_data_dict['gfcf_by_sector'] = gfcf_by_sector
rsa_data_dict['gdp_current'] = blue_book

pickle.dump(rsa_data_dict,open(os.path.join('outputs','rsa_data_dict.p'),'wb'))

