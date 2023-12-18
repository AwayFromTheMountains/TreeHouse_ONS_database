# a script to make a matching utility for geography codes
# The various inputs are hard-coded. It expects the code lists to already be in 'data_folder'.
# To update this in future years, get an updated list of names and codes for local authorities and work through the
# logic in this script.

import pandas as pd
import os
import pickle
data_folder = 'data downloads'

def build_lad_mappers():
    print('Starting to build LAD mappings...')
    ####################################################################
    # Read the lists of codes and names and put them in a dictionary
    ####################################################################

    code_lists = ['LAD_(April_1991)_Names_and_Codes_in_England_and_Wales.csv',
     'LAD_(April_2015)_Names_and_Codes_in_the_United_Kingdom.csv',
     'LAD_(Dec_2016)_Names_and_Codes_in_the_United_Kingdom.csv',
     'LAD_(Dec_2017)_Names_and_Codes_in_the_United_Kingdom.csv',
     'LAD_(Dec_2018)_Names_and_Codes_in_the_United_Kingdom.csv',
     'LAD_(Dec_2019)_Names_and_Codes_in_the_United_Kingdom.csv',
     'LAD_(Dec_2020)_Names_and_Codes_in_the_United_Kingdom.csv',
     'Local_Authority_Districts_(April_2023)_Names_and_Codes_in_the_United_Kingdom.csv']
    years = [1991, 2015, 2016, 2017, 2018, 2019, 2020, 2023]

    code_years = {}
    for filename, year in zip(code_lists, years):
        if year<=2020:
            code_years[year] = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', filename), index_col=0).iloc[:,:2]
        else:
            code_years[year] = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', filename)).iloc[:,:2]
    code_years[2011] = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'Local_Authority_Districts_December_2011_GB_BFE_2022_624631654860830960.csv')).loc[:,['LAD11NM', 'LAD11CD']].drop_duplicates()
    code_years[2021] = pd.read_excel(os.path.join(data_folder, 'local_authority_boundaries', 'LAD_DEC_2021_UK_NC.xlsx'), engine='openpyxl').loc[:,['LAD21NM', 'LAD21CD']].drop_duplicates()
    code_years[2016] = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'LAD_(Dec_2016)_Names_and_Codes_in_the_United_Kingdom.csv')).loc[:,['LAD16NM', 'LAD16CD']].drop_duplicates()

    ################################################################################
    # Merge the years into wide dataframes, merged on names and codes separately
    ################################################################################

    # Merge them into a mega-dataframe
    merge_on_code = code_years[2011]\
        .merge(code_years[2015].loc[:,['LAD15CD', 'LAD15NM']], how='outer', left_on='LAD11CD', right_on='LAD15CD')\
        .merge(code_years[2016].loc[:,['LAD16CD', 'LAD16NM']], how='outer', left_on='LAD15CD', right_on='LAD16CD')\
        .merge(code_years[2017].loc[:,['LAD17CD', 'LAD17NM']], how='outer', left_on='LAD16CD', right_on='LAD17CD')\
        .merge(code_years[2018].loc[:,['LAD18CD', 'LAD18NM']], how='outer', left_on='LAD17CD', right_on='LAD18CD')\
        .merge(code_years[2019].loc[:,['LAD19CD', 'LAD19NM']], how='outer', left_on='LAD18CD', right_on='LAD19CD')\
        .merge(code_years[2020].loc[:,['LAD20CD', 'LAD20NM']], how='outer', left_on='LAD19CD', right_on='LAD20CD')\
        .merge(code_years[2021].loc[:,['LAD21CD', 'LAD21NM']], how='outer', left_on='LAD20CD', right_on='LAD21CD')\
        .merge(code_years[2023].loc[:,['LAD23CD', 'LAD23NM']], how='outer', left_on='LAD21CD', right_on='LAD23CD')

    merge_on_name = code_years[2011]\
        .merge(code_years[2015].loc[:,['LAD15CD', 'LAD15NM']], how='outer', left_on='LAD11NM', right_on='LAD15NM')\
        .merge(code_years[2016].loc[:,['LAD16CD', 'LAD16NM']], how='outer', left_on='LAD15NM', right_on='LAD16NM')\
        .merge(code_years[2017].loc[:,['LAD17CD', 'LAD17NM']], how='outer', left_on='LAD16NM', right_on='LAD17NM')\
        .merge(code_years[2018].loc[:,['LAD18CD', 'LAD18NM']], how='outer', left_on='LAD17NM', right_on='LAD18NM')\
        .merge(code_years[2019].loc[:,['LAD19CD', 'LAD19NM']], how='outer', left_on='LAD18NM', right_on='LAD19NM')\
        .merge(code_years[2020].loc[:,['LAD20CD', 'LAD20NM']], how='outer', left_on='LAD19NM', right_on='LAD20NM')\
        .merge(code_years[2021].loc[:,['LAD21CD', 'LAD21NM']], how='outer', left_on='LAD20NM', right_on='LAD21NM')\
        .merge(code_years[2023].loc[:,['LAD23CD', 'LAD23NM']], how='outer', left_on='LAD21NM', right_on='LAD23NM')

    ################################################################################
    # Create some lists of codes and names to use for identifying a code or name
    ################################################################################

    # Starting with the 2023 codes, loop back through previous lists and add any codes
    # that aren't present, along with the year they dropped out of existence.
    all_years_23 = code_years[2023].loc[:,['LAD23NM', 'LAD23CD']]
    all_years_23['year code'] = 'LAD23'
    all_years_23.columns = ['LADNM', 'LADCD', 'year code']
    for year in [2021, 2020, 2019, 2018, 2017, 2016, 2015, 2011]:
        specific_year = code_years[year].copy()
        col = 'LAD'+str(year-2000)+'CD'
        diff = list(set(specific_year[col]) - set(all_years_23['LADCD']))
        to_add = specific_year[specific_year[col].isin(diff)].copy()
        to_add['year code'] = 'LAD'+str(year-2000)
        to_add = to_add.rename({'LAD'+str(year-2000)+'CD':'LADCD', 'LAD'+str(year-2000)+'NM':'LADNM'}, axis=1)
        all_years_23 = pd.concat([all_years_23, to_add], axis=0)
        #print('Year: {}\nAdded: {} LADs.\n'.format(year, len(diff)))

    # Starting with the 2023 names, loop back through previous lists and add any names
    # that aren't present, along with the year they dropped out of existence.
    all_years_23_names = code_years[2023].loc[:,['LAD23NM', 'LAD23CD']]
    all_years_23_names['year code'] = 'LAD23'
    all_years_23_names.columns = ['LADNM', 'LADCD', 'year code']
    for year in [2021, 2020, 2019, 2018, 2017, 2016, 2015, 2011]:
        specific_year = code_years[year].copy()
        col = 'LAD'+str(year-2000)+'NM'
        diff = list(set(specific_year[col]) - set(all_years_23_names['LADNM']))
        to_add = specific_year[specific_year[col].isin(diff)].copy()
        to_add['year code'] = 'LAD'+str(year-2000)
        to_add = to_add.rename({'LAD'+str(year-2000)+'CD':'LADCD', 'LAD'+str(year-2000)+'NM':'LADNM'}, axis=1)
        all_years_23_names = pd.concat([all_years_23_names, to_add], axis=0)
        #print('Year: {}\nAdded: {} LADs.\n'.format(year, len(diff)))

    # Starting with the 2021 codes, loop back through previous lists and add any codes
    # that aren't present, along with the year they dropped out of existence.
    all_years_21 = code_years[2021].loc[:,['LAD21NM', 'LAD21CD']]
    all_years_21['year code'] = 'LAD21'
    all_years_21.columns = ['LADNM', 'LADCD', 'year code']
    for year in [2020, 2019, 2018, 2017, 2016, 2015, 2011]:
        specific_year = code_years[year].copy()
        col = 'LAD'+str(year-2000)+'CD'
        diff = list(set(specific_year[col]) - set(all_years_21['LADCD']))
        to_add = specific_year[specific_year[col].isin(diff)].copy()
        to_add['year code'] = 'LAD'+str(year-2000)
        to_add = to_add.rename({'LAD'+str(year-2000)+'CD':'LADCD', 'LAD'+str(year-2000)+'NM':'LADNM'}, axis=1)
        all_years_21 = pd.concat([all_years_21, to_add], axis=0)
        #print('Year: {}\nAdded: {} LADs.\n'.format(year, len(diff)))

    # Starting with the 2021 names, loop back through previous lists and add any names
    # that aren't present, along with the year they dropped out of existence.
    all_years_21_names = code_years[2021].loc[:,['LAD21NM', 'LAD21CD']]
    all_years_21_names['year code'] = 'LAD21'
    all_years_21_names.columns = ['LADNM', 'LADCD', 'year code']
    for year in [2020, 2019, 2018, 2017, 2016, 2015, 2011]:
        specific_year = code_years[year].copy()
        col = 'LAD'+str(year-2000)+'NM'
        diff = list(set(specific_year[col]) - set(all_years_21_names['LADNM']))
        to_add = specific_year[specific_year[col].isin(diff)].copy()
        to_add['year code'] = 'LAD'+str(year-2000)
        to_add = to_add.rename({'LAD'+str(year-2000)+'CD':'LADCD', 'LAD'+str(year-2000)+'NM':'LADNM'}, axis=1)
        all_years_21_names = pd.concat([all_years_21_names, to_add], axis=0)
        #print('Year: {}\nAdded: {} LADs.\n'.format(year, len(diff)))


    ################################################################################
    # Carefully work out changes from year to year
    # NB this was originally focused on mapping everything to LAD21, so all the
    # changes from year to year include the LAD21NM and LAD21CD to which they can
    # be mapped, except the changes from 22-23.
    ################################################################################

    # 21-23
    # NB there were no changes from 21 to 22
    code_changes_21_23 = all_years_23[all_years_23['year code']=='LAD21'].copy()
    name_changes_21_23 = all_years_23_names[all_years_23_names['year code']=='LAD21'].copy()
    #print('Code Changes:\n{}'.format(code_changes_21_23))
    #print('Name Changes:\n{}'.format(name_changes_21_23))
    row_checks = list(set(code_changes_21_23.index).difference(set(name_changes_21_23.index)).union(set(name_changes_21_23.index).difference(set(code_changes_21_23.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))

    changes_21_23 = pd.DataFrame([['Allerdale', 'Carlisle', 'Copeland'] + ['Barrow-in-Furness', 'Eden', 'South Lakeland'] + ['Craven', 'Hambleton', 'Harrogate', 'Richmondshire', 'Ryedale', 'Scarborough', 'Selby'] + ['Mendip', 'Sedgemoor', 'Somerset West and Taunton', 'South Somerset'],
                                  ['Cumberland']*3+['Westmorland and Furness']*3+['North Yorkshire']*7+['Somerset']*4], index=['LAD21NM', 'LAD23NM']).transpose()
    changes_21_23 = changes_21_23.merge(code_years[2021], how='left', on='LAD21NM')
    changes_21_23 = changes_21_23.merge(code_years[2023], how='left', on='LAD23NM')
    changes_21_23 = changes_21_23.loc[:,['LAD21CD', 'LAD21NM', 'LAD23CD', 'LAD23NM']]

    # 21-22
    # No changes!

    # 20-21
    code_changes_20_21 = all_years_23[all_years_23['year code']=='LAD20'].copy()
    name_changes_20_21 = all_years_23_names[all_years_23_names['year code']=='LAD20'].copy()
    #print('Code Changes:\n{}'.format(code_changes_20_21))
    #print('Name Changes:\n{}'.format(name_changes_20_21))
    row_checks = list(set(code_changes_20_21.index).difference(set(name_changes_20_21.index)).union(set(name_changes_20_21.index).difference(set(code_changes_20_21.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))

    changes_20_21 = pd.DataFrame([['Corby', 'East Northamptonshire', 'Kettering', 'Wellingborough'] + ['Daventry', 'Northampton', 'South Northamptonshire'],
                                  ['North Northamptonshire']*4+['West Northamptonshire']*3], index=['LAD20NM', 'LAD21NM']).transpose()
    changes_20_21 = changes_20_21.merge(code_years[2021], how='left', on='LAD21NM')
    changes_20_21 = changes_20_21.merge(code_years[2020], how='left', on='LAD20NM')
    changes_20_21 = changes_20_21.loc[:,['LAD20CD', 'LAD20NM', 'LAD21CD', 'LAD21NM']]

    # 19-20
    code_changes_19_20 = all_years_23[all_years_23['year code']=='LAD19'].copy()
    name_changes_19_20 = all_years_23_names[all_years_23_names['year code']=='LAD19'].copy()
    #print('Code Changes:\n{}'.format(code_changes_19_20))
    #print('Name Changes:\n{}'.format(name_changes_19_20))
    row_checks = list(set(code_changes_19_20.index).difference(set(name_changes_19_20.index)).union(set(name_changes_19_20.index).difference(set(code_changes_19_20.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))

    changes_19_20 = pd.DataFrame([['Aylesbury Vale', 'Chiltern', 'South Bucks', 'Wycombe'],
                                  ['Buckinghamshire']*4], index=['LAD19NM', 'LAD20NM']).transpose()
    changes_19_20 = changes_19_20.merge(code_years[2020], how='left', on='LAD20NM')
    changes_19_20 = changes_19_20.merge(code_years[2019], how='left', on='LAD19NM')
    changes_19_20 = changes_19_20.loc[:,['LAD19CD', 'LAD19NM', 'LAD20CD', 'LAD20NM']]

    # 18-19
    code_changes_18_19 = all_years_23[all_years_23['year code']=='LAD18'].copy()
    name_changes_18_19 = all_years_23_names[all_years_23_names['year code']=='LAD18'].copy()
    #print('Code Changes:\n{}'.format(code_changes_18_19))
    #print('Name Changes:\n{}'.format(name_changes_18_19))
    row_checks = list(set(code_changes_18_19.index).difference(set(name_changes_18_19.index)).union(set(name_changes_18_19.index).difference(set(code_changes_18_19.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))

    changes_18_19 = pd.DataFrame([['Bournemouth', 'Poole', 'Christchurch'] + ['East Dorset', 'North Dorset', 'Purbeck', 'West Dorset', 'Weymouth and Portland'] + ['Taunton Deane', 'West Somerset'] + ['Forest Heath', 'St Edmundsbury'] + ['Suffolk Coastal', 'Waveney'] + ['Glasgow City', 'North Lanarkshire'],
                                  ['Bournemouth, Christchurch and Poole']*3+['Dorset']*5+['Somerset West and Taunton']*2+['West Suffolk']*2+['East Suffolk']*2 + ['Glasgow City', 'North Lanarkshire']], index=['LAD18NM', 'LAD19NM']).transpose()
    changes_18_19 = changes_18_19.merge(code_years[2019], how='left', on='LAD19NM')
    changes_18_19 = changes_18_19.merge(code_years[2018], how='left', on='LAD18NM')
    changes_18_19 = changes_18_19.loc[:,['LAD18CD', 'LAD18NM', 'LAD19CD', 'LAD19NM']]
    # NB this includes 'Glasgow City' and 'North Lanarkshire', which have code changes but not name changes

    # 17-18
    code_changes_17_18 = all_years_23[all_years_23['year code']=='LAD17'].copy()
    name_changes_17_18 = all_years_23_names[all_years_23_names['year code']=='LAD17'].copy()
    #print('Code Changes:\n{}'.format(code_changes_17_18))
    #print('Name Changes:\n{}'.format(name_changes_17_18))
    row_checks = list(set(code_changes_17_18.index).difference(set(name_changes_17_18.index)).union(set(name_changes_17_18.index).difference(set(code_changes_17_18.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))
    # 'Fife' and 'Perth and Kinross' are code changes
    # 'Shepway' is a name change - it is called 'Folkestone and Hythe' from 2018 onwards
    changes_17_18 = pd.DataFrame([['Shepway', 'Fife', 'Perth and Kinross'],
                                  ['Folkestone and Hythe', 'Fife', 'Perth and Kinross']], index=['LAD17NM', 'LAD18NM']).transpose()
    changes_17_18 = changes_17_18.merge(code_years[2018], how='left', on='LAD18NM')
    changes_17_18 = changes_17_18.merge(code_years[2017], how='left', on='LAD17NM')
    changes_17_18 = changes_17_18.loc[:,['LAD17CD', 'LAD17NM', 'LAD18CD', 'LAD18NM']]

    # 16-17
    code_changes_16_17 = all_years_23[all_years_23['year code']=='LAD16'].copy()
    name_changes_16_17 = all_years_23_names[all_years_23_names['year code']=='LAD16'].copy()
    #print('Code Changes:\n{}'.format(code_changes_16_17))
    #print('Name Changes:\n{}'.format(name_changes_16_17))
    row_checks = list(set(code_changes_16_17.index).difference(set(name_changes_16_17.index)).union(set(name_changes_16_17.index).difference(set(code_changes_16_17.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))
    # No changes!

    # 15-16
    code_changes_15_16 = all_years_23[all_years_23['year code']=='LAD15'].copy()
    name_changes_15_16 = all_years_23_names[all_years_23_names['year code']=='LAD15'].copy()
    #print('Code Changes:\n{}'.format(code_changes_15_16))
    #print('Name Changes:\n{}'.format(name_changes_15_16))
    row_checks = list(set(code_changes_15_16.index).difference(set(name_changes_15_16.index)).union(set(name_changes_15_16.index).difference(set(code_changes_15_16.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))
    # I think there are some names in 2015 that only appear in 2015, so I'm going to ignore them. There are no code changes

    # 11-15
    code_changes_11_15 = all_years_23[all_years_23['year code']=='LAD11'].copy()
    name_changes_11_15 = all_years_23_names[all_years_23_names['year code']=='LAD11'].copy()
    #print('Code Changes:\n{}'.format(code_changes_11_15))
    #print('Name Changes:\n{}'.format(name_changes_11_15))
    row_checks = list(set(code_changes_11_15.index).difference(set(name_changes_11_15.index)).union(set(name_changes_11_15.index).difference(set(code_changes_11_15.index))))


    # Changes 11-17
    code_changes_11_17 = all_years_23[all_years_23['year code']=='LAD11'].copy()
    name_changes_11_17 = all_years_23_names[all_years_23_names['year code']=='LAD11'].copy()
    #print('Code Changes:\n{}'.format(code_changes_11_17))
    #print('Name Changes:\n{}'.format(name_changes_11_17))
    row_checks = list(set(code_changes_11_17.index).difference(set(name_changes_11_17.index)).union(set(name_changes_11_17.index).difference(set(code_changes_11_17.index))))
    #print('\nNumber of differences between the two approaches: {:.0f}'.format(len(row_checks)))
    changes_11_17 = pd.DataFrame([['Dumfries & Galloway', 'Eilean Siar', 'Perth & Kinross', 'Argyll & Bute', 'Edinburgh, City of', 'The Vale of Glamorgan'] + ['Northumberland', 'East Hertfordshire', 'St Albans', 'Stevenage', 'Welwyn Hatfield', 'Gateshead'],
                                  ['Dumfries and Galloway', 'Na h-Eileanan Siar', 'Perth and Kinross', 'Argyll and Bute', 'City of Edinburgh', 'Vale of Glamorgan'] + ['Northumberland', 'East Hertfordshire', 'St Albans', 'Stevenage', 'Welwyn Hatfield', 'Gateshead']], index=['LAD11NM', 'LAD17NM']).transpose()
    changes_11_17 = changes_11_17.merge(code_years[2017], how='left', on='LAD17NM')
    changes_11_17 = changes_11_17.merge(code_years[2011], how='left', on='LAD11NM')
    changes_11_17 = changes_11_17.loc[:,['LAD11CD', 'LAD11NM', 'LAD17CD', 'LAD17NM']]

    ################################################################
    # Now put all the changes together into a lookup dataframe
    ################################################################


    # For 2011 to 2017, first get all of the LADs that are unchanged, then concatenate in all of those that changed. This gives us a complete set of LADs for both 2011 and 2017.
    persistent = merge_on_code[(merge_on_code['LAD11CD']==merge_on_code['LAD17CD']) & (merge_on_code['LAD11NM']==merge_on_code['LAD17NM'])].loc[:,['LAD11CD', 'LAD11NM', 'LAD17CD', 'LAD17NM']]
    # NB this misses Northern Ireland out, because it comes in in 2015. So I'm adding it back in.
    persistent_NI_A = pd.DataFrame([['N09000001',
                                   'N09000002',
                                   'N09000003',
                                   'N09000004',
                                   'N09000005',
                                   'N09000006',
                                   'N09000007',
                                   'N09000008',
                                   'N09000009',
                                   'N09000010'],
     ['Antrim and Newtownabbey',
     'Armagh, Banbridge and Craigavon',
     'Belfast',
     'Causeway Coast and Glens',
     'Derry and Strabane',
     'Fermanagh and Omagh',
     'Lisburn and Castlereagh',
     'Mid and East Antrim',
     'Mid Ulster',
     'Newry, Mourne and Down']], index=['LAD11CD','LAD11NM']).transpose()
    persistent_NI_B = persistent_NI_A.copy()
    persistent_NI_B.columns = ['LAD17CD', 'LAD17NM']
    persistent_NI = pd.concat([persistent_NI_A, persistent_NI_B], axis=1)
    persistent = pd.concat([persistent, persistent_NI], axis=0)

    combo = pd.concat([persistent, changes_11_17], axis=0)
    # Next, merge in the changes from 2017 to 2018. This again keeps a full set, but it also leaves a lot of NA, which I now need to right fill.
    combo = combo.merge(changes_17_18, how='left')
    combo['LAD18CD'] = combo.apply(lambda x: x['LAD18CD'] if pd.notna(x['LAD18CD']) else x['LAD17CD'], axis=1)
    combo['LAD18NM'] = combo.apply(lambda x: x['LAD18NM'] if pd.notna(x['LAD18NM']) else x['LAD17NM'], axis=1)
    # Now repeat for the changes from 2018 to 2019
    combo = combo.merge(changes_18_19, how='left')
    combo['LAD19CD'] = combo.apply(lambda x: x['LAD19CD'] if pd.notna(x['LAD19CD']) else x['LAD18CD'], axis=1)
    combo['LAD19NM'] = combo.apply(lambda x: x['LAD19NM'] if pd.notna(x['LAD19NM']) else x['LAD18NM'], axis=1)
    # Now repeat for the changes from 2019 to 2020
    combo = combo.merge(changes_19_20, how='left')
    combo['LAD20CD'] = combo.apply(lambda x: x['LAD20CD'] if pd.notna(x['LAD20CD']) else x['LAD19CD'], axis=1)
    combo['LAD20NM'] = combo.apply(lambda x: x['LAD20NM'] if pd.notna(x['LAD20NM']) else x['LAD19NM'], axis=1)
    # Now repeat for the changes from 2020 to 2021
    combo = combo.merge(changes_20_21, how='left')
    combo['LAD21CD'] = combo.apply(lambda x: x['LAD21CD'] if pd.notna(x['LAD21CD']) else x['LAD20CD'], axis=1)
    combo['LAD21NM'] = combo.apply(lambda x: x['LAD21NM'] if pd.notna(x['LAD21NM']) else x['LAD20NM'], axis=1)
    # Now repeat for the changes from 2021 to 2023
    combo = combo.merge(changes_21_23, how='left')
    combo['LAD23CD'] = combo.apply(lambda x: x['LAD23CD'] if pd.notna(x['LAD23CD']) else x['LAD21CD'], axis=1)
    combo['LAD23NM'] = combo.apply(lambda x: x['LAD23NM'] if pd.notna(x['LAD23NM']) else x['LAD21NM'], axis=1)



    ################################################################################
    # Make a version of all_years_23 that links through to the LAD23CD and NM
    # repeat for all_years_21 in case we need that
    ################################################################################

    year_code_list = ['LAD23', 'LAD21', 'LAD20', 'LAD19', 'LAD18', 'LAD17', 'LAD11']
    df_list = []
    for year in year_code_list:
        year_code = year+'CD'
        temp_df_a = all_years_23[all_years_23['year code'] == year].copy()
        temp_df_b = all_years_23_names[all_years_23_names['year code'] == year].copy()
        temp_df = pd.concat([temp_df_a, temp_df_b], axis=0).drop_duplicates()
        if year == 'LAD23':
            temp_df = temp_df.merge(combo.loc[:,['LAD23NM', 'LAD23CD']], how='left', left_on='LADCD',
                                    right_on=year_code)
            temp_df['LAD21NM'] = temp_df['LAD23NM']
            temp_df['LAD21CD'] = temp_df['LAD23CD']
        elif year == 'LAD21':
            temp_df = temp_df.merge(combo.loc[:,['LAD21NM', 'LAD21CD', 'LAD23NM', 'LAD23CD']], how='left', left_on='LADCD',
                                    right_on=year_code)
        else:
            temp_df = temp_df.merge(combo.loc[:,[year_code, 'LAD21NM', 'LAD21CD', 'LAD23NM', 'LAD23CD']], how='left', left_on='LADCD',
                                    right_on=year_code)
        temp_df = temp_df.drop_duplicates()
        df_list.append(temp_df)
    lad_multiyear_lookup = pd.concat(df_list, axis=0).iloc[:,:7]

    ################################################################################
    # Now save it
    ################################################################################
    print('Saving the mappings as CSVs')
    # change column headers to lower case and save
    combo.columns = [x.lower() for x in combo.columns]
    lad_multiyear_lookup.columns = [x.lower() for x in lad_multiyear_lookup.columns]
    lad_multiyear_lookup = lad_multiyear_lookup.rename({'year code':'year_code'}, axis=1) # for use in database
    combo.to_csv(os.path.join(data_folder, 'local_authority_boundaries', 'LA_mappings.csv'), index=False)
    lad_multiyear_lookup.to_csv(os.path.join(data_folder, 'local_authority_boundaries', 'LAD_multiyear_lookup.csv'), index=False)


# Run the script if
if __name__ == '__main__':
    build_lad_mappers()