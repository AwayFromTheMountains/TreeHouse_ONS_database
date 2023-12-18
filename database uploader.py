# A programme to upload data to my ONS database

# import packages
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import os
import datetime

from utils.db_config import config

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


#######################################################################
# LAD mapppings
# a set of tables to allow us to map from local authoritie codes in a
# particular year to those in another year
#######################################################################

data_folder = 'data downloads'

### Do the LAD mappings first
###############################

lad_mappings = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'LA_mappings.csv'), index_col=None)
sql_string = ', '.join([x+' VARCHAR' for x in lad_mappings.columns])
parent_script = 'database_uploader.py'

# get the connection parameters
params = config(filename='geoproj_aws_db.ini')

# create a table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS lad_mappings (
                {},
                created timestamptz,
                parent_script VARCHAR
                )""".format(sql_string))
    cur.close()
    con.commit()

# add the metadata
lad_mappings['created'] = datetime.datetime.now()
lad_mappings['parent_script'] = parent_script

# upload the data
with psycopg2.connect(**params) as con:
    execute_values(df=lad_mappings, table='lad_mappings', con=con)

### now do the LAD lookup
###############################

lad_multiyear_lookup = pd.read_csv(os.path.join(data_folder, 'local_authority_boundaries', 'lad_multiyear_lookup.csv'), index_col=None)
sql_string = ', '.join([x+' VARCHAR' for x in lad_multiyear_lookup.columns])
parent_script = 'database_uploader.py'

# create a table
with psycopg2.connect(**params) as con:
    cur = con.cursor()
    # execute a create table query and commit it
    cur.execute("""CREATE TABLE IF NOT EXISTS lad_multiyear_lookup (
                {},
                created timestamptz,
                parent_script VARCHAR
                )""".format(sql_string))
    cur.close()
    con.commit()

# add the metadata
lad_multiyear_lookup['created'] = datetime.datetime.now()
lad_multiyear_lookup['parent_script'] = parent_script

# upload the data
with psycopg2.connect(**params) as con:
    execute_values(df=lad_multiyear_lookup, table='lad_multiyear_lookup', con=con)
