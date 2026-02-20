#!/usr/bin/env python
# coding: utf-8

# Import packages
import os
import pandas as pd
from psycopg2 import connect
from datetime import datetime, timedelta
import numpy as np
os.chdir('/Volumes/lil_baby/2019_db_redcpa')
print(os.getcwd())

# os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
# os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
# import modin.pandas as pd

# ### use database backup file to load into db

# In[2]:
# last_backup=datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
# last_backup="backup_%s_00_00.sql.gz"%(last_backup)
last_backup = "backup_20200811_00_00.sql.gz"
# os.system('rclone copyto stanford_box:/"SCSNL Database"/2019/%s %s; gunzip %s'%(last_backup,last_backup,last_backup))


# In[3]:
os.system("dropdb scsnl; createdb scsnl; psql -d scsnl -U daelsaid -f %s" %
          last_backup.rstrip('.gz'))
conn = connect(dbname='scsnl', user='daelsaid', host='localhost')


# In[4]:
tables_df = pd.read_sql(
    "SELECT * FROM information_schema.tables where table_schema = 'public'", conn)
table_name_list = tables_df.table_name

select_template = 'SELECT * FROM {table_name}'
scsnl = dict()
for tname in table_name_list:
    query = select_template.format(table_name=tname)
    scsnl[tname] = pd.read_sql(query, conn)

data_tables = [x for x in scsnl.keys() if 'data_' in x]
scsnl_data = pd.concat([scsnl[table] for table in data_tables])


# In[6]:
print(scsnl['formfields'].columns)
print(scsnl['formfields'].shape)
formfields = scsnl['formfields']
# for idx,ff in scsnl['formfields'].iterrows():


# In[7]:
subj_data = pd.DataFrame()
print(scsnl_data.shape)
print(scsnl_data.columns)
print(scsnl_data.head(5))
scsnl_data_df = scsnl_data.drop('id', axis=1)
# scsnl_data_df=scsnl_data_df.set_index(['pid','visit','dataset'])
ffid_list = ['ffid_%d' % x for x in sorted(
    scsnl_data_df.ffid.unique().tolist())]
#test=pd.pivot_table(scsnl_data_df, index=['pid', 'visit','dataset'],columns=['ffid'],values='data',aggfunc=lambda x: ' '.join(str(v) for v in x))


# In[8]:

scsnl_data_df = scsnl_data.drop('id', axis=1)

formfields = scsnl['formfields']
formsets = scsnl['formsets']
formfields = formfields.set_index(['ffid'])
formsets = formsets.set_index(['fsid'])


def full_ffid_name(ffid):
    fsid = formfields.at[ffid, 'fsid']
    ffid_name = formfields.at[ffid, 'unique_name']
    fsid_name = formsets.at[fsid, 'name']
    order = formfields.at[ffid, 'order']
    full_ffid = 'fsid%04d-%s_%04d_ffid%06d_%s' % (
        fsid, fsid_name, order, ffid, ffid_name)
    full_ffid = full_ffid[:99].lower()
    full_ffid = full_ffid.replace(' ', '_')
    full_ffid = ''.join(char for char in full_ffid if (
        char.isalnum() or char == '_'))

    return full_ffid


scsnl_data_df = scsnl_data.drop('id', axis=1)
scsnl_data_df['ffid'] = scsnl_data_df.apply(
    lambda row: full_ffid_name(row['ffid']), axis=1)
print(scsnl_data_df['ffid'].head())


# In[9]:
scsnl_data_df.data = scsnl_data_df.data.astype(str).replace(',', ' ')
scsnl_data_df = pd.pivot_table(scsnl_data_df, index=['pid', 'visit', 'dataset'], columns=[
                               'ffid'], values='data', aggfunc=lambda x: ' '.join(str(v) for v in x))
scsnl_data_df = scsnl_data_df.reset_index(level=['pid', 'visit', 'dataset'])
scsnl_data_df.index.rename('record_id', inplace=True)
scsnl_data_df.to_csv('scsnl_data_redcap.csv')
scsnl_data_df.head(1).to_csv('scsnl_data_redcap_001.csv')

# In[10]:
print(scsnl_data_df.head(2))

# In[11]:
scsnl_data_df.to_csv('scsnl_data_redcap.csv')
new_formfields = scsnl_data_df.columns.tolist()

# ## Make project Data Dictionary
# In[12]:
new_formfields = scsnl_data_df.columns.tolist()
data_dict = pd.DataFrame(columns=['Variable / Field Name', 'Form Name', 'Section Header', 'Field Type', 'Field Label', '"Choices', ' Calculations', ' OR Slider Labels"', 'Field Note', 'Text Validation Type OR Show Slider Number',
                                  'Text Validation Min', 'Text Validation Max', 'Identifier?', 'Branching Logic (Show field only if...)', 'Required Field?', 'Custom Alignment', 'Question Number (surveys only)', 'Matrix Group Name', 'Matrix Ranking?', 'Field Annotation'])

data_dict.loc[0, 'Variable / Field Name'] = 'record_id'
data_dict.loc[0, 'Form Name'] = 'main_info'
data_dict.loc[0, 'Field Type'] = 'text'
data_dict.loc[0, 'Field Label'] = 'record_id'

data_dict.loc[1, 'Variable / Field Name'] = 'pid'
data_dict.loc[1, 'Form Name'] = 'main_info'
data_dict.loc[1, 'Field Type'] = 'text'
data_dict.loc[1, 'Field Label'] = 'pid'

data_dict.loc[2, 'Variable / Field Name'] = 'visit'
data_dict.loc[2, 'Form Name'] = 'main_info'
data_dict.loc[2, 'Field Type'] = 'text'
data_dict.loc[2, 'Field Label'] = 'visit'

data_dict.loc[3, 'Variable / Field Name'] = 'dataset'
data_dict.loc[3, 'Form Name'] = 'main_info'
data_dict.loc[3, 'Field Type'] = 'text'
data_dict.loc[3, 'Field Label'] = 'dataset'

print(len(new_formfields))
print(new_formfields[:5])

for idx, ff in enumerate(new_formfields[3:]):
    data_dict.loc[idx + 4, 'Variable / Field Name'] = ff[:99]
    # print('_'.join(ff.split('ffid')[0].split('_')[:-2])[8:])
    data_dict.loc[(4 + idx), 'Form Name'] = '_'.join(ff.split('ffid')
                                                     [0].split('_')[:-2])[8:]
    #data_dict.loc[(4+idx),'Form Name'] = '_'.join('-'.join(ff.split('ffid')[0].split('-')[2:]).split('_')[:-2])
    data_dict.loc[(4 + idx), 'Field Type'] = 'text'
    ffid_num = ff.split('ffid')[-1].split('_')[0]
    ffid_num = '%d' % int(ffid_num)
    data_dict.loc[(4 + idx),
                  'Field Label'] = formfields.at[int(ffid_num), 'name']
#   print(data_dict.loc[(4+idx),'Field Label'],data_dict.loc[idx+4,'Variable / Field Name'])

data_dict.to_csv('Dev_DataDictionary_2020-08-11.csv', index=False)

# ## Redcap data import
# In[17]:
import redcap
project = redcap.Project('https://redcap.stanford.edu/api/',
                         '#####')
all_data = project.export_records()

import csv

chunk = []
imported = 0
chunk_size = 100

with open('scsnl_data_redcap.csv') as fobj:
    reader = csv.DictReader(fobj)
    for record in reader:
        chunk.append(record)
        if len(chunk) == chunk_size:
            # import this chunk of records
            project.import_records(chunk)
            # reset the chunk
            chunk = []
            imported += chunk_size
            print("Imported {} records".format(imported))
    # make sure to get the remainder
    # projec/t.import_records(chunk)


# In[55]:
print(data_dict.head(4))
