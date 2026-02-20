#!/usr/bin/env python
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"></ul></div>

# Import packages

# In[1]:
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

# In[2]:=
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


# In[6]
print(scsnl['formfields'].columns)
print(scsnl['formfields'].shape)
formfields = scsnl['formfields']


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


# createe mapping of studys and thier respective IDs
print(tables_df)
studies_test = [x for x in scsnl.keys() if 'studies' in x]
study_id_mapping = dict()
for idx, keys in scsnl['studies'].iterrows():
    study_id_mapping[keys.study_id] = keys.label
print([study_id_mapping])


# ### Create unique study identifiers to cross check against study id compiled for each subject in database file
# In[70]:
scsnl_data_df.visit = scsnl_data_df.visit.astype(int)
scsnl_data_df['subj_unique_identifier'] = scsnl_data_df['pid'].astype(
    str) + '_visit' + scsnl_data_df['visit'].astype(str)
scsnl_data_df['subj_unique_identifier']


# In[71]:
print(scsnl_data_df['subj_unique_identifier'].head(10))
study_id = pd.read_csv('all_study_subjects.csv', dtype=str)
study_id['whiz'] = study_id['asdwhiz_PID'].dropna().astype(
    str) + '_visit' + study_id['asdwhiz_visit'].dropna().astype(str)
study_id['mathfun'] = study_id['mathfun_PID'].astype(
    str) + '_visit' + study_id['mathfun_visit'].astype(str)
study_id['math8week'] = study_id['math_8week_PID'].astype(
    str) + '_visit' + study_id['math_8week_visit'].astype(str)
study_id['adhd'] = study_id['adhd_PID'].astype(
    str) + '_visit' + study_id['adhd_visit'].astype(str)
study_id['met'] = study_id['met_PID'].astype(
    str) + '_visit' + study_id['met_visit'].astype(str)
study_id['smp_orig'] = study_id['smp_PID'].astype(
    str) + '_visit' + study_id['smp_visit'].astype(str)
print(study_id)

print(study_id['smp_orig'].dropna())


# ### Make a redcap data import CSV for each project using data iin db

# #### SMP

# In[72]:
smporig = scsnl_data_df[(
    scsnl_data_df['subj_unique_identifier'].isin(study_id['smp_orig']))]

smporig.reset_index(level=['record_id'])
smporig.to_csv('smporig_from_db_for_rc_import.csv')
smporig.dropna(how='all', axis=1).to_csv(
    'smporig_dropna_from_db_for_rc_import.csv')


# #### MET (met can be parsed using study IDs since all 9000s are met and asd met specific)

# In[41]:
met = scsnl_data_df[(scsnl_data_df['pid'].isin(range(9000, 10000)))]
met.reset_index(level=['record_id'])
met.to_csv('met_from_db_for_rc_import.csv')
met.dropna(how='all', axis=1).to_csv('met_dropna_from_db_for_rc_import.csv')


# #### Mathfun

# In[42]:
mathfun = scsnl_data_df[(
    scsnl_data_df['subj_unique_identifier'].isin(study_id['mathfun']))]
mathfun.reset_index(level=['record_id'])
mathfun.to_csv('mathfun_from_db_for_rc_import.csv')
mathfun.dropna(how='all', axis=1).to_csv(
    'mathfun_dropna_from_db_for_rc_import.csv')

# #### Whiz

# In[43]:
whiz = scsnl_data_df[(
    scsnl_data_df['subj_unique_identifier'].isin(study_id['whiz']))]
whiz.reset_index(level=['record_id'])
whiz.dropna(how='all', axis=1).to_csv(
    'asdwhiz_dropna_from_db_for_rc_import.csv')
whiz.to_csv('asdwhiz_from_db_for_rc_import.csv')


# ####math 8 hweek

# In[44]:
math8week = scsnl_data_df[(
    scsnl_data_df['subj_unique_identifier'].isin(study_id['math8week']))]
math8week.reset_index(level=['record_id'])
math8week.dropna(how='all', axis=1).to_csv(
    'math8week_dropna_from_db_for_rc_import.csv')
math8week.to_csv('math8week_from_db_for_rc_import.csv')


# #### ADHD

# In[45]:
adhd = scsnl_data_df[(
    scsnl_data_df['subj_unique_identifier'].isin(study_id['adhd']))]
adhd.reset_index(level=['record_id'])
adhd.dropna(how='all', axis=1).to_csv('adhd_dropna_from_db_for_rc_import.csv')
adhd.to_csv('adhd_from_db_for_rc_import.csv')


# In[113]:
# study_fsid_list=scsnl['studies_formsets'][scsnl['studies_formsets']['study_id'] == 26.2]['fsid'].values
# study_ffid_list=scsnl['studies_formfields'][scsnl['studies_formfields']['study_id'] == 26.2]['ffid'].values
# formfield_list=scsnl['formfields']
# formset_list=scsnl['formsets']
# study_formfields_1=scsnl['formfields'][scsnl['formfields']['fsid'].isin(study_fsid_list)]
# study_formfields_2=scsnl['formfields'][scsnl['formfields']['ffid'].isin(study_ffid_list)]
# study_formfields=pd.concat([study_formfields_1,study_formfields_2])
# subjlist=pd.read_csv('/Users/cdla/Desktop/notebooks/menon_lab/2019-11-14_mathfun_org/mathfun_files/subj_list_jk.txt')
# scsnl_data_pid_visit=scsnl_data.set_index(['pid','visit'])

# # In[ ]:
#
#
# asd_met=pd.read_excel('ASD_MET_Enrollment_PID.xlsx',dtype=str)
# met_whiz=pd.read_csv('subjectlist.np.bx.csv')
#
# subj_list=pd.concat(met_whiz.loc[:,['PID','Visit']])
# subj_list=np.concatenate(asd_met.PID.to_list(),met_whiz.PID.to_list())
# print(asd_met.head(1))
# print(met_whiz.head(1))
#
