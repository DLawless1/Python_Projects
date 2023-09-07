from __future__ import print_function
from pprint import pprint
from googleapiclient import discovery
import gspread
import pandas as pd
pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 10000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.width', 1000)
from datetime import datetime
import requests
import json
import pandas as pd
import openpyxl
import pyodbc
from six.moves import urllib
from sqlalchemy import create_engine

import time



def create_column_heads(headers, data):
    column_headers = []
    for i in headers:
        column_headers.append(i[0])
    df = pd.DataFrame.from_records(data, columns=column_headers)
    return df


######## Get Data from Azure #####
##################################
##################################
import sql_scripts
pa_df = sql_scripts.import_from_azure(sql_scripts.pa_data)
pa_df['year_val'] = pa_df['year_val'].astype(int)
pa_df['month_val'] = pa_df['month_val'].astype(int)

import datetime
now = datetime.datetime.now()
current_year = now.year
current_month = now.month

pa_df = pa_df[(pa_df['year_val'] == current_year)] # & (pa_df['month_val'] == current_month)
pa_df['FirstOfMonth'] = pd.to_datetime(pa_df['year_val'].astype(str) + '-' + pa_df['month_val'].astype(str) + '-01')

# Group by
pa_df = pa_df.groupby(['office_nm', 'FirstOfMonth']).agg({'Production': 'sum', 'Booked Prod Gross': 'sum', 'Prior Year Production': 'sum'}).reset_index()
pa_df = pa_df[pa_df['Booked Prod Gross'] > 0] #(pa_df['Production'] > 0)

# At this point, the dataset will have 5 columns of all data since 1/1/2023



# pa_df = pa_df[pa_df['office_nm']=='Century City Aesthetic Dentistry']



##################################
# Giving the offices their Location ID
##################################
# pa_data_location_id = pd.read_json(r'C:\Users\DylanLawless\OneDrive - Gen4 Dental Partners\Data Team\Normalization Process\PA_LocationID.json')
# pa_data_location_id.to_excel(r"C:\Users\DylanLawless\OneDrive - Gen4 Dental Partners\Data Team\Delete Later\palocationids.xlsx")
#The above files have been deprecated as of 5/3/2023 by Dylan and is to be modiefied in the table or alteryx
pa_data_location_id = sql_scripts.import_from_azure(sql_scripts.pa_ids)
outliers_needto_map = pd.merge(pa_df,pa_data_location_id, how='left', left_on='office_nm', right_on='office_nm')
outliers_needto_map = outliers_needto_map[outliers_needto_map['Production'] > 0]
outliers_needto_map = outliers_needto_map[outliers_needto_map['locationId'].isnull()]['office_nm'].to_list()
outliers_needto_map = list(set(outliers_needto_map))
print('Need to map: ', outliers_needto_map)
pa_df = pd.merge(pa_df,pa_data_location_id, how='inner', left_on='office_nm', right_on='office_nm')
pa_df = pa_df[pa_df['locationId']!= 'UTPEX0004'].reset_index(drop=True)



##################################
# BUDGET DATA
##################################
column_nums = []
for i in range(1,17):
    column_nums.append(i)
budget = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Gen4 Dental Partners\Data Team\Excel\Gen4 2023 Budget - v4.xlsx", skiprows=2, usecols=column_nums)

# budget = budget[(~budget['dimPracticeKey'].isna()) & (budget['Category']=='Production')]
budget = budget[(budget['Category'] == 'Production') & (~budget['Sage ID'].isna())]

budget = budget[['dimPracticeKey', 'Sage ID', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']]
new_cols = ['dimPracticeKey', 'Sage ID', '2023-01-01',  '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01',
            '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01']

budget.columns = new_cols

budget = pd.melt(budget, id_vars=['dimPracticeKey', 'Sage ID'], var_name='StartOfMonth', value_name='Budget_prod')


budget['StartOfMonth'] = pd.to_datetime(budget['StartOfMonth'])
budget['Budget_prod'] = budget['Budget_prod'].astype(float)
budget = budget[~budget['Budget_prod'].isna()]


##################################
# Joining PA and Budget Data
##################################
# If it does have a BUDGET
full_df = pd.merge(pa_df,budget, how='inner', left_on=['locationId','FirstOfMonth'], right_on=['Sage ID', 'StartOfMonth'])
full_df = full_df[['office_nm', 'FirstOfMonth', 'Production', 'Booked Prod Gross', 'Prior Year Production', 'locationId', 'Budget_prod']]
# print(full_df)
# print(full_df)
# If does NOT have a BUDGET
fivepercent = pd.merge(pa_df,budget, how='left', left_on=['locationId','FirstOfMonth'], right_on=['Sage ID', 'StartOfMonth'])
fivepercent = fivepercent[fivepercent['Sage ID'].isnull()]


if len(fivepercent['office_nm']) > 0:
    fivepercent['Budget_prod'] = fivepercent['Prior Year Production']*1.05
    fivepercent = fivepercent.drop(columns={'dimPracticeKey', 'Sage ID', 'StartOfMonth'})
    fivepercent['five_per'] = 1
full_df = pd.concat([full_df,fivepercent])
print(full_df)
full_df['five_per'] = full_df['five_per'].fillna(0)
full_df['Budget_prod'] = full_df['Budget_prod'].astype(float)




##################################################################################
#################  MANUAL OFFICES ################################################
##################################################################################
non_pa = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Gen4 Dental Partners\General\Operations Review\2022 and 2023 Non-PA Location Metrics.xlsx",sheet_name='Daily')

column_names = []
new_column_names = []
for i in non_pa.columns:
    column_names.append(i)
for z in column_names:
    if 'datetime' in str(type(z)):
        value_date = pd.to_datetime(z).strftime('%m/%d/%Y')
        new_column_names.append(value_date)
    else:
        new_column_names.append(z)
non_pa.columns = new_column_names
# print(non_pa)

### Have to exclude rather than include because office names are in the column too
non_pa = non_pa[ (non_pa["Practice and Metrics"] != 'Daily Dentist Production')
                  & (non_pa["Practice and Metrics"] != 'Daily Hygiene Production ')
                    & (non_pa["Practice and Metrics"] != 'Daily Net Collections')
                        & (non_pa["Practice and Metrics"] != 'Daily Adjustments to Production ')
                            & (non_pa["Practice and Metrics"] != 'Daily Dentist  Production ')
                                & (~non_pa["Practice and Metrics"].isna())
                  ]



#making it pivot
trial = pd.melt(non_pa, id_vars=['Practice and Metrics'], var_name='Dates', value_name='amount')
# print(non_pa)

#This gets the practice to another column at the end
trial.loc[(trial['Practice and Metrics'] != 'Daily Gross Production') &
                (trial['Practice and Metrics'] != 'Total Monthly Gross Booked Production'), 'practice_rough'] = trial['Practice and Metrics']
#This allows you to eventually filter out all the rows that are the office's name
trial.loc[(trial['Practice and Metrics'] != 'Daily Gross Production') &
                (trial['Practice and Metrics'] != 'Total Monthly Gross Booked Production') , 'to_filter'] = 0

#This fills all the rows below each practice with that practice's name
trial['practice'] = trial['practice_rough'].fillna(method='ffill')
trial['to_filter'] = trial['to_filter'].fillna(1)
#filters out rows with the practices name
trial = trial[trial['to_filter'] == 1]
trial = trial[~trial['Dates'].str.contains('Unnamed')].reset_index(drop=True)
trial['Dates'] = pd.to_datetime(trial['Dates'], format='%m/%d/%Y')



from datetime import datetime, timedelta
current_date = datetime.now()
days_to_subtract = 5
new_date = current_date - timedelta(days=days_to_subtract)

trial = trial[(~trial['amount'].isna()) & (trial['amount'] != 'Closed ')] #(trial['Dates'] >= new_date) &
trial = trial[['practice','Practice and Metrics','Dates','amount']]
trial['month_num'] = trial['Dates'].dt.month
trial['year_num'] = trial['Dates'].dt.year

# print(trial[trial['amount'] == ' '])
trial['amount'] = trial['amount'].astype(float)
trial['sage_id'] = trial['practice'].str.extract(r'\(([^()]+)\)')
trial['practice'] = trial['practice'].str.extract(r'^([^()]*)\s*\(')
trial['sage_id'] = trial['sage_id'].str.strip()
trial['practice'] = trial['practice'].str.strip()

max_date = trial.groupby(['sage_id','practice','Practice and Metrics'])['Dates'].max().reset_index()
print(max_date)
trial = pd.merge(trial,max_date,how='inner',left_on=['sage_id', 'practice', 'Practice and Metrics', 'Dates'],right_on=['sage_id', 'practice', 'Practice and Metrics', 'Dates'])
# print(trial[['practice']].drop_duplicates().reset_index(drop=True))
print(trial)
# Top which is the Scheduled Production
top = trial[trial['Practice and Metrics'] == 'Total Monthly Gross Booked Production']
top = top.groupby(['sage_id','practice', 'month_num','year_num'])['amount'].sum().reset_index()
top = top.rename(columns={'amount':'scheduled_production'})

# Bottom is the Gross Production
bottom = trial[trial['Practice and Metrics'] == 'Daily Gross Production']
bottom = bottom.groupby(['sage_id','practice', 'month_num','year_num'])['amount'].sum().reset_index()
bottom = bottom.rename(columns={'amount':'production'})

all_manual_offices = pd.merge(top,bottom,how='left',left_on=['sage_id', 'practice', 'month_num','year_num'],right_on=['sage_id', 'practice', 'month_num','year_num'])
all_manual_offices['FirstOfMonth'] = pd.to_datetime(all_manual_offices['year_num'].astype(str) + '-' + all_manual_offices['month_num'].astype(str) + '-01')
print(all_manual_offices)
# print(all_manual_offices.sort_values(by='production', ascending=False))

##################################
# Joining Manual offices to budget
##################################
# Joining Manual data and Budget Data
dropouts = pd.merge(all_manual_offices,budget, how='left', left_on=['sage_id','FirstOfMonth'], right_on=['Sage ID', 'StartOfMonth'])
dropouts = dropouts[dropouts['Sage ID'].isnull()]
dropouts = dropouts[['sage_id', 'practice', 'FirstOfMonth', 'production', 'scheduled_production']].rename(columns={
    'sage_id':'locationId', 'production':'Production', 'scheduled_production':'Booked Prod Gross', 'practice':'office_nm'})


with_budg = pd.merge(all_manual_offices,budget, how='inner', left_on=['sage_id','FirstOfMonth'], right_on=['Sage ID', 'StartOfMonth'])
with_budg = with_budg[['sage_id','practice', 'FirstOfMonth', 'production', 'scheduled_production', 'Budget_prod']].rename(columns={
    'sage_id':'locationId', 'production':'Production', 'scheduled_production':'Booked Prod Gross', 'practice':'office_nm'})

all_manual_offices = pd.concat([with_budg, dropouts])
all_manual_offices['PA'] = 0
full_df['PA'] = 1
current_date = datetime.now().strftime("%Y-%m-%d")



max_date.loc[max_date['Dates'] == current_date, 'current_date_flag'] = 1
max_date['current_date_flag'] = max_date['current_date_flag'].fillna(0)
max_date = max_date[max_date['current_date_flag'] == 0]
max_date = max_date[['practice', 'Dates']].drop_duplicates()
sql_scripts.write_to_azure(max_date, 'ManualOfficesToFillOut')




# all_manual_offices = pd.merge(all_manual_offices, max_date, how='inner', left_on=['sage_id', 'practice', 'month_num','year_num'],right_on=['sage_id', 'practice', 'month_num','year_num'])
# print(all_manual_offices)
#Use this for the flag
###df.loc[df['appointmentDate'] >= date_today, 'DrDays_Scheduled'] = 1

##################################
# Concats the manual and the PA offices
##################################
full_df = pd.concat([full_df, all_manual_offices])
full_df = full_df[['office_nm', 'FirstOfMonth', 'Production', 'Booked Prod Gross', 'locationId', 'Budget_prod', 'PA', 'five_per']]
full_df['five_per'] = full_df['five_per'].fillna(0)
full_df['Budget_prod'] = full_df['Budget_prod'].fillna(0)
# print(full_df)




##################################
#excludes old offices
##################################
old_offices = ['MISTB0007', 'UTPEX0002', 'AZSWD0005']    #[idk, Pex Alpine (5/3), Prescott (5/3)]
full_df = full_df[~full_df['locationId'].isin(old_offices)]
full_df = full_df.reset_index(drop=True)


##################################
#Most Important
##################################
# Combines offices per braden's request


# ACD Lawrence becmoing ACR Robbins
full_df.loc[full_df['locationId'] == 'KSACD0001', 'Budget_prod'] = 1
full_df.loc[full_df['locationId'] == 'KSACD0001', 'locationId'] = 'KSACR0001'
full_df.loc[full_df['office_nm'] == 'ACD Lawrence', 'office_nm'] = 'ACR Robbins'
full_df.loc[full_df['locationId'] == 'KSACR0001' , 'five_per'] = 0

#STB Cascade to Smile Cascade
full_df.loc[full_df['locationId'] == 'MISTB0002' , 'Budget_prod'] = 1
full_df.loc[full_df['locationId'] == 'MISTB0002' , 'PA'] = 0 #only for this specifically
full_df.loc[full_df['locationId'] == 'MISTB0002' , 'locationId'] = 'MISDP0002'
full_df.loc[full_df['office_nm'] == 'STB Cascade' , 'office_nm'] = 'Smile Cascade'

full_df = full_df.groupby(['office_nm', 'FirstOfMonth', 'locationId', 'PA', 'five_per']).agg({'Production':'sum', 'Booked Prod Gross': 'sum', 'Budget_prod':'sum'}).reset_index()
full_df = full_df.sort_values(by='FirstOfMonth', ascending=False).reset_index(drop=True)
full_df['date_string'] = full_df['FirstOfMonth'].dt.strftime('%Y%m%d')

#This is wagner
# full_df = full_df[full_df['locationId']!= 'NVWAG0001']


sql_scripts.write_to_azure(full_df,'Gen4_PA_and_Manual')



manualfilled = full_df[full_df['PA']==0]
sql_scripts.write_to_azure(manualfilled, 'ManualOfficesData')


###########################################
###########################################
#### Trend Table
from datetime import datetime
# get the current date and time
now = datetime.now()
rn = now.strftime('%Y-%m-%d')

# get the first day of the current month
first_day_of_month = datetime(now.year, now.month, 1)
first_day_of_month = first_day_of_month.strftime('%Y-%m-%d')



table = full_df[full_df['FirstOfMonth'] == first_day_of_month].reset_index(drop=True)
table['todays_date'] = rn


table['%_to_budget'] = table['Booked Prod Gross']/table['Budget_prod']
table['%_to_budget'] = table['%_to_budget'].round(3)
table.loc[table['Budget_prod'] == 0, '%_to_budget'] = 0

# table['todays_date'] = table['todays_date'].str.replace('-', '')  I GOT RId of this because the dtypes weren't right
table['todays_date'] = pd.to_datetime(table['todays_date'])
table['pk'] = table['locationId'] + rn.replace('-', '')



pk_query = sql_scripts.import_from_azure(sql_scripts.to_get_pk)
existing_pk_values = pk_query['pk'].values
df_to_insert = table[~table['pk'].isin(existing_pk_values)]
###setting up the loop
to_delete_rows = table[table['pk'].isin(existing_pk_values)]['pk'].values
