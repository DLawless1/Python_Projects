import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
pd.set_option('display.max_columns',25)
pd.set_option('display.max_rows',10000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None
import pandas_gbq
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
import warnings
warnings.filterwarnings("ignore")
import time
start_time=time.time()


# filtered_coll = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\filtered_coll.xlsx")
# filtered_prod = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\filtered_prod.xlsx")
filtered_coll = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\stlpedscollections.xlsx")
filtered_prod = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\stlpedsproduction.xlsx")
filtered_prod.sort_values(by=['patient_id','DOS'], ascending=True)
filtered_coll.sort_values(by=['patient_id','ledger_date'], ascending=True)


# filtered_coll = filtered_coll[filtered_coll['patient_id']=='10005633c38bda8d34c001ac5a414']
# filtered_prod = filtered_prod[filtered_prod['patient_id']=='10005633c38bda8d34c001ac5a414']

#filtered_prod = filtered_prod[(filtered_prod['patient_id']==8459) | (filtered_prod['patient_id']==29808)]
#filtered_coll = filtered_coll[(filtered_coll['patient_id']==8459) | (filtered_coll['patient_id']==29808)]
#
#
#
#### Production side manipulation #####
filtered_prod['total_fee'] = filtered_prod['total_fee'].round(0)
filtered_prod = filtered_prod.groupby(["clinic_id", "provider_id", "patient_id", "DOS"]).agg({'total_fee':'sum'})
filtered_prod = filtered_prod.reset_index(level=0)
filtered_prod = filtered_prod.reset_index(level=0)
filtered_prod = filtered_prod.reset_index(level=0)
filtered_prod = filtered_prod.reset_index(level=0)
filtered_prod['Type'] = 'Production'
filtered_prod = filtered_prod.rename(columns={"DOS": "date", "total_fee": "amount"})
filtered_prod['amount'] = filtered_prod['amount'].astype(int)
filtered_prod = filtered_prod[filtered_prod['amount'] != 0]
filtered_prod = filtered_prod.sort_values(by=['clinic_id', 'patient_id', 'date'])
filtered_prod = filtered_prod.reset_index(drop=True)

# # ########################################

filtered_coll = filtered_coll[(filtered_coll['category']=='Collection') | (filtered_coll['category']=='Adjustment')]
filtered_coll['collection'] = filtered_coll['credit'] + filtered_coll['debit']
#filtered_coll = filtered_coll.groupby(["clinic_id", "patient_id", "ledger_date"]).agg({'collection':'sum'}) I don't think I am supposed to sum it
filtered_coll = filtered_coll[["clinic_id", "patient_id", "ledger_date",  "collection"]] #"category",
# filtered_coll = filtered_coll.reset_index(level=0)
# filtered_coll = filtered_coll.reset_index(level=0)
# filtered_coll = filtered_coll.reset_index(level=0)
filtered_coll = filtered_coll.rename(columns={"collection": "amount", "ledger_date":"date"}) #, "category":"Type"
filtered_coll['amount'] = filtered_coll['amount'].round(0)
filtered_coll['amount'] = filtered_coll['amount'].astype(int)
filtered_coll['Type'] = 'Collection'
filtered_coll = filtered_coll[filtered_coll['amount'] > 0]
filtered_coll = filtered_coll.sort_values(by=['clinic_id', 'patient_id', 'date'])
filtered_coll= filtered_coll.reset_index(drop=True)



combined = pd.concat([filtered_prod, filtered_coll])


# # ###############################################################################################################################




combined.reset_index(inplace=False)


final_df = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\merged.xlsx") #integral part of the flow

list_of_ids = combined['patient_id'].to_list()
distinct_pats = []
for i in list_of_ids:
    if i not in distinct_pats:
        distinct_pats.append(i)

ticker = 1

for i in distinct_pats:
    ##### Sets up the combind production and stuff right before it goes into the loop######
    combined_copy = combined[combined['patient_id'] == i]
    combined_copy = combined_copy.sort_values(by=['clinic_id', 'patient_id', 'date'])

    combined_copy = combined_copy.reset_index(drop=True) #inplace=True

    combined_copy = combined_copy.reset_index()

    combined_copy['index'] = combined_copy['index']+1
    combined_prod = combined_copy[combined_copy['Type']=='Production']
    combined_coll = combined_copy[combined_copy['Type'] == 'Collection']


    #print(combined_prod.dtypes)

    #### append production ########
    appended_prod = combined_prod
    vals = combined_prod['index'].to_list()

    for x in vals:
        trial = combined_prod[combined_prod['index']==x]
        appended_prod = appended_prod.append([trial]*(trial['amount'].values[0]-1))
    appended_prod = appended_prod.sort_values(by=['index'])
    appended_prod = appended_prod.reset_index(drop=True) #inplace=True
    appended_prod = appended_prod.reset_index()
    appended_prod['level_0'] = appended_prod['level_0']+1
    appended_prod = appended_prod.rename(columns={"level_0": "record_id_specific"})

    ##############################


############ append collection ###########
    appended_coll = combined_coll
    vals = combined_coll['index'].to_list()
    for x in vals:
        trial = combined_coll[combined_coll['index']==x]
        appended_coll = appended_coll.append([trial]*(trial['amount'].values[0]-1))
    appended_coll = appended_coll.sort_values(by=['index'])
    appended_coll = appended_coll.reset_index(drop=True) #inplace=True
    appended_coll = appended_coll.reset_index()
    appended_coll['level_0'] = appended_coll['level_0']+1
    appended_coll = appended_coll.rename(columns={"level_0": "record_id_specific"})
    merged = pd.merge(appended_prod,appended_coll, how='outer',  on=['patient_id', 'record_id_specific'])
    #merged = merged.groupby(["index_x", "clinic_id_x", "provider_id_x", "patient_id", "Type_x"])#.agg({'total_fee':'sum'})
    merged = merged[["index_x", "clinic_id_x", "provider_id_x", "patient_id", "Type_x", "date_x", "amount_x", "index_y", "date_y", "Type_y", "amount_y"]].drop_duplicates()
    merged = merged.sort_values(by=['index_x', "index_y"])
    ####################

    to_union = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Desktop\DeleteLater\Running_Balance_Union.xlsx")
    merged['amount_x_charge_amt'] = merged['amount_x']
    merged['amount_y_payment_amt'] = merged['amount_y']
    merged['amount_x'] = merged['amount_x'].fillna(0)
    merged['amount_y'] = merged['amount_y'].fillna(0)
    merged = merged.reset_index()

    for i in merged.index:
        if i == 0:
            temp = merged.loc[[i]]
            temp["difference"] = int(temp['amount_x'] - temp['amount_y'])
            to_union = to_union.append(temp)
        else:
            run_bal = to_union.loc[[i - 1]]['difference'].values[0]
            prod_index_curr = merged.loc[[i]]['index_x'].values[0]
            prod_index_prev = merged.loc[[i - 1]]['index_x'].values[0]
            coll_index_curr = merged.loc[[i]]['index_y'].values[0]
            coll_index_prev = merged.loc[[i - 1]]['index_y'].values[0]
            if run_bal >= 0:  ###### elif then this statement
                if prod_index_prev == prod_index_curr:
                    temp = merged.loc[[i]]
                    temp['amount_x'] = run_bal
                    temp["difference"] = int(temp['amount_x'] - temp['amount_y'])
                    to_union = to_union.append(temp)
                elif prod_index_curr != prod_index_prev:
                    temp = merged.loc[[i]]
                    temp['amount_x'] = run_bal + temp['amount_x']
                    temp["difference"] = int(temp['amount_x'] - temp['amount_y'])
                    to_union = to_union.append(temp)
            if run_bal < 0:
                if coll_index_curr == coll_index_prev:
                    temp = merged.loc[[i]]
                    temp['amount_y'] = abs(run_bal)
                    temp["difference"] = int(temp['amount_x'] - temp['amount_y'])
                    to_union = to_union.append(temp)
                elif coll_index_curr != coll_index_prev:
                    temp = merged.loc[[i]]
                    temp['amount_y'] = abs(run_bal) + temp['amount_y']
                    temp["difference"] = int(temp['amount_x'] - temp['amount_y'])
                    to_union = to_union.append(temp)
        to_union = to_union[["index_x", "clinic_id_x", "provider_id_x","patient_id","Type_x","date_x","amount_x_charge_amt", "amount_x","index_y","date_y","amount_y_payment_amt","amount_y","Type_y", "difference"]]
    final_df = final_df.append(to_union)
