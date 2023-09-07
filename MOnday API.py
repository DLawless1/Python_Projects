import requests
import json
import pandas as pd
import openpyxl
import pyodbc
from six.moves import urllib
from sqlalchemy import create_engine
from datetime import datetime
import time
import schedule
pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 10000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.width', 1000)


def create_column_heads(headers, data):
    column_headers = []
    for i in headers:
        column_headers.append(i[0])
    df = pd.DataFrame.from_records(data, columns=column_headers)
    return df

def job():
### create pandas df #####
    df = pd.DataFrame()


    ####################################
    #### Building the list of boards ###
    ####################################

    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization": apiKey}
    trial = 'limit: 500'
    # odl query = '{ boards (limit:10) {name id workspace_ids} }' ids: [3819034167]
    query = f'''
    
    query {{
      boards({trial}) {{
        name
        id
        workspace_id
        columns {{
          title
          id
          type
        }}
      }}
    }}
    '''
    data = {'query': query}
    r = requests.post(url=apiUrl, json=data, headers=headers)  # make request
    output = r.json()['data']
    boardlist = []
    ticker = 0
    for i in output['boards']:
        if i['workspace_id'] == 2302567:
            boardlist.append(i['id'])
            ticker +=1
    #boardlist = boardlist[:5]











    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization": apiKey}


    ticker1 = 0
    for id in boardlist:
        ticker1 += 1
        print(ticker1, id)
        apiUrl = "https://api.monday.com/v2"
        headers = {"Authorization": apiKey}

        # odl query = '{ boards (limit:10) {name id workspace_ids} }'
        query = f'''
        query {{
          boards(ids: [{id}]) {{
            name
            id
            workspace_id
            columns {{
              title
              id
              type
            }}
    
            groups {{
                title
              id
            }}
    
            items {{
              name
              updated_at
              id
              group {{
                id
              }}
    
              column_values {{
                id
                value
                text
              }}
            }}
            updates (limit: 100) {{
                body
                      creator {{
                id
              }}
              assets {{
                id
              }}
              item_id
              replies {{
                id
              }}
              text_body
              updated_at
                id
            created_at
            }}
    
    
    
          }}
        }}
        '''

        data = {'query': query}

        r = requests.post(url=apiUrl, json=data, headers=headers)  # make request
        output = r.json()['data']

        ##start of the loops
        for i in output['boards']:
            if i['workspace_id'] == 2302567:

                #######################################################################

                #######################################################################
                #### this won't be iterated over, this will be a dim table###
                board_name = []
                workspace_id = []
                board_groups_name = []
                board_groups_id = []

                # Gets the Board's Name and ID
                board_name_raw = i['name']
                board_id_raw = i['id']

                #### can use this for a table to join the data will be at the top ###
                for z in i['groups']:
                    board_name.append(board_name_raw)
                    workspace_id.append(board_id_raw)
                    board_groups_name.append(z['title'])
                    board_groups_id.append(z['id'])
                    # print(board_name)
                    # print(workspace_id)
                    # print(board_groups_name)
                    # print(board_groups_id)
                    # print(len(board_name))
                    # print(len(workspace_id))
                    # print(len(board_groups_name))
                    # print(len(board_groups_id))

                    task_name = []
                    task_name_group = []
                    task_owner = []
                    task_status = []
                    update_date = []  # Reallyi last modified date
                    task_id = []

                for t in i[
                    "items"]:  # this is looping through each row   THIS IS WHERE THE ID FIELD WILL COMe from NEED TO Add task ID

                    task_name.append(t['name'])
                    task_name_group.append(t['group']['id'])  ###what you will have to join to eventually
                    update_date.append(t['updated_at'])
                    task_id.append(t['id'])

                    listofids = []
                    for a in t[
                        'column_values']:  # checks to see what the columns are and searches for the relevant ones I need
                        listofids.append(a['id'])

                    #########################################################################
                    ### This is pulling the assigned person to the task, won't always have one
                    #########################################################################
                    if 'people0' in listofids:

                        for k in t['column_values']:
                            if k['id'] == 'people0':
                                if k['text'] == '':
                                    task_owner.append('Unassigned')
                                else:
                                    task_owner.append(k['text'])
                    else:
                        task_owner.append('Unassigned')

                    ##########################################################################
                    ### This is pulling the status for the task. ie (Done, IN progress etc. )
                    #########################################################################
                    if 'status' in listofids:
                        for k in t['column_values']:
                            if k['id'] == 'status':
                                if k['text'] == '':
                                    task_status.append('Unassigned')
                                else:
                                    task_status.append(k['text'])
                    else:
                        task_status.append('Unassigned')
                        ## Pandas Dataframe Creation for Fact and Dimension Tables

                ##########################################################################
                ### Pandas
                #########################################################################
                data_fact = {
                    'task_name': task_name,
                    'task_id': task_id,
                    'task_name_group': task_name_group,
                    'task_owner': task_owner,
                    'task_status': task_status,
                    'last_modified_date': update_date

                }
                df_fact = pd.DataFrame(data_fact)

                data_dim = {'workspace_id': workspace_id,
                            'board_name': board_name,
                            'board_groups_id': board_groups_id,
                            'board_groups_name': board_groups_name
                            }
                df_dim = pd.DataFrame(data_dim)
                combined = pd.merge(df_dim, df_fact, how='inner', left_on='board_groups_id',
                                    right_on='task_name_group').drop(columns={'task_name_group'})
                df = pd.concat([df, combined])

    #df = df[df['last_modified_date'] != 'Unassigned']

    # df.loc[df['task_status'].isna()] = 'Unassigned'
    df = df[~df['board_name'].str.contains('Template') & ~df['board_name'].str.contains('Templates')]
    df['task_status'] = df['task_status'].str.title()
    df = df[~df['board_name'].str.startswith('Subitems')]
    df['board_name'] = df['board_name'].str.strip()
    df['location_tag'] = df['board_name'].str[-9:]
    df['location_tag'] = df['location_tag'].str.replace("-","").str.strip()
    df.to_excel(r"C:\Users\DylanLawless\OneDrive - Gen4 Dental Partners\Data Team\Delete Later\Project_board_data.xlsx",index=False)
    df['last_modified_date'] = pd.to_datetime(df['last_modified_date'])
    df['last_modified_date'] = pd.to_datetime(df['last_modified_date'].dt.strftime('%Y%m%d'))
    df['last_refresh_date'] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    df['folder_name'] = 'Practice Project Plans'
