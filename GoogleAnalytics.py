########################################################



service_account = r'C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Data Team\Marketing Projects\ga_connector.json'
property_id = ['290799796', '256524627', '263262192', '264103215', '264992796', '267487360', '271561512', '271580890',
               '271923694', '273448090', '273956075', '275534325', '275876214', '275883343', '275885564', '275895837',
               '275905285', '275926380', '275932054', '275933448', '275941796', '275951993', '275956348', '276097917',
               '282558082', '283916215', '297954798', '298254124', '298342768', '298352718', '299943754', '300063764',
               '300120292', '300136838', '300138663', '300140983', '300143208', '300145554', '300146644', '300151804',
               '300153415', '300153510', '300162813', '300163416', '303839889', '304032998', '305301716', '313358457',
               '313373743', '315932010', '316115726', '316896274', '318007194', '318031277', '318741967', '318796381',
               '319002287', '319621433', '319882492', '319925154', '320153992', '321550569', '321563177', '324981059',
               '324997309', '325003342', '325006283', '325009003', '325032449', '325035267', '325043858']

for i in property_id:
    if i == '290799796':
        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='sessionDefaultChannelGrouping'),
                gp.Dimension(name='hostname'),
                gp.Dimension(name='date')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='activeUsers')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        df1 = gp.query(service_account, request)
        ##########################################################################
        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='hostname'),
                gp.Dimension(name='eventName'),
                gp.Dimension(name='date')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='activeUsers')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        df2 = gp.query(service_account, request)
    #################################################################################
    else:
        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='sessionDefaultChannelGrouping'),
                gp.Dimension(name='hostname'),
                gp.Dimension(name='date')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='activeUsers')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        temp1 = gp.query(service_account, request)

        df1 = pd.concat([df1, temp1])
        ##########################################################################
        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='hostname'),
                gp.Dimension(name='eventName'),
                gp.Dimension(name='date')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='activeUsers')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        temp2 = gp.query(service_account, request)

        df2 = pd.concat([df2, temp2])



df1 = df1.rename(columns={'sessionDefaultChannelGrouping': 'Dimension', 'hostname': "Domain", 'sessions': "Sessions", 'totalUsers': "TotalUsers", 'screenPageViews':"PageViews", 'newUsers': "NewUsers"})
df1 = df1[['Dimension', 'Domain', 'date', 'Sessions', 'TotalUsers', 'PageViews', 'NewUsers', 'activeUsers']]
df1['pos'] = 'Top'

df2 = df2.rename(columns={'hostname': 'Domain', 'eventName':'Dimension', 'sessions':'Sessions', 'totalUsers':'TotalUsers', 'screenPageViews':'PageViews', 'newUsers':'NewUsers'})
df2 = df2[['Dimension', 'Domain', 'date', 'Sessions', 'TotalUsers', 'PageViews', 'NewUsers', 'activeUsers']]
df2['pos'] = 'Bottom'
dfdone = pd.concat([df1,df2])

dfdone['date'] = pd.to_datetime(dfdone['date'])

locations = pd.read_excel(r'C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Data Team\Alteryx Flat File Inputs\For GA4 Workflow.xlsx', sheet_name='Data')

df_ga4_1 = dfdone.merge(locations, how='left', left_on='Domain', right_on='Domain')
print(df_ga4_1.shape)

# cursor.execute('''TRUNCATE TABLE dbo.ga4_marketing''')
# for index, row in df_ga4_1.iterrows():
#     cursor.execute('INSERT INTO dbo.ga4_marketing([Domain], [Dimension], [date], [Sessions], [PageViews], [NewUsers], [pos], [TotalUsers], [activeUsers],  [business], [specialties]) values (?,?,?,?,?,?,?,?,?,?,?)',
#                     row['Domain'],
#                     row['Dimension'],
#                     row['date'],
#                     row['Sessions'],
#                     row['PageViews'],
#                     row['NewUsers'],
#                     row['pos'],
#                     row['TotalUsers'],
#                     row['activeUsers'],
#                     row['business'],
#                     row['specialties']
#                    )
#     cursor.commit()






##################### GA4_2 ####################################
service_account = r'C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Data Team\Marketing Projects\ga_connector.json'
property_id = ['290799796', '256524627', '263262192', '264103215', '264992796', '267487360', '271561512', '271580890',
               '271923694', '273448090', '273956075', '275534325', '275876214', '275883343', '275885564', '275895837',
               '275905285', '275926380', '275932054', '275933448', '275941796', '275951993', '275956348', '276097917',
               '282558082', '283916215', '297954798', '298254124', '298342768', '298352718', '299943754', '300063764',
               '300120292', '300136838', '300138663', '300140983', '300143208', '300145554', '300146644', '300151804',
               '300153415', '300153510', '300162813', '300163416', '303839889', '304032998', '305301716', '313358457',
               '313373743', '315932010', '316115726', '316896274', '318007194', '318031277', '318741967', '318796381',
               '319002287', '319621433', '319882492', '319925154', '320153992', '321550569', '321563177', '324981059',
               '324997309', '325003342', '325006283', '325009003', '325032449', '325035267', '325043858']


for i in property_id:
    if i == '290799796':
        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='hostname'),
                gp.Dimension(name='eventName'),
                gp.Dimension(name='date'),
                gp.Dimension(name='city')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='eventcount')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        df2 = gp.query(service_account, request)
    #################################################################################
    else:

        request = gp.RunReportRequest(
            property=f'properties/{i}',
            dimensions=[
                gp.Dimension(name='hostname'),
                gp.Dimension(name='eventName'),
                gp.Dimension(name='date'),
                gp.Dimension(name='city')
            ],
            metrics=[
                gp.Metric(name='sessions'),
                gp.Metric(name='totalUsers'),
                gp.Metric(name='screenPageViews'),
                gp.Metric(name='newUsers'),
                gp.Metric(name='eventcount')
            ],
            date_ranges=[gp.DateRange(start_date='2021-01-01', end_date='2022-09-14')],
        )
        temp2 = gp.query(service_account, request)

        df2 = pd.concat([df2, temp2])

df2 = df2.rename(columns={'hostname': 'Domain', 'sessions':'Sessions', 'totalUsers':'Users', 'screenPageViews': 'PageViews', 'newUsers':'NewUsers', 'eventName':'Dimension'})
df2 = df2[['Domain', 'date', 'city', 'Sessions', 'Users', 'PageViews', 'NewUsers']]
df2['date'] = pd.to_datetime(df2['date'])
specialties = pd.read_excel(r"C:\Users\DylanLawless\OneDrive - Specialty Dental Brands\Data Team\Alteryx Flat File Inputs\Office Specialties.xlsx", sheet_name='Data')
df_ga4_2 = df2.merge(specialties, how='left', left_on='Domain', right_on='Domain')
print(df_ga4_2)
