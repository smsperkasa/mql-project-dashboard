"""This is a Python script to generate data for the dashboard"""
import datetime
import pandas as pd
import phonenumbers
import psycopg2
import xmlrpc.client

ODOO_URL = "https://smsperkasa.odoo.com"
ODOO_DB = "smsperkasa-master-1574977"
ODOO_USERNAME = "admin"
ODOO_PASSWORD = "q82fD^YMXL246cUNr2Qp{a{TN"

df = None

def search_from_odoo(model, payload, fields, order=''):
    """Mixin function to search data from Odoo with Odoo's xmlrpc"""
    try:
        common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
        dbname = ODOO_DB
        user = ODOO_USERNAME
        pw = ODOO_PASSWORD

        uid = common.authenticate(dbname, user, pw, {})

        models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
        
        result = models.execute_kw(
            dbname,uid,pw,
            model,'search_read',[payload],
            {
                'fields': fields, 
                'context': {'lang': 'en_GB'}, 
                'order': order
            }
        )

        return result
    except Exception as err:
        return ["Error", err]

def convert_phonenumber(phonenumber):
    """Function to convert phone number to the right format"""
    try:
        if phonenumber[0] == '0':
            phonenumber = '+62' + phonenumber[1:]
        parsed_phone = phonenumbers.parse(phonenumber, 'ID')
        return phonenumbers.format_number(
            parsed_phone,
            phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
    except Exception as e:
        if phonenumber is None:
            return None
        return "error"

def reverse_param(feature_param):
    if feature_param == 'cleaned_email':
        return 'cleaned_phone'
    return 'cleaned_email'

def duplicated_label(feature_param, row, depth, label):
    if depth >= 4:
        return None
    found_dup = df[(df[feature_param] == row[feature_param])]
    if len(found_dup) > 1:
        for i, r in found_dup.loc[:, [reverse_param(feature_param)]].iterrows():
            df.loc[(df[reverse_param(feature_param)] == r[reverse_param(feature_param)]), 'group_label'] = label
            duplicated_label(reverse_param(feature_param), r, depth + 1, label)
        df.loc[(df[feature_param] == row[feature_param]), 'group_label'] = label
    return None

def extend_dataframe(param_df, param_list):
    start_date = pd.Timestamp(param_df['date'].max() + datetime.timedelta(days=1))
    end_date = pd.Timestamp('2023-12-31')
    date_range = pd.date_range(start_date, end_date, freq='D')

    extended_data = {'date': date_range}
    for param in param_list:
        extended_data[param] = [None] * len(date_range)
    extended_df = pd.DataFrame(extended_data)

    return pd.concat([param_df, extended_df])

def extend_series(param_series, start_date, end_date):
    complete_date_range = pd.date_range(start=start_date, end=end_date)

    # Convert the index of the Series to a datetime object
    param_series.index = pd.to_datetime(param_series.index)
    # Reindex the Series with the complete date range and fill missing values with 0
    return param_series.reindex(complete_date_range, fill_value=0)

conn = psycopg2.connect(
    host="167.71.206.23",
    database="chatwoot_production",
    user="postgres",
    password="s0f!a_s3xy?!",
)

cursor = conn.cursor()
query = """
SELECT * FROM contacts
WHERE
    (email IS NOT NULL AND email <> '') OR
    (phone_number IS NOT NULL AND phone_number <> '')
ORDER BY id
"""
cursor.execute(query)

records = cursor.fetchall()
cursor.close()

raw_cw_df = pd.DataFrame(records, columns=['id', 'name', 'email', 'phone_number', 'account_id', 'created_at', 'updated_at', 'additional_attributes', 'identifier', 'custom_attributes', 'last_activity_at'])
cw_df = raw_cw_df
cw_df.rename(columns={
    'created_at': 'create_date',
    'phone_number': 'phone',
    'email': 'cleaned_email'
}, inplace=True)
cw_df = cw_df.replace('', None)
cw_df['create_date'] = pd.to_datetime(cw_df['create_date'], errors='coerce')
cw_df['cleaned_phone'] = cw_df.apply(lambda x: convert_phonenumber(x.phone), axis=1)
cw_df = cw_df[cw_df.cleaned_phone != 'error']

odoo_contacts = search_from_odoo(
    'res.partner',
    [
        ['email', 'not like', 'smsperkasa'],
    ],
    ['id', 'name', 'email', 'phone', 'mobile', 'create_date']
)
raw_odoo_contacts_df = pd.DataFrame(odoo_contacts)

odoo_contacts_df = raw_odoo_contacts_df.replace('', None)
odoo_contacts_df = odoo_contacts_df.replace(False, None)
odoo_contacts_df['create_date'] = pd.to_datetime(odoo_contacts_df['create_date'], errors='coerce')

odoo_contacts_df[['phone1', 'phone2', 'phone3']] = odoo_contacts_df['phone'].str.split(r'[,/<>;]+', expand=True)
odoo_contacts_df = odoo_contacts_df.melt(id_vars=['id', 'name', 'email', 'phone', 'mobile', 'create_date'], value_vars=['phone1', 'phone2', 'phone3'], value_name='cleaned_phone')
odoo_contacts_df = odoo_contacts_df[~(((odoo_contacts_df.cleaned_phone.isna()) | (odoo_contacts_df.cleaned_phone.str.strip() == "")) & ((odoo_contacts_df.variable == 'phone2') | (odoo_contacts_df.variable == 'phone3')))]
odoo_contacts_df.drop(columns='variable', inplace=True)

tmp_contact_copy = odoo_contacts_df
work_phones = tmp_contact_copy[['id', 'name', 'email', 'phone', 'create_date', 'cleaned_phone']]
other_phones = (
    tmp_contact_copy[~tmp_contact_copy.mobile.isna()]['mobile'].str.split(',')
    .explode()
    .to_frame('cleaned_phone')
    
    # grab the correct name from the original DataFrame
    .join(tmp_contact_copy[['id', 'name', 'email', 'phone', 'create_date']])
)

tmp_combined_contact = pd.concat([work_phones, other_phones]).reset_index(level=0)
sorted_contacts_df = tmp_combined_contact.sort_values('create_date', ascending=False)
sorted_contacts_df = sorted_contacts_df.drop_duplicates(subset=['id', 'name', 'email', 'phone', 'create_date', 'cleaned_phone'], keep='last')
sorted_contacts_df = sorted_contacts_df[['id', 'name', 'email', 'phone', 'create_date', 'cleaned_phone']]

sorted_contacts_df[['email1', 'email2', 'email3']] = sorted_contacts_df['email'].str.split(r'[ ,/<>]+', expand=True)
sorted_contacts_df = sorted_contacts_df.melt(id_vars=['id', 'name', 'email', 'phone', 'create_date', 'cleaned_phone'], value_vars=['email1', 'email2', 'email3'], value_name='cleaned_email')
sorted_contacts_df = sorted_contacts_df[~(((sorted_contacts_df.cleaned_email.isna()) | (sorted_contacts_df.cleaned_email.str.strip() == "")) & ((sorted_contacts_df.variable == 'email2') | (sorted_contacts_df.variable == 'email3')))]
sorted_contacts_df.drop(columns='variable', inplace=True)

sorted_contacts_df['cleaned_phone'] = sorted_contacts_df.apply(lambda x: convert_phonenumber(x.cleaned_phone), axis=1)

odoo_leads = search_from_odoo(
    'crm.lead',
    [
        "&", "|", ["active", "=", True], ["active", "=", False], ["type", "=", "lead"]
    ],
    ['id', 'name', 'email_from', 'phone', 'create_date', 'type']
)
raw_odoo_leads_df = pd.DataFrame.from_dict(odoo_leads)
odoo_leads_df = raw_odoo_leads_df
odoo_leads_df.rename(columns={
    'email_from': 'cleaned_email'
}, inplace=True)
odoo_leads_df = odoo_leads_df.replace('', None)
odoo_leads_df = odoo_leads_df.replace(False, None)
odoo_leads_df['create_date'] = pd.to_datetime(odoo_leads_df['create_date'], errors='coerce')
odoo_leads_df['cleaned_phone'] = odoo_leads_df.apply(lambda x: convert_phonenumber(x.phone), axis=1)

selected_cw_df = cw_df[['id', 'name', 'cleaned_email', 'cleaned_phone', 'create_date']]
selected_odoo_contacts_df = sorted_contacts_df[['id', 'name', 'cleaned_email', 'cleaned_phone', 'create_date']]
selected_odoo_leads_df = odoo_leads_df[['id', 'name', 'cleaned_email', 'cleaned_phone', 'create_date']]

joined_contacts = pd.concat([selected_cw_df, selected_odoo_contacts_df, selected_odoo_leads_df])
joined_contacts = joined_contacts[(~joined_contacts.cleaned_email.isna()) | (~joined_contacts.cleaned_phone.isna())]
joined_contacts['cleaned_email'] = joined_contacts['cleaned_email'].str.lower()
joined_contacts = joined_contacts.sort_values(by='create_date', ascending=False)
joined_contacts = joined_contacts.drop_duplicates(subset=['cleaned_email', 'cleaned_phone'], keep='last')

df = joined_contacts
df['group_label'] = None

for index, row in df.iterrows():
    found_dup = df[(df.cleaned_email == row.cleaned_email)]
    if len(found_dup) > 1:
        label = found_dup.reset_index().at[0, 'id']
        duplicated_label('cleaned_email', row, 1, label)

for index, row in df.iterrows():
    found_dup = df[(df.cleaned_phone == row.cleaned_phone)]
    if len(found_dup) > 1:
        label = found_dup.reset_index().at[0, 'id']
        duplicated_label('cleaned_phone', row, 1, label)

duplicated_df = df[~df.group_label.isna()]
non_duplicated_df = df[df.group_label.isna()]
filtered_duplicated_df = duplicated_df.drop_duplicates(subset='group_label', keep='last')

date_range_2023 = filtered_duplicated_df[(filtered_duplicated_df['create_date'] >= '2023-10-01') & (filtered_duplicated_df['create_date'] <= '2023-12-31')]
filtered_duplicated_mql_2023 = date_range_2023.groupby([date_range_2023['create_date'].dt.date.rename('date')]).agg({'count'})['create_date']

date_range_2023 = non_duplicated_df[(non_duplicated_df['create_date'] >= '2023-10-01') & (non_duplicated_df['create_date'] <= '2023-12-31')]
non_duplicated_mql_2023 = date_range_2023.groupby([date_range_2023['create_date'].dt.date.rename('date')]).agg({'count'})['create_date']

date_range_q3 = filtered_duplicated_df[(filtered_duplicated_df['create_date'] >= '2023-07-01') & (filtered_duplicated_df['create_date'] <= '2023-10-01')]
filtered_duplicated_mql_q3 = date_range_q3.groupby([date_range_q3['create_date'].dt.date.rename('date')]).agg({'count'})['create_date']

date_range_q3 = non_duplicated_df[(non_duplicated_df['create_date'] >= '2023-07-01') & (non_duplicated_df['create_date'] <= '2023-10-01')]
non_duplicated_mql_q3 = date_range_q3.groupby([date_range_q3['create_date'].dt.date.rename('date')]).agg({'count'})['create_date']

mql_target_total = 6000
days_total = 91
mql_daily_target = mql_target_total / days_total

daily_mql = non_duplicated_mql_2023.add(filtered_duplicated_mql_2023, fill_value=0)
daily_mql = daily_mql.reset_index()
daily_mql.rename(columns={
    'count': 'mql'
}, inplace=True)
daily_mql['agg_mql'] = daily_mql['mql'].cumsum()
daily_mql['daily_target'] = mql_daily_target
daily_mql['agg_daily_target'] = daily_mql['daily_target'].cumsum()
daily_mql['daily_percentage'] = daily_mql['agg_mql'] / daily_mql['agg_daily_target'] * 100
daily_mql['mql_target_total'] = mql_target_total
daily_mql['achivement_percentage'] = daily_mql['agg_mql'] / daily_mql['mql_target_total'] * 100

daily_mql = extend_dataframe(daily_mql, ['mql', 'agg_mql', 'daily_target', 'agg_daily_target', 'daily_percentage', 'mql_target_total', 'achivement_percentage'])
daily_mql['daily_target'] = mql_daily_target
daily_mql['agg_daily_target'] = daily_mql['daily_target'].cumsum()

daily_mql['date'] = pd.to_datetime(daily_mql['date'], errors='coerce').dt.date
daily_mql = daily_mql.sort_values(by='date')

daily_mql_q3 = non_duplicated_mql_q3.add(filtered_duplicated_mql_q3, fill_value=0)
daily_mql_q3 = daily_mql_q3.reset_index()
daily_mql_q3.rename(columns={
    'count': 'mql'
}, inplace=True)
daily_mql_q3['agg_mql'] = daily_mql_q3['mql'].cumsum()
agg_mql_q3 = daily_mql_q3.agg_mql.values.tolist()

daily_mql['agg_mql_q3'] = agg_mql_q3
daily_mql['agg_mql_qtd_percentage'] = ((daily_mql.agg_mql - daily_mql.agg_mql_q3) / daily_mql.agg_mql_q3) * 100
daily_mql = daily_mql[['date', 'mql', 'agg_mql', 'agg_mql_q3', 'agg_mql_qtd_percentage', 'daily_target', 'agg_daily_target', 'daily_percentage', 'mql_target_total', 'achivement_percentage']]

conn = psycopg2.connect(
    host="178.128.83.235",
    database="warehouse_db",
    user="postgres",
    password="s0f!a_s3xy?!",
)

cursor = conn.cursor()
query = """
SELECT uuid, date, unifiedpagepathscreen, engagedsessions, sessions, conversions, totalusers, engagementrate, userconversionrate, activeusers, userengagementduration, screenpageviews 
FROM g_analytics.pages_and_screens_____path_and_screen_class
WHERE
    unifiedpagepathscreen LIKE '/produk/%'
"""
cursor.execute(query)

g_anal_records = cursor.fetchall()
cursor.close()

raw_ga_df = pd.DataFrame(g_anal_records, columns=['uuid', 'date', 'unifiedpagepathscreen', 'engagedsessions', 'sessions', 'conversions', 'totalusers', 'engagementrate', 'userconversionrate', 'activeusers', 'userengagementduration', 'screenpageviews'])
raw_ga_df['date'] = pd.to_datetime(raw_ga_df['date'], format='%Y%m%d')

ga_df = raw_ga_df[(raw_ga_df['date'] >= '2023-10-01') & (raw_ga_df['date'] <= '2023-12-31')]
ga_q3_df = raw_ga_df[(raw_ga_df['date'] >= '2023-07-01') & (raw_ga_df['date'] < '2023-10-01')]

engaged_sessions = ga_df.groupby('date')['engagedsessions'].sum()
sessions = ga_df.groupby('date')['sessions'].sum()

conversions = ga_df.groupby('date')['conversions'].sum()
totalusers = ga_df.groupby('date')['totalusers'].sum()


engaged_sessions_q3 = ga_q3_df.groupby('date')['engagedsessions'].sum()
sessions_q3 = ga_q3_df.groupby('date')['sessions'].sum()

conversions_q3 = ga_q3_df.groupby('date')['conversions'].sum()
totalusers_q3 = ga_q3_df.groupby('date')['totalusers'].sum()

start_date = datetime.date(2023, 7, 1)
end_date = datetime.date(2023, 9, 30)

engaged_sessions_q3 = extend_series(engaged_sessions_q3, start_date, end_date)
sessions_q3 = extend_series(sessions_q3, start_date, end_date)
conversions_q3 = extend_series(conversions_q3, start_date, end_date)
totalusers_q3 = extend_series(totalusers_q3, start_date, end_date)

daily_engagement_rate = engaged_sessions.divide(sessions, fill_value=0)
daily_conversion_rate = conversions.cumsum().divide(totalusers.cumsum(), fill_value=0)

daily_engagement_rate = daily_engagement_rate.reset_index(name='engagement_rate')
daily_conversion_rate = daily_conversion_rate.reset_index(name='conversion_rate')

daily_engagement_rate['engagement_rate'] = daily_engagement_rate['engagement_rate'] * 100
daily_conversion_rate['conversion_rate'] = daily_conversion_rate['conversion_rate'] * 100

daily_engagement_rate = extend_dataframe(daily_engagement_rate, ['engagement_rate', 'target'])
daily_conversion_rate = extend_dataframe(daily_conversion_rate, ['conversion_rate', 'target'])

daily_engagement_rate['initial'] = 55.47
daily_conversion_rate['initial'] = 1.18

daily_engagement_rate['target'] = 61.01
daily_conversion_rate['target'] = 1.298

daily_engagement_rate_q3 = engaged_sessions_q3.divide(sessions_q3, fill_value=0)
daily_conversion_rate_q3 = conversions_q3.cumsum().divide(totalusers_q3.cumsum(), fill_value=0)

daily_engagement_rate_q3 = daily_engagement_rate_q3.reset_index(name='engagement_rate_q3')
daily_conversion_rate_q3 = daily_conversion_rate_q3.reset_index(name='conversion_rate_q3')

daily_engagement_rate['engagement_rate_q3'] = daily_engagement_rate_q3.engagement_rate_q3.values.tolist()
daily_conversion_rate['conversion_rate_q3'] = daily_conversion_rate_q3.conversion_rate_q3.values.tolist()

daily_engagement_rate['engagement_rate_q3'] = daily_engagement_rate['engagement_rate_q3'] * 100
daily_conversion_rate['conversion_rate_q3'] = daily_conversion_rate['conversion_rate_q3'] * 100

daily_engagement_rate['engagement_rate_qtd_percentage'] = ((daily_engagement_rate.engagement_rate - daily_engagement_rate.engagement_rate_q3) / daily_engagement_rate.engagement_rate_q3) * 100
daily_conversion_rate['conversion_rate_qtd_percentage'] = ((daily_conversion_rate.conversion_rate - daily_conversion_rate.conversion_rate_q3) / daily_conversion_rate.conversion_rate_q3) * 100

daily_engagement_rate['daily_percentage'] = ((daily_engagement_rate.engagement_rate - daily_engagement_rate.initial) / (daily_engagement_rate.target - daily_engagement_rate.initial)) * 100
daily_conversion_rate['daily_percentage'] = ((daily_conversion_rate.conversion_rate - daily_conversion_rate.initial) / (daily_conversion_rate.target - daily_conversion_rate.initial)) * 100

daily_mql.to_csv('/project_dashboard/data/daily_mql.csv', index=False)
daily_engagement_rate.to_csv('/project_dashboard/data/daily_engagement_rate.csv', index=False)
daily_conversion_rate.to_csv('/project_dashboard/data/daily_conversion_rate.csv', index=False)
