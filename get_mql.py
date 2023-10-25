import calendar
import xmlrpc.client
import csv
import pandas as pd
import phonenumbers

ODOO_URL = "https://smsperkasa.odoo.com"
ODOO_DB = "smsperkasa-master-1574977"
ODOO_USERNAME = "admin"
ODOO_PASSWORD = "q82fD^YMXL246cUNr2Qp{a{TN"

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
    if pd.isna(phonenumber) == False:
        phone = "+"+str(phonenumber).replace(".0", "")
        res = ''
        try:
            res = phonenumbers.format_number(phonenumbers.parse(phone.replace("+69","+62"),"ID"), phonenumbers.PhoneNumberFormat.NATIONAL).replace('0', '+62 ',1)
        except:
            # print(phone)
            res = phonenumbers.format_number(phonenumbers.parse(phone.replace("+","+62"),"ID"), phonenumbers.PhoneNumberFormat.NATIONAL).replace('0', '+62 ',1)
        return res
    else:
        return None

if __name__ == '__main__':
    df_cw = pd.read_csv('data/chatwoot_contacts.csv')
    df_cw['created_at_date'] = pd.to_datetime(df_cw['created_at_date'], errors='coerce')
    df_cw.groupby([df_cw['created_at_date'].dt.year.rename('year'), df_cw['created_at_date'].dt.month.rename('month')]).agg({'count'})['created_at_date']
    cleaned_cw_df = df_cw[(~df_cw['email'].isna() | ~df_cw['phone_number'].isna()) & ~df_cw['email'].str.contains("smsperkasa", na=False)]

    cw_emails = cleaned_cw_df['email'].values
    cw_phones = cleaned_cw_df['phone_number'].values

    cw_emails = cw_emails[~pd.isna(cw_emails)]
    cw_phones = cw_phones[~pd.isna(cw_phones)]
    phones = ["+"+str(phone).replace(".0", "") for phone in cw_phones]
    correct_phones = []
    for phone in phones:
        # print(phone)
        try:
            correct_phones.append(
                phonenumbers.format_number(phonenumbers.parse(phone.replace("+69","+62"),"ID"), phonenumbers.PhoneNumberFormat.NATIONAL).replace('0', '+62 ',1)
            )
        except:
            # print(phone)
            correct_phones.append(
                phonenumbers.format_number(phonenumbers.parse(phone.replace("+","+62"),"ID"), phonenumbers.PhoneNumberFormat.NATIONAL).replace('0', '+62 ',1)
            )

    odoo_contacts = search_from_odoo(
        'res.partner',
        [
            # '|',
            ['email', 'not in', list(cw_emails)],
            ['phone', 'not in', list(correct_phones)],
            ['email', 'not like', 'smsperkasa'],
            # ['lifecycle_stage', 'in', ['leads', 'mql']]
        ],
        ['name', 'phone', 'email', 'create_date']
    )
    df_contacts = pd.DataFrame(odoo_contacts)
    odoo_emails = df_contacts[df_contacts.email != False]['email'].values
    odoo_phones = df_contacts[df_contacts.phone != False]['phone'].values

    results_leads = search_from_odoo(
        'crm.lead',
        [
            "&", "|", ["active", "=", True], ["active", "=", False], ["type", "=", "lead"]
        ],
        ['name', 'phone', 'email_from', 'create_date', 'type']
    )
    df_leads = pd.DataFrame.from_dict(results_leads)

    df_leads = df_leads[(~df_leads.email_from.isin(cw_emails)) & (~df_leads.phone.isin(correct_phones)) & (~df_leads.email_from.isin(odoo_emails)) & (~df_leads.phone.isin(odoo_phones))]
    df_leads = df_leads[['name', 'email_from', 'phone', 'create_date']]
    df_leads.rename(columns={
        'email_from': 'email'
    }, inplace=True)

    cleaned_cw_df['phone'] = cleaned_cw_df.apply(lambda x: convert_phonenumber(x.phone_number), axis=1)

    cleaned_cw_df = cleaned_cw_df[['name', 'email', 'phone', 'created_at']]
    cleaned_cw_df.rename(columns={
        'created_at': 'create_date'
    }, inplace=True)

    mql_df = df_contacts[['name', 'email', 'phone', 'create_date']].append(df_leads, ignore_index = True)
    mql_df = mql_df.append(cleaned_cw_df, ignore_index = True)
    mql_df['create_date'] = pd.to_datetime(mql_df['create_date'], errors='coerce')
    mql_df = mql_df.sort_values(by=['create_date'])
    mql_df = mql_df.drop_duplicates(subset=['email', 'phone'])

    date_range_2023 = mql_df[(mql_df['create_date'] >= '2023-10-01') & (mql_df['create_date'] <= '2023-12-31')]
    date_groupby_mql_2023 = date_range_2023.groupby([date_range_2023['create_date'].dt.date.rename('month')]).agg({'count'})['create_date']

    date_range_2022 = mql_df[(mql_df['create_date'] >= '2022-01-01') & (mql_df['create_date'] <= '2022-12-31')]
    date_groupby_mql_2022 = date_range_2022.groupby([date_range_2022['create_date'].dt.month.rename('month')]).agg({'count'})['create_date']

    print(date_groupby_mql_2023)
