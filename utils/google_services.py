from __future__ import print_function
import os.path

import pandas
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive', ]


def get_creds():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def get_service():
    return build('sheets', 'v4', credentials=get_creds())


def save_list_to_sheets_tab(list, tab_name, columns=None):
    PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

    service = get_service()

    df = pandas.DataFrame(list)
    df.fillna('', inplace=True)
    # TODO dovrebbe riordinare le colonne ma non lo fa
    df.reindex(columns=columns, copy=False, fill_value='')

    # with open('./petition-example.json', 'w') as outfile:
    #     json.dump(data['items'][0], outfile, indent=4)

    service.spreadsheets().values().clear(spreadsheetId=PETITIONS_SPREADSHEET_ID,
                                          range=f"{tab_name}!A2:ZZZ").execute()

    service.spreadsheets().values().update(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!1:1",
                                           body={"values": [df.columns.tolist()]},
                                           valueInputOption="USER_ENTERED"
                                           ).execute()

    service.spreadsheets().values().update(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!A2:ZZZ",
                                           body={"values": df.values.tolist()},
                                           valueInputOption="USER_ENTERED"
                                           ).execute()
    print("Saved to sheets.")
