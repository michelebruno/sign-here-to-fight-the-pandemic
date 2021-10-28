import os

import pandas
from dotenv import load_dotenv

from utils.google_services import service
from utils.http import http

load_dotenv()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

def get_related_tags(tag):
    related_tags = http.get(f"https://www.change.org/api-proxy/-/tags/{tag}/related_tags?limit=999").json()['items']
    return related_tags


scraped_data = []


def store_related_tags(tags, query_term):
    global scraped_data
    for i in range(len(tags)):
        tag = tags[i]
        row = {
            'query_term': query_term,
            'index': i,
            **tag
        }

        row.pop('photo_id', None)

        scraped_data.append(row)


def save_to_sheets():
    df = pandas.DataFrame(scraped_data)

    tab_name = 'related_tags'

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


if __name__ == '__main__':
    tags = [
        'corona-virus-it-it'
    ]

    for tag in tags:
        store_related_tags(get_related_tags(tag), tag)

    save_to_sheets()
