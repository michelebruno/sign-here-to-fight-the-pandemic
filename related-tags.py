import os

import pandas
from dotenv import load_dotenv

from utils.change import get_petitions_by_tag, get_related_tags, filter_petitions_by_tag
from utils.google_services import service

load_dotenv()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

scraped_data = []


def store_related_tags(tags, query_term):
    global scraped_data

    petitions = get_petitions_by_tag(query_term)['items']

    for i in range(len(tags)):
        tag = tags[i]

        row = {
            'query_term': query_term,
            'index': i,
            **tag,
            'total_count': len(filter_petitions_by_tag(petitions, tag) )
        }

        row.pop('photo_id', None)

        scraped_data.append(row)

    print(f"Scraped {len(tags)} from {query_term}")


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
    print("Saved to sheets.")


if __name__ == '__main__':
    tags = [
        'corona-virus-it-it',
        'coronavirus-de-de',
        'coronavirüs-tr-tr',
        'coronavirus-es-es',
        'коронавирус-ru-ru',
        # Added after first search
        'covid-19-tr-tr',
        'covid-19-es-es',
        'covid-19-fr-fr',

    ]

    for tag in tags:
        store_related_tags(get_related_tags(tag), tag)

    save_to_sheets()
