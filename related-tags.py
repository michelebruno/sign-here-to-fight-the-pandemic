import os

from dotenv import load_dotenv

from utils.change import get_petitions_by_tag, get_related_tags, filter_petitions_by_tag
from utils.google_services import save_list_to_sheets_tab

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
            'total_count': len(filter_petitions_by_tag(petitions, tag))
        }

        row.pop('photo_id', None)

        scraped_data.append(row)

    print(f"Scraped {len(tags)} from {query_term}")


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
        'coronavirus-aid-en-us',
        'covid-19-en-us',
         'coronavirus-en-in'
     ]

    for tag in tags:
        store_related_tags(get_related_tags(tag), tag)

    save_list_to_sheets_tab(scraped_data, 'related_tags')
