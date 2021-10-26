import json
import os.path
from pprint import pprint

import pandas
from tqdm import tqdm
from google_services import get_creds, get_service
from dotenv import load_dotenv
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

SKIP_ALREADY_DOWNLOADED = True

service = get_service()

retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, )

http = requests.Session()

# http.headers.update(headers)

http.mount("https://", adapter)

PETITIONS_SPREADSHEET_ID = '11tssKfqx7xpcU-bZdSVEvqLOZpnCOqw7Xm9VbHn_50c'


def get_petions_from(url):
    """

    :param url: URL to fetch
    :return: Object of the fetched data
    :rtype object
    """

    res = http.get(url).json()

    if 'err' in res:
        print(res)
        raise Exception(f"Sorry, {res['err']}")

    total_count = res['total_count']

    limit = 500

    if total_count < limit:
        return http.get(f"{url}&limit={total_count}").json()

    items = []

    remaining = total_count - 1

    res = {}

    while remaining > 0:
        print("Remaining {0}".format(remaining))
        res = http.get(f"{url}&limit={limit if limit < remaining else remaining}&offset={total_count - remaining}").json()
        items = items + res['items']
        remaining = remaining - res['count']

    res['items'] = items
    res['count'] = len(items)

    return res


def get_petition_by_keyword(keyword, lang='it-IT'):
    return get_petions_from(f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')


def get_petitions_by_tag(tag):
    return get_petions_from(f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')


def download_images_from_petitions(data, folder_name='unnamed'):
    download_count = 0
    already_downloaded = 0
    no_pic_found = 0

    folder_path = f"{os.environ.get('ONEDRIVE_FOLDER_PATH')}/{folder_name}/"

    os.makedirs(os.path.dirname(folder_path), exist_ok=True)

    print('Scaricoooooo')

    for each in tqdm(data['items']):

        filename = f"{folder_path}{each['petition']['slug']}.jpg"

        if SKIP_ALREADY_DOWNLOADED and os.path.isfile(filename):
            # print(each['id'] + ' has already been downloaded')
            already_downloaded = already_downloaded + 1
            continue

        if type(each['petition']['photo']) is dict:
            img = http.get(f"https:{each['petition']['photo']['sizes']['large']['url']}")

            with open(filename, 'wb') as f:
                download_count = download_count + 1
                f.write(img.content)
                # print(f"{each['id']} downloaded")
        else:
            # print(f"{each['id']} has no pic")
            no_pic_found = no_pic_found + 1
            continue

    print(
        f"Petitions found {data['total_count']}. "
        f"Total images {already_downloaded + download_count}. "
        f"Downloaded {download_count}. "
        f"Already downloaded: {already_downloaded}. "
        f"With no images {no_pic_found} ")


sheet_cleared = False

stored_petitions = []


def store_petitions(
        data, key_term,
        tab_name='petitions',
        found_through='tag', ):
    global stored_petitions
    for each in data['items']:
        petition = each['petition']

        if 'missingPetition' in petition:
            print('Error missingPetition')
            continue
        title = petition['title']
        slug = petition['slug']
        user = petition['user']
        creator_name = petition['creator_name']
        targets = petition['targets']
        relevant_location = petition['relevant_location']

        tags_ = petition['tags']

        row = {
            'origin': found_through,
            'key_term': key_term,
            'country': relevant_location['country_code'],
            'id': petition['id'],
            'title': title,
            'slug': slug,
            'link': f'https://www.change.org/p/{quote(slug)}',
            'tags': ', '.join([x['slug'] for x in tags_]),
            'user_id': user['id'],
            'user': creator_name,
        }

        if 'original_locale' in petition:
            row['original_locale'] = petition['original_locale']

        # TODO add image url, target, and so on...
        stored_petitions.append(row)


def save_petitions_to_sheets(
        tab_name='petitions',
        **kwargs
):
    global sheet_cleared
    global stored_petitions

    df = pandas.DataFrame(stored_petitions)

    if not sheet_cleared:
        # with open('./petition-example.json', 'w') as outfile:
        #     json.dump(data['items'][0], outfile, indent=4)

        service.spreadsheets().values().clear(spreadsheetId=PETITIONS_SPREADSHEET_ID,
                                              range=f"{tab_name}!A2:ZZZ").execute()

        service.spreadsheets().values().update(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!1:1",
                                               body={"values": [df.columns.tolist()]},
                                               valueInputOption="USER_ENTERED"
                                               ).execute()
        sheet_cleared = True
        print("Sheets cleared")

    service.spreadsheets().values().update(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!A2:ZZZ",
                                           body={"values": df.values.tolist()},
                                           valueInputOption="USER_ENTERED"
                                           ).execute()


if __name__ == '__main__':
    tags = [
        # 'diritti-civili',
        'coronavirus-aid-en-us',
        'sanidad',
        'coronavirus-it-it',
        'coronavirus-es-419'
    ]

    keywords = [
        'covid',
    ]
    langs = [
        'it-IT',
        # 'en-US',
        # 'en-GB'
    ]

    for lang in langs:
        for keyword in keywords:
            print(f"\033[94mLooking for keywork {keyword} in lang {lang}\033[0m")
            petitions = get_petition_by_keyword(keyword, lang)
            if not len(petitions['items']):
                continue
            print(f"Found {len(petitions['items'])} in keyword {keyword}")
            store_petitions(petitions, key_term=keyword, found_through='keyword')
            #download_images_from_petitions(petitions, f"keywords/{lang}/{keyword}")

    for tag in tags:
        print(f"\033[94mLooking for tag {tag}\033[0m")
        petitions = get_petitions_by_tag(tag)
        if not len(petitions['items']):
            continue
        print(f"Found {len(petitions['items'])} in tag {tag}")
        store_petitions(petitions, key_term=tag)
        #download_images_from_petitions(petitions, 'tags/' + tag)

    save_petitions_to_sheets()
