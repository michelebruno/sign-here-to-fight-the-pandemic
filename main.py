import json
import os.path
from df2gspread import df2gspread as d2g

import pandas
from tqdm import tqdm
from google_services import get_creds, get_service
from dotenv import load_dotenv

import requests

# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

SKIP_ALREADY_DOWNLOADED = True

service = get_service()

headers = {
    'Cookie': '_change_session=15196171a94c90b238a75969f6afb7ee; '
              '_change_lang=%7B%22locale%22%3A%22it-IT%22%2C%22countryCode%22%3A%22IT%22%7D; '
              '__cfruid=da7e15961cb1aaf81ca2c6db2c8eedb117acb6b3-1635174210 '
}

PETITIONS_SPREADSHEET_ID = '11tssKfqx7xpcU-bZdSVEvqLOZpnCOqw7Xm9VbHn_50c'
CHANGE_RANGE_NAME = 'Change!A2:E'


def get_petions_from(url):
    """

    :param url: URL to fetch
    :return: Object of the fetched data
    :rtype object
    """
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'

    return json.loads(res.content)


def get_petitions_by_tag(topic):
    d = get_petions_from(f'https://www.change.org/api-proxy/-/tags/{topic}/petitions?&limit=5')
    if 'err' in d:
        print(d)
        raise Exception(f"Sorry, {d['err']}")

    found = d['total_count']
    return get_petions_from(f'https://www.change.org/api-proxy/-/tags/{topic}/petitions?&limit={found}')


def download_images_from_petitions(data, folder_name='unnamed'):
    download_count = 0
    already_downloaded = 0
    no_pic_found = 0

    folder_path = f"{os.environ.get('ONEDRIVE_FOLDER_PATH')}/{folder_name}/"

    os.makedirs(os.path.dirname(folder_path), exist_ok=True)

    for each in tqdm(data['items']):

        filename = f"{folder_path}{each['petition']['slug']}.jpg"

        if SKIP_ALREADY_DOWNLOADED and os.path.isfile(filename):
            # print(each['id'] + ' has already been downloaded')
            already_downloaded = already_downloaded + 1
            continue

        if type(each['petition']['photo']) is dict:

            img = requests.get(f"https:{each['petition']['photo']['sizes']['large']['url']}", headers=headers)

            with open(filename, 'wb') as f:
                download_count = download_count + 1
                f.write(img.content)
                # print(f"{each['id']} downloaded")
        else:
            print(f"{each['id']} has no pic")
            no_pic_found = no_pic_found + 1
            continue

    print(
        f"Petitions found {data['total_count']}. "
        f"Total images {already_downloaded + download_count}. "
        f"Downloaded {download_count}. "
        f"Already downloaded: {already_downloaded}. "
        f"With no images {no_pic_found} ")


def save_petitions_to_sheets(data, tab_name):
    df = []

    for each in data['items']:
        title = each['petition']['title']
        slug = each['petition']['slug']
        tags = each['petition']['tags']

        # TODO add image url, target, and so on...
        df.append({
            'title': title,
            'slug': slug,
            'link': f'https://www.change.org/p/{slug}',
            'tags': ', '.join([x['slug'] for x in tags])
        })

    df = pandas.DataFrame(df)

    service.spreadsheets().values().clear(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}").execute()
    # service.spreadsheets().values().append(spreadsheetId=PETITIONS_SPREADSHEET_ID, body=df.columns.values,
    #                                        range=tab_name)
    service.spreadsheets().values().append(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!A:A",
                                           body={"values": [df.columns.tolist()]},
                                           insertDataOption='INSERT_ROWS',
                                           valueInputOption="USER_ENTERED"
                                           ).execute()
    service.spreadsheets().values().append(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!A:A",
                                           body={"values": df.values.tolist()},
                                           insertDataOption='INSERT_ROWS',
                                           valueInputOption="USER_ENTERED"
                                           ).execute()


if __name__ == '__main__':
    tags = [
        'diritti-civili',
        'coronavirus-it-it'
    ]
    # tag = 'diritti-civili'
    for tag in tags:
        petitions = get_petitions_by_tag(tag)
        if not len(petitions['items']):
            continue
        save_petitions_to_sheets(petitions, tag)
        download_images_from_petitions(petitions, 'tags/' + tag)
