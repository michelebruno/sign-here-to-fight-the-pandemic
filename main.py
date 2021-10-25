import json
import os.path

from dotenv import load_dotenv

import requests

load_dotenv()

headers = {
    'Cookie': '_change_session=15196171a94c90b238a75969f6afb7ee; '
              '_change_lang=%7B%22locale%22%3A%22it-IT%22%2C%22countryCode%22%3A%22IT%22%7D; '
              '__cfruid=da7e15961cb1aaf81ca2c6db2c8eedb117acb6b3-1635174210 '
}


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
    d = get_petions_from(f'https://www.change.org/api-proxy/-/tags/{topic}/petitions?order=trending&limit=10')
    if 'err' in d:
        print(d)
        raise Exception(f"Sorry, {d['err']}")

    found = d['total_count']
    return get_petions_from(f'https://www.change.org/api-proxy/-/tags/{topic}/petitions?order=trending&limit={found}')


skip_already_downloaded = True


def download_images_from_petitions(data, folder_name='unnamed'):
    download_count = 0
    already_downloaded = 0
    no_pic_found = 0

    folder_path = f"{os.environ.get('ONEDRIVE_FOLDER_PATH')}/{folder_name}/"

    os.makedirs(os.path.dirname(folder_path), exist_ok=True)

    for each in data['items']:

        filename = f"{folder_path}{each['petition']['id']}-{each['petition']['slug']}.jpg"

        if skip_already_downloaded and os.path.isfile(filename):
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


if __name__ == '__main__':
    tag = 'coronavirus-it-it'
    petitions = get_petitions_by_tag(tag)
    download_images_from_petitions(petitions, 'tags/' + tag)
