from datetime import datetime
import itertools
import json
import os

import pandas
from dotenv import load_dotenv
from tqdm import tqdm

import utils.google_services
from utils.http import http

load_dotenv()

service = utils.google_services.get_service()


def _parse_petition(petition):
    '''
    Always executed on each petition found from change.org
    :param petition:
    :return:
    '''

    petition['published_at'] = datetime.strptime(petition['published_at'], '%Y-%m-%dT%H:%M:%SZ')

    petition['month'] = petition['published_at'].strftime('%Y-%m')

    petition['country'] = petition['relevant_location']['country_code']

    tag_names = []

    tags = []

    tag_slugs = []

    tag_raw_names = []

    for tag in petition['tags']:
        tag_raw_names.append(tag['name'])
        tag['name'] = tag['name'].lower()
        n = normalize_tag(tag['name'])
        tag_names.append(n)
        tags.append(tag)
        tag_slugs.append(tag['slug'])

    petition['tag_names'] = set(tag_names)
    petition['tag_raw_names'] = tag_raw_names
    petition['tag_slugs'] = tag_slugs
    petition['tags'] = tags

    return petition


_all_petitions = {}


def get_all_petitions():
    global _all_petitions

    if not isinstance(_all_petitions, pandas.DataFrame):
        _all_petitions = pandas.read_json(
            os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'all_petitions.json'))

    return _all_petitions


def save_all_petitions(petitions: pandas.DataFrame):
    return petitions.to_json(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'all_petitions.json'))


_normalized_tags = {}


def get_normalized_tags():
    global _normalized_tags

    if not _normalized_tags:
        normalized = service.spreadsheets().values().get(
            spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='tuttitag_dict!A2:D').execute()

        normal_rows = normalized.get('values', [])

        _normalized_tags = {}
        for row in normal_rows:
            _normalized_tags[row[0].lower()] = row[3]

    return _normalized_tags


def has_tag_been_normalized(tag):
    tags = get_normalized_tags()

    return tag in tags


def normalize_tag(tag):
    tags = get_normalized_tags()

    if tag in tags and tags[tag] and tags[tag] != '':
        return tags.get(tag)
    else:
        return tag


def slugs_from_normalized_tag(tag):
    slugs = []

    all_tags = service.spreadsheets().values().get(
        spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='tuttitag_dict!A2:D'
    ).execute().get('values', [])

    for slug, name, translation, clean in all_tags:
        if clean == tag:
            slugs.append(slug)

    return slugs


def _get_file_or_fetch(path: str, url: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.isfile(path) and os.path.getsize(path):
        with open(path, 'r') as pkl:
            # print('Got from cache')
            data = json.load(pkl)
            data['items'] = [_parse_petition(i['petition']) for i in data['items'] if
                             'missingPetition' not in i['petition']]
            return data
    with open(path, 'w') as pkl:
        res = get_petitions_from(url)
        json.dump(res, pkl)

        res['items'] = [_parse_petition(i['petition']) for i in res['items'] if 'missingPetition' not in i['petition']]
        return res


def get_petitions_from(url):
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

    with tqdm(total=total_count) as pbar:
        while remaining > 0:
            res = http.get(
                f"{url}&limit={limit if limit < remaining else remaining}&offset={total_count - remaining}").json()

            if 'err' in res:
                print(res)
                raise Exception(f"Sorry, {res['err']}")

            items = items + res['items']
            remaining = remaining - res['count']
            pbar.update(total_count - remaining)

    res['items'] = items
    res['count'] = len(items)

    return res


def get_petitions_by_keyword(keyword: str, lang: str = 'it-IT'):
    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'keywords', lang, f"{keyword}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')


def get_petitions_by_tag(tag: str):
    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'),'json', 'tags', f"{tag}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')


def get_related_tags(tag: str):
    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'related_tags', f"{tag}.json")

    return _get_file_or_fetch(pkl_path, f"https://www.change.org/api-proxy/-/tags/{tag}/related_tags?limit=999")


def filter_petitions_by_tag(petitions, tag):
    filtered = []
    for pet in petitions:
        for t in pet['tags']:
            if t['slug'] == tag['slug']:
                filtered.append(pet)
                break

    return filtered


def group_by_relevant_location(petitions):
    return itertools.groupby(petitions, key=lambda p: p['relevant_location']['country_code'])


def group_petitions_by_month(petitions):
    return itertools.groupby(petitions,
                             key=lambda p: p['published_at'].strftime('%Y-%m'))


def count_tags(petitions: pandas.DataFrame, **kwargs):
    if not isinstance(petitions, pandas.DataFrame):
        petitions = pandas.DataFrame(petitions)

    found_tags = {}

    for index, petition in petitions.iterrows():

        for tag in petition['tag_names']:

            if tag not in found_tags:
                found_tags[tag] = {
                    'total_count': 0,
                    'name': tag,
                    **kwargs,
                }

            found_tags[tag]['total_count'] = found_tags[tag]['total_count'] + 1

    df = pandas.DataFrame([i for k, i in found_tags.items()])

    return df


def count_not_normalized_tags(petitions: pandas.DataFrame, **kwargs):
    if not isinstance(petitions, pandas.DataFrame):
        petitions = pandas.DataFrame(petitions)

    found_tags = {}

    for index, petition in petitions.iterrows():

        for tag in petition['tags']:
            key = tag['slug']

            if key not in found_tags:
                found_tags[key] = {
                    'total_count': 0,
                    **kwargs,
                    **tag
                }

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    df = pandas.DataFrame([i for k, i in found_tags.items()])

    return df


def get_tags_through_keyword(keyword, lang='en-GB', country=None):
    pets = get_petitions_by_keyword(keyword, lang)['items']

    if country:
        pets = [i for i in pets if i['country'] == country]

    found_tags = {}

    for petition in pets:

        for tag in petition['tags']:
            key = tag['slug']

            if key not in found_tags:
                found_tags[key] = {
                    'total_count': 0,
                    **tag
                }

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    tags = pandas.DataFrame([i for k, i in found_tags.items()])

    return tags.to_dict('records')


SKIP_ALREADY_DOWNLOADED = True


def download_images_from_petitions(petitions: pandas.DataFrame, folder_name='unnamed'):
    download_count = 0
    already_downloaded = 0
    no_pic_found = 0

    folder_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), folder_name)

    os.makedirs(os.path.dirname(folder_path), exist_ok=True)
    os.makedirs(folder_path, exist_ok=True)

    print('Sto scaricando le immagini...')

    for _i, each in tqdm(petitions.iterrows(), total=petitions.shape[0], unit='images'):

        # Esiste almeno una petizione per cui "slug" non è un key valido nel JSON
        # Dio solo sa come

        if "slug" not in each:
            continue

        # ho aggiunto [:140] perché win ha un limite sulla lunghezza del nome del file e del percorso del file.
        # da documentazione dovrebbe essere 160, ma con win non si sa mai

        filename = os.path.join(folder_path, f"{each['slug'][:140]}.jpg", )

        # Controlla se l'immagine è già stata scaricata
        if SKIP_ALREADY_DOWNLOADED and os.path.isfile(filename):
            # print(each['id'] + ' has already been downloaded')
            already_downloaded = already_downloaded + 1
            continue

        # Prende l'url dell'immagine alla risoluzione più alta e la scarica
        if type(each['photo']) is dict:
            img = http.get(f"https:{each['photo']['sizes']['large']['url']}")

            with open(filename, 'wb') as f:
                download_count = download_count + 1
                f.write(img.content)
                # print(f"{each['id']} downloaded")
        else:
            # print(f"{each['id']} has no pic")
            no_pic_found = no_pic_found + 1
            continue

    print(
        f"Total images {already_downloaded + download_count}. "
        f"Downloaded {download_count}. "
        f"Already downloaded: {already_downloaded}. "
        f"With no images {no_pic_found} ")


def from_petitions_get_list_of_tags(petitions, filename='all_normalized_tags_in_petitions.csv'):
    tags = []
    for i, petition in petitions.iterrows():
        t = []

        for tag in petition['tag_names']:
            if has_tag_been_normalized(tag):
                t.append(tag)

        if len(t):
            tags.append(','.join(t))

    myPath = r"C:\Users\lucad\Desktop\scratch\taglist.csv"
    pandas.DataFrame(tags).to_csv(myPath, index=False)
