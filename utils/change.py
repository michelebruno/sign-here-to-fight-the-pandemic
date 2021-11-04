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

    for tag in petition['tags']:
        tag['name'] = tag['name'].lower()
        n = normalize_tag(tag['name'])
        tag_names.append(n)
        tag['name'] = n
        tags.append(tag)
    petition['tag_names'] = set(tag_names)
    petition['tags'] = tags

    return petition


_normalized_tags = {}


def get_all_petitions():
    return pandas.read_json(os.path.join('json', 'all_petitions.json'))


def save_all_petitions(petitions: pandas.DataFrame):
    return petitions.to_json(os.path.join('json', 'all_petitions.json'))


def get_normalized_tags():
    global _normalized_tags

    if _normalized_tags:
        return _normalized_tags

    normalized = service.spreadsheets().values().get(
        spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='normal_tags!A:C').execute()
    normal_rows = normalized.get('values', [])

    _normalized_tags = {}
    for r in normal_rows:
        _normalized_tags[r[0].lower()] = r[2]

    return _normalized_tags


def normalize_tag(tag):
    tags = get_normalized_tags()

    if tag in tags and tags[tag] and tags[tag] != '':
        return tags.get(tag)
    else:
        return tag


def get_tags_from_normalized(tag):
    tags = get_normalized_tags()

    result = []

    for t in tags:
        if tag == t:
            result.append(t)

    return set(result)


def tag_slugs_from_normalized(all_pets, found_tags):
    slugs = []
    for i, p in all_pets.iterrows():

        for t in p['tags']:
            if t['name'] in found_tags:
                slugs.append(t['slug'])

    return set(slugs)


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


def get_petitions_by_keyword(keyword: str, lang: str = 'it-IT'):
    pkl_path = os.path.join(os.getcwd(), 'json', 'keywords', lang, f"{keyword}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')


def get_petitions_by_tag(tag: str):
    pkl_path = os.path.join('json', 'tags', f"{tag}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')


def get_related_tags(tag: str):
    pkl_path = os.path.join('json', 'related_tags', f"{tag}.json")

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
