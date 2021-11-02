from datetime import datetime
import itertools
import json
import os

from dotenv import load_dotenv
from tqdm import tqdm

import utils.google_services
from utils.http import http

load_dotenv()

service = utils.google_services.get_service()
normalized = service.spreadsheets().values().get(
    spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='normal_tags!A:C').execute()
normal_rows = normalized.get('values', [])

tags_to_normal = {}
for r in normal_rows:
    tags_to_normal[r[0].lower()] = r[1]


def normalize_tag(tag, report=False):
    if report and tag not in tags_to_normal:
        print(f"{tag}\tnot found")
    return tags_to_normal.get(tag, tag)


def _parse_petition(petition):
    petition['published_at'] = datetime.strptime(petition['published_at'], '%Y-%m-%dT%H:%M:%SZ')

    tags = []

    for tag in petition['tags']:
        tag['name'] = tag['name'].lower()
        tags.append(tag)

    petition['tags'] = tags

    return petition


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


def unique_petitions(petitions):
    grouped = itertools.groupby(petitions, key=lambda x: x['id'])

    l = []

    for k, g in grouped:
        l.append(next(g))

    return l


def _get_file_or_fetch(path, url):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.isfile(path) and os.path.getsize(path):
        with open(path, 'r') as pkl:
            # print('Got from cache')
            data = json.load(pkl)
            data['items'] = [_parse_petition(i['petition']) for i in data['items']]
            return data
    with open(path, 'w') as pkl:
        res = get_petitions_from(url)
        json.dump(res, pkl)

        res['items'] = [_parse_petition(i['petition']) for i in res['items']]
        return res


def get_petitions_by_keyword(keyword, lang='it-IT'):
    pkl_path = os.path.join(os.getcwd(), 'json', 'keywords', lang, f"{keyword}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')


def get_petitions_by_tag(tag):
    pkl_path = os.path.join('json', 'tags', f"{tag}.json")

    return _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')


def get_related_tags(tag):
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


def count_tags(petitions, **kwargs):
    found_tags = {}

    for petition in petitions:
        for tag in petition['tags']:
            key = normalize_tag(tag['name'])
            if key not in found_tags:
                found_tags[key] = {
                    'total_count': 0,
                    **tag
                }

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    return found_tags


def get_tags_through_keyword(keyword, lang='en-GB', country=None):
    pets = get_petitions_by_keyword(keyword, lang)['items']

    if country:
        pets = [i for i in pets if 'relevant_location' in i and i['relevant_location']['country_code'] == country]

    tags = count_tags(pets)

    return [tags[t] for t in tags]
