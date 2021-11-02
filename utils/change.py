from datetime import datetime
import itertools
import json
import os
from tqdm import tqdm

from utils.http import http


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


def _normalize_petitions(petitions):
    '''
    Flattens petition list from {id, petition...} to petition only
    :param petitions:
    :return:
    '''
    updated = []
    for petition in petitions:
        updated.append(petition['petition'])

    return updated


def get_petitions_by_keyword(keyword, lang='it-IT'):
    pkl_path = os.path.join(os.getcwd(), 'json', 'keywords', lang, f"{keyword}.json")
    os.makedirs(os.path.dirname(pkl_path), exist_ok=True)
    if os.path.isfile(pkl_path) and os.path.getsize(pkl_path):
        with open(pkl_path, 'r') as pkl:
            # print('Got from cache')
            data = json.load(pkl)
            data['items'] = _normalize_petitions(data['items'])
            return data
    with open(pkl_path, 'w') as pkl:
        res = get_petitions_from(f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')
        json.dump(res, pkl)

        res['items'] = _normalize_petitions(res['items'])

        return res


def get_petitions_by_tag(tag):
    pkl_path = os.path.join('json', 'tags', f"{tag}.json")
    os.makedirs(os.path.dirname(pkl_path), exist_ok=True)
    if os.path.isfile(pkl_path) and os.path.getsize(pkl_path):
        with open(pkl_path, 'r') as pkl:
            # print('Got from cache')
            data = json.load(pkl)
            data['items'] = _normalize_petitions(data['items'])
            return data
    with open(pkl_path, 'w') as pkl:
        res = get_petitions_from(f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')

        if 'err' in res:
            print(res)
            raise Exception(f"Sorry, {res['err']} for tag {tag} to url https://www.change.org/api-proxy/-/tags/{tag}/petitions?")

        print(f"Looking for {tag},\t found {res['total_count']}")

        json.dump(res, pkl)
        print("Saved in cache.")
        res['items'] = _normalize_petitions(res['items'])

        return res


def get_related_tags(tag):
    pkl_path = os.path.join('json', 'related_tags', f"{tag}.json")

    os.makedirs(os.path.dirname(pkl_path), exist_ok=True)
    if os.path.isfile(pkl_path) and os.path.getsize(pkl_path):
        with open(pkl_path, 'r') as pkl:
            # print('Got from cache')
            return json.load(pkl)
    with open(pkl_path, 'w') as pkl:
        res = http.get(f"https://www.change.org/api-proxy/-/tags/{tag}/related_tags?limit=999").json()['items']
        json.dump(res, pkl)
        print("Saved in cache.")
        return res


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
    return itertools.groupby(_parse_date(petitions),
                             key=lambda p: f"{p['published_at'].year}-{p['published_at'].month}")


def _parse_date(petitions, prop='published_at'):
    l = []
    for pet in petitions:
        pet[prop] = datetime.strptime(pet[prop], '%Y-%m-%dT%H:%M:%SZ')
        l.append(pet)

    return l


def count_tags(petitions, **kwargs):
    found_tags = {}

    for petition in petitions:
        for tag in petition['tags']:
            key = tag['name']
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
