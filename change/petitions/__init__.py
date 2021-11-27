import json
import os
from datetime import datetime

import pandas
from tqdm import tqdm

import utils.google_services
from utils import cleanhtml
from .. import tags

from change.tags import has_tag_been_normalized, normalize_tag
from utils.http import http
import utils.google_services
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

        if has_tag_been_normalized(tag['name']):
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


def get():
    global _all_petitions

    if not isinstance(_all_petitions, pandas.DataFrame):
        _all_petitions = pandas.read_json(
            os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'all_petitions.json'))

    return _all_petitions


def save_all_petitions(petitions: pandas.DataFrame):
    return petitions.to_json(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'all_petitions.json'))


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
    '''

    :param keyword:
    :param lang:
    :return: pandas.Dataframe
    '''
    if type(keyword) is list:
        pets = pandas.DataFrame()

        for k in tqdm(keyword, desc=f'Scraping keyword {keyword}'):
            p = get_petitions_by_keyword(k, lang)

            pets = pandas.concat([pets, p], ignore_index=True)

        return pets.drop_duplicates('id', ignore_index=True)

    if type(lang) is list:
        pets = pandas.DataFrame()

        for la in tqdm(lang, desc=f'Scraping keyword {keyword} for each lang'):
            p = get_petitions_by_keyword(keyword, la)
            p = p.loc[p['original_locale'] == la]

            pets = pandas.concat([pets, p], ignore_index=True)

        return pets.drop_duplicates('id', ignore_index=True)

    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'keywords', lang, f"{keyword}.json")

    return pandas.DataFrame(
        _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')[
            'items'])


def get_petitions_by_tag(tag: str):
    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'tags', f"{tag}.json")

    return pandas.DataFrame(
        _get_file_or_fetch(pkl_path, f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')['items'])


def filter(pets=get(), slugs=None, country=None, limit=None, sort_by='total_signature_count'):
    filtered_pets = pets

    if slugs:
        filtered_pets = pets.loc[pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

    if country is not None:
        filtered_pets = pets.loc[pets['country'] == 'US']

    if sort_by:
        filtered_pets = filtered_pets.sort_values(by=sort_by, ascending=False)

    if limit:
        filtered_pets = filtered_pets.head(limit)

    return filtered_pets


def get_promask_petitions(limit=None):
    return filter(slugs=tags.promask_slugs, country='US', sort_by='total_signature_count', limit=limit)


def get_noomask_petitions(limit=None):
    return filter(slugs=tags.nomask_slugs, country='US', sort_by='total_signature_count', limit=limit)


def flatten_petitions(
        data):
    '''
    Da un dataframe di petizioni, restituisce una list pronta per essere caricata su un excel/csv

    :param data:
    :return:
    '''
    petition_rows = []

    if not isinstance(data, pandas.DataFrame):
        data = pandas.DataFrame(data)

    for _index, petition in data.iterrows():

        # A quanto pare non era la questione del -1, se provi a lanciare la ricerca per keyword con "covid" su en-US
        # ne ha tipo 5-6 missing quindi penso sia sempre la questione del limit > remaining
        # troppi pochi neuroni a disposizione per risolvere ora. Anche perche honestly 5 su 67279 capita, amen, ciao.
        # P.S. ricercando per tag pare non succeda

        if 'missingPetition' in petition:
            print('ERROR: PETITION NOT FOUND :(')
            continue

        # TODO questo potenzialmente tutto in row?
        title = petition['title']
        slug = petition['slug']
        user = petition['user']
        creator_name = petition['creator_name']
        targets = petition['targeting_description']
        relevant_location = petition['relevant_location']
        date = petition['published_at'].strftime('%Y-%m-%d')
        description = cleanhtml(petition['description'])
        tags_ = petition['tags']
        is_victory = petition['is_victory']
        is_verified_victory = petition['is_verified_victory']
        sponsored = petition['sponsored_campaign']
        signatures = petition['total_signature_count']
        page_views = petition['total_page_views']
        share_count = petition['total_share_count']

        try:
            img_url = str(f"https:{petition['photo']['sizes']['large']['url']}")
        except TypeError:
            img_url = 'n/a'

        # Confeitor Dennis Ritchie Omnipotenti
        # et vobis, fratres
        # quia peccavi nimis,
        # for-loop, digitatione,
        # KeyError et ripetitione,
        # mea culpa, mea culpa,
        # mea maxima culpa,
        # Ideo precor beatam Ada Lovelace semper virginem
        # omnes coders et hackers
        # et vobis fratres
        # orare pro me ab Guido van Rossum Deum Nostrum

        row = {
            'date': date,
            'title': title,
            'id': petition['id'],
            'slug': slug,
            'link': f'https://www.change.org/p/{urllib.quote(slug)}',
            'signatures': signatures,
            'page_views': page_views,
            'country': relevant_location['country_code'],
            'tags': ', '.join(petition['tag_names']),
            'tag_raw_names': ', '.join(petition['tag_raw_names']),
            'tag_slugs': ', '.join(petition['tag_slugs']),
            'user_id': user['id'],
            'user': creator_name,
            'targets': targets,
            'description': description,
            'victory': is_victory,
            'verified_victory': is_verified_victory,
            # 'sponsored': sponsored,
            'share_count_total': share_count,
            'img_url': img_url,
        }

        if 'original_locale' in petition:
            row['original_locale'] = petition['original_locale']

        # TODO add share by platform
        # TODO pars secunda: controlla che le info siano effettivamente diverse fra json delle keyword e json dei tag
        # dovremmo star parlando di ['petitions']['activity'][feature da pescare]
        petition_rows.append(row)

    return petition_rows


def filter_only_for_chosen_countries(petitions, countries=('US', 'GB', 'IN', 'CA', 'IT')):
    return petitions.loc[petitions['country'].isin(countries)]


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
