import re
from datetime import datetime
import itertools
import json
import os
from urllib.parse import quote

import pandas
from dotenv import load_dotenv
from tqdm import tqdm
from yaspin import yaspin

import utils.google_services
from utils.http import http

load_dotenv()

service = utils.google_services.get_service()

all_change_langs = [
    'de-DE',
    'en-AU',
    'en-CA',
    'en-GB',
    'en-IN',
    'en-US',
    'es-AR',
    'es-ES',
    'id-ID',
    'it-IT',
    'ja-JP',
    'pt-BR',
    'ru-RU',
    'th-TH',
    'tr-TR',
    'hi-IN',
    'es-419',
    'fr-FR'
]


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
            spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='chosen_tags!A2:G').execute()

        normal_rows = normalized.get('values', [])

        _normalized_tags = {}
        for row in normal_rows:
            if len(row) > 6:
                _normalized_tags[row[5].lower()] = row[6]

    return _normalized_tags


def has_tag_been_normalized(tag):
    tags = get_normalized_tags()

    return tag.lower() in tags and tags[tag.lower()] and tags[tag.lower()] != ''


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


def get_related_tags(tag: str):
    pkl_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'related_tags', f"{tag}.json")

    return _get_file_or_fetch(pkl_path, f"https://www.change.org/api-proxy/-/tags/{tag}/related_tags?limit=999")


def filter_petitions_by_slug_tag(petitions, tags):
    if type(tags) is not tuple:
        tags = tuple(tags)
    return petitions.loc[petitions['tag_slugs'].apply(lambda x: [t for t in x if t in tags])]


def filter_only_for_chosen_countries(petitions, countries=('US', 'GB', 'IN', 'CA', 'IT')):
    return petitions.loc[petitions['country'].isin(countries)]


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
                newtag = {
                    'total_count': 0,
                    **kwargs,
                    **tag,
                    'total_count': 0
                }
                found_tags[key] = newtag

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    df = pandas.DataFrame([i for k, i in found_tags.items()])

    return df


def get_tags_through_keyword(keyword, lang='en-GB', country=None):
    pets = get_petitions_by_keyword(keyword, lang)

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


def from_petitions_get_list_of_tags(petitions, normalized: bool = True, only_normalized: bool = True):
    '''

    :param normalized:
    :param petitions:
    :param filename:
    :return:
    '''
    tags = []

    for i, petition in petitions.iterrows():

        t = []

        if normalized:
            for tag in petition['tag_raw_names']:
                if has_tag_been_normalized(tag.lower()) or not only_normalized:
                    t.append(normalize_tag(tag.lower()))
        else:
            for tag in petition['tag_raw_names']:
                t.append(tag)

        for _t in set(t):
            tags.append((petition['id'], _t))

    return tags


CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext


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
            'sponsored': sponsored,
            'share_count_total': share_count,
            'img_url': img_url,
            'id': petition['id'],
            'slug': slug,
            'link': f'https://www.change.org/p/{quote(slug)}'
        }

        if 'original_locale' in petition:
            row['original_locale'] = petition['original_locale']

        # TODO add share by platform
        # TODO pars secunda: controlla che le info siano effettivamente diverse fra json delle keyword e json dei tag
        # dovremmo star parlando di ['petitions']['activity'][feature da pescare]
        petition_rows.append(row)

    return petition_rows


def get_json_path(*folders):
    return os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', *folders)


def get_petition_by_id(petitions, id):
    x = petitions.loc[petitions['id'] == id]
    x = x.to_dict('records')
    return x[0]


def get_petition_comments(petition_id):
    json_filename = get_json_path('comments', f"{petition_id}.json")

    if os.path.exists(json_filename) > 0:
        print("Got from chache.")
        return pandas.read_json(json_filename)

    is_last = False
    offset = 0
    limit = 10

    comments = []
    with yaspin(
            text=f"Looking for comments in petition {petition_id}") as spinner:
        while not is_last:
            # il while loop che aumenta di uno è mostruosamente lento obv ma altrimenti dobbiamo indovinare
            # quanti ne mancano. In alternativa incrementiamo di 100 ogni loop fino a risposta negativa,
            # poi incrementiamo di 1 fino a nuova risposta negativa

            base_url = r'https://www.change.org/api-proxy/-/comments'

            res = http.get(
                f"{base_url}?limit={limit}&offset={offset}&commentable_type=Event&commentable_id={petition_id}").json()

            offset += limit

            is_last = bool(res['last_page'])

            comments.extend(res['items'])
        spinner.ok(f"Scraped {len(comments)} comments in petition {petition_id}")

        comments_df = pandas.DataFrame(comments)

        comments_df.to_json(json_filename)
        return comments_df
