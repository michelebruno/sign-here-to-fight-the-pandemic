import json
import os.path
from utils.http import http

import pandas
from tqdm import tqdm
from utils.google_services import get_service
from dotenv import load_dotenv
from urllib.parse import quote

import re

CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext


# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

SKIP_ALREADY_DOWNLOADED = True

service = get_service()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')


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
    with tqdm(total=total_count) as pbar:
        while remaining > 0:
            res = http.get(
                f"{url}&limit={limit if limit < remaining else remaining}&offset={total_count - remaining}").json()
            items = items + res['items']
            remaining = remaining - res['count']
            pbar.update(total_count - remaining)

    res['items'] = items
    res['count'] = len(items)

    return res


def get_petition_by_keyword(keyword, lang='it-IT'):
    pkl_path = os.path.join('json', 'keywords', lang, f"{keyword}.json")
    os.makedirs(os.path.dirname(pkl_path), exist_ok=True)
    if os.path.isfile(pkl_path) and os.path.getsize(pkl_path):
        with open(pkl_path, 'r') as pkl:
            print('Got from cache')
            return json.load(pkl)
    with open(pkl_path, 'w') as pkl:
        res = get_petions_from(f'https://www.change.org/api-proxy/-/petitions/search?q={keyword}&lang={lang}')
        json.dump(res, pkl)
        return res


def get_petitions_by_tag(tag):
    pkl_path = os.path.join('json', 'tags', f"{tag}.json")
    os.makedirs(os.path.dirname(pkl_path), exist_ok=True)
    if os.path.isfile(pkl_path) and os.path.getsize(pkl_path):
        with open(pkl_path, 'r') as pkl:
            print('Got from cache')
            return json.load(pkl)
    with open(pkl_path, 'w') as pkl:
        res = get_petions_from(f'https://www.change.org/api-proxy/-/tags/{tag}/petitions?')
        json.dump(res, pkl)
        print("Saved in cache.")
        return res


def download_images_from_petitions(data, folder_name='unnamed'):
    download_count = 0
    already_downloaded = 0
    no_pic_found = 0

    folder_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), folder_name)

    os.makedirs(os.path.dirname(folder_path), exist_ok=True)

    print('Sto scaricando le immagini...')

    for each in tqdm(data['items']):

        # Esiste almeno una petizione per cui "slug" non è un key valido nel JSON
        # Dio solo sa come

        if "slug" not in each['petition']:
            continue

        # ho aggiunto [:140] perché win ha un limite sulla lunghezza del nome del file e del percorso del file.
        # da documentazione dovrebbe essere 160, ma con win non si sa mai

        filename = os.path.join(folder_path, f"{each['petition']['slug'][:140]}.jpg", )

        # Controlla se l'immagine è già stata scaricata
        if SKIP_ALREADY_DOWNLOADED and os.path.isfile(filename):
            # print(each['id'] + ' has already been downloaded')
            already_downloaded = already_downloaded + 1
            continue

        # Prende l'url dell'immagine alla risoluzione più alta e la scarica
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

        # A quanto pare non era la questione del -1, se provi a lanciare la ricerca per keyword con "covid" su en-US
        # ne ha tipo 5-6 missing quindi penso sia sempre la questione del limit > remaining
        # troppi pochi neuroni a disposizione per risolvere ora. Anche perche honestly 5 su 67279 capita, amen, ciao.
        # P.S. ricercando per tag pare non succeda

        if 'missingPetition' in petition:
            print('ERROR: PETITION NOT FOUND :(')
            continue

        # pprint(petition['activity'])

        # TODO questo potenzialmente tutto in row?
        title = petition['title']
        slug = petition['slug']
        user = petition['user']
        creator_name = petition['creator_name']
        targets = petition['targeting_description']
        relevant_location = petition['relevant_location']
        date = petition['created_at'][:10]
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

        # Array con le variabili che inserisco nelle colonne
        # cols_lst = {
        #     'share_copylink': 0,
        #     'share_email' : 0,
        #     share_facebook,
        #     share_sms,
        #     share_twitter,
        #     share_whatsapp,
        #     recruit,
        #     recruit_copylink,
        #     recruit_email,
        #     recruit_facebook,
        #     recruit_sms,
        #     recruit_twitter,
        #     recruit_whatsapp
        # }
        #
        # keys_lst = [
        #     'share.copylink.count',
        #     'share.email.count',
        #     'share.facebook.count',
        #     'share.sms.count',
        #     'share.twitter.count',
        #     'share.whatsapp.count',
        #     'recruit..count',
        #     'recruit.copylink.count',
        #     'recruit.email.count',
        #     'recruit.facebook.count',
        #     'recruit.sms.count',
        #     'recruit.twitter.count',
        #     'recruit.whatsapp.count'
        # ]

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

        ##SHARE##
        try:
            share_copylink = petition['activity']['share.copylink.count']
        except KeyError:
            share_copylink = ''

        try:
            share_email = petition['activity']['share.email.count']
        except KeyError:
            share_email = ''

        try:
            share_facebook = petition['activity']['share.facebook.count']
        except KeyError:
            share_facebook = ''

        try:
            share_sms = petition['activity']['share.sms.count']
        except KeyError:
            share_sms = ''

        try:
            share_twitter = petition['activity']['share.twitter.count']
        except KeyError:
            share_twitter = ''

        try:
            share_whatsapp = petition['activity']['share.whatsapp.count']
        except KeyError:
            share_whatsapp = ''

        ##CONVERSION##
        try:
            recruit = petition['activity'][
                'recruit..count']  # Questo non so a cosa si riferisca onestamente, immagino le firme da sito e basta?
        except KeyError:
            recruit = ''

        try:
            recruit_copylink = petition['activity']['recruit.copylink.count']
        except KeyError:
            recruit_copylink = ''

        try:
            recruit_email = petition['activity']['recruit.email.count']
        except KeyError:
            recruit_email = ''

        try:
            recruit_facebook = petition['activity']['recruit.facebook.count']
        except KeyError:
            recruit_facebook = ''

        try:
            recruit_sms = petition['activity']['recruit.sms.count']
        except KeyError:
            recruit_sms = ''

        try:
            recruit_twitter = petition['activity']['recruit.twitter.count']
        except KeyError:
            recruit_twitter = ''

        try:
            recruit_whatsapp = petition['activity']['recruit.whatsapp.count']
        except KeyError:
            recruit_whatsapp = ''

        row = {
            'date': date,
            'title': title,
            'signatures': signatures,
            'page_views': page_views,
            # 'origin': found_through,
            # 'key_term': key_term,
            'country': relevant_location['country_code'],
            'tags': ', '.join([x['slug'] for x in tags_]),
            'user_id': user['id'],
            'user': creator_name,
            'targets': targets,
            'description': description,
            'victory': is_victory,
            'verified_victory': is_verified_victory,
            'sponsored': sponsored,
            'share_count_total': share_count,
            'recruit': recruit,
            'share_copylink': share_copylink,
            'recruit_copylink': recruit_copylink,
            'share_email': share_email,
            'recruit_email': recruit_email,
            'share_facebook': share_facebook,
            'recruit_facebook': recruit_facebook,
            # 'share_facebook_messenger': share_facebook_messenger,
            # 'recruit_facebook_messenger': recruit_facebook_messenger,
            'share_sms': share_sms,
            'recruit_sms': recruit_sms,
            'share_twitter': share_twitter,
            'recruit_twitter': recruit_twitter,
            'share_whatsapp': share_whatsapp,
            'recruit_whatsapp': recruit_whatsapp,
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

    print()
    service.spreadsheets().values().update(spreadsheetId=PETITIONS_SPREADSHEET_ID, range=f"{tab_name}!A2:ZZZ",
                                           body={"values": df.values.tolist()},
                                           valueInputOption="USER_ENTERED"
                                           ).execute()


if __name__ == '__main__':
    tags = [
        # IT-IT
        'coronavirus-it-it',
        'giustizia-economica',
        'salute',
        # EN-US
        'coronavirus-epidemic-en-us',
        'coronavirus-aid-en-us',
        'economic-justice-10',
        'health-en-us'
        # ES
        # 'sanidad',
        # 'coronavirus-es-419'
    ]

    keywords = [
        # 'covid',
        # 'covid-19',
        # 'coronavirus',
        # 'no vax',
        # 'vaccine',
        # 'vaccino',
        # 'lockdown',
        # 'greenpass'
    ]

    langs = [
        # 'it-IT',
        # 'en-US',
        # 'en-GB'
    ]

    for tag in tags:
        print(f"\033[94mLooking for tag {tag}\033[0m")
        petitions = get_petitions_by_tag(tag)
        if not len(petitions['items']):
            continue
        print(f"Found {len(petitions['items'])} in tag {tag}")
        store_petitions(petitions, key_term=tag)

        if os.environ.get('DOWNLOAD_IMAGES', False):
            download_images_from_petitions(petitions, os.path.join('tags', tag))

    # for lang in langs:
    #     for keyword in keywords:
    #         print(f"\033[94mLooking for keyword {keyword} in lang {lang}\033[0m")
    #         petitions = get_petition_by_keyword(keyword, lang)
    #         if not len(petitions['items']):
    #             continue
    #         print(f"Found {len(petitions['items'])} in keyword {keyword}")
    #         store_petitions(petitions, key_term=keyword, found_through='keyword')
    #
    #         if os.environ.get('DOWNLOAD_IMAGES', False):
    #             download_images_from_petitions(petitions, os.path.join('keywords', lang, keyword))

    save_petitions_to_sheets()
