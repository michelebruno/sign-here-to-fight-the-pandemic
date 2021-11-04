import os.path
from pprint import pprint

from utils.http import http
from utils.change import get_petitions_by_tag
import pandas
from tqdm import tqdm
from utils.google_services import get_service, save_list_to_sheets_tab
from dotenv import load_dotenv
from urllib.parse import quote
from tags_per_country import all_pets
import re

CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext


# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

service = get_service()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

sheet_cleared = False

stored_petitions = []


def store_petitions(
        data, key_term,
        tab_name='petitions',
        found_through='tag', ):
    global stored_petitions

    for _index, petition in data.iterrows():

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
            'origin': key_term,
            'date': date,
            'title': title,
            'signatures': signatures,
            'page_views': page_views,
            # 'key_term': key_term,
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
        stored_petitions.append(row)


if __name__ == '__main__':
    tags = [
        # IT-IT
        'coronavirus-it-it',
        # 'giustizia-economica',
        # 'salute',
        # EN-US
        # 'coronavirus-epidemic-en-us',
        # 'coronavirus-aid-en-us',
        # 'economic-justice-10',
        # 'health-en-us'
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
    store_petitions(all_pets, '')
    save_list_to_sheets_tab(stored_petitions, 'petitions')
