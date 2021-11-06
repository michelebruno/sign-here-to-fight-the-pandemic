import datetime
import os.path
from tags_per_country import found_tags as covid_tags


import pandas as pd

from utils.change import get_petitions_by_tag, tag_slugs_from_normalized, download_images_from_petitions, \
    get_normalized_tags


def download_from_normalized_tag(t):
    look_for_this_normalized_tag = t

    normalized = list(get_normalized_tags())

    found_tags = tag_slugs_from_normalized(look_for_this_normalized_tag)

    european_countries = pd.read_csv('country-code_dict.csv')

    all_pets = []

    # TODO qui riscarica tutte le foto di ciascun tag, non solo delle petizioni legate al covid
    for tag in found_tags:
        res = get_petitions_by_tag(tag)
        # print(f"Found for tag\t{tag}\t{res['total_count']}")
        petitions = res['items']
        for pet in petitions:
            if any(i in pet['tag_slugs'] for i in covid_tags):
                pet['origin_tag'] = tag
                all_pets.append(pet)

    all_pets = pd.DataFrame(all_pets)

    all_pets.drop_duplicates('id', inplace=True)

    all_pets = all_pets.loc[(all_pets['published_at'] > datetime.datetime(2020, 1, 1)) & (
        all_pets['country'].isin(european_countries['country-code']))]

    download_images_from_petitions(all_pets, os.path.join('normal-tags', look_for_this_normalized_tag))
