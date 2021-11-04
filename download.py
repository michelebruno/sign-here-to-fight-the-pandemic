import datetime
import os.path

import pandas as pd

from utils.change import get_petitions_by_tag, tag_slugs_from_normalized, download_images_from_petitions

def download_from_normalized_tag(t):
    look_for_this_normalized_tag = t

    found_tags = tag_slugs_from_normalized(look_for_this_normalized_tag)

    european_countries = pd.read_csv('country-code_dict.csv')

    all_pets = []

    for tag in found_tags:
        res = get_petitions_by_tag(tag)
        # print(f"Found for tag\t{tag}\t{res['total_count']}")
        petitions = res['items']
        for pet in petitions:
            pet['origin_tag'] = tag
            all_pets.append(pet)

    all_pets = pd.DataFrame(all_pets)

    all_pets.drop_duplicates('id', inplace=True)

    all_pets = all_pets.loc[(all_pets['published_at'] > datetime.datetime(2020, 1, 1)) & (
        all_pets['country'].isin(european_countries['country-code']))]

    download_images_from_petitions(all_pets, os.path.join('normal-tags', look_for_this_normalized_tag))
