import datetime
import os.path

import pandas as pd

from main import download_images_from_petitions
from utils.change import get_petitions_by_tag

found_tags = ['vacunas-es-es', 'vaccini', 'vaccination-fr-fr', 'vaccines-en-gb']


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

download_images_from_petitions(all_pets, os.path.join('normal-tags', 'vaccines'))