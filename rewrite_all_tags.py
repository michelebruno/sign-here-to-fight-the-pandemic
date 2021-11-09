import datetime
import os.path

import pandas

import utils.change as change

all_pets = change.get_petitions_by_keyword('covid', lang=change.all_change_langs)

print(all_pets.shape)

all_pets = all_pets.loc[all_pets['published_at'] > datetime.datetime(2020, 1, 1)]

print(all_pets.shape)

all_pets = change.filter_only_for_chosen_countries(all_pets)

change.save_all_petitions(all_pets)

tags_per_country = pandas.DataFrame()

for country, pets in all_pets.groupby('country'):
    tags = change.count_not_normalized_tags(pets, country=country)
    chosen_tags = tags.sort_values(by='total_count', ascending=False).head(150)
    tags_per_country = pandas.concat([tags_per_country, chosen_tags], ignore_index=True)

tags_per_country = tags_per_country.assign(
    normalized=lambda t: t['name'].apply(lambda x: change.normalize_tag(x) if change.has_tag_been_normalized(x) else ''))

tags_per_country.to_csv(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'chosen_tags.csv'))
