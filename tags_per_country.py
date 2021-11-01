from pprint import pprint

from dotenv import load_dotenv

from utils.change import get_petitions_by_tag, group_by_relevant_location, count_tags, group_petitions_by_month
from utils.google_services import save_list_to_sheets_tab

load_dotenv()

tags = [
    'coronavirus-it-it',
    'coronavirus-de-de',
    'coronavirüs-tr-tr',
    'coronavirus-es-es',
    'коронавирус-ru-ru'
]

all_pets = []

for tag in tags:
    petitions = get_petitions_by_tag(tag)['items']
    for pet in petitions:
        pet['origin_tag'] = tag
        all_pets.append(pet)

stored_tags_by_country = []

for country, pets in group_by_relevant_location(all_pets):

    country_tags = count_tags(pets)

    for _tag in country_tags:
        tag = {
            'country': country,
            **country_tags[_tag]
        }

        stored_tags_by_country.append(tag)

save_list_to_sheets_tab(stored_tags_by_country, 'tags_country',
                        columns=['total_count', 'relevant_country', 'id', 'locale', 'name', 'slug', 'created_by_owner',
                                 'created_by_staff_member', ])

# BY MONTH
stored_tags_by_month = []

for country, pets in group_by_relevant_location(all_pets):

    by_month = group_petitions_by_month(pets)

    for month, l in by_month:
        groups = count_tags(l)

        for group in groups:
            tag = {
                'month': month,
                'country': country,
                **groups[group]
            }

            stored_tags_by_month.append(tag)

save_list_to_sheets_tab(stored_tags_by_month, 'tags_months_country',
                        columns=['total_count', 'relevant_country', 'id', 'locale', 'name', 'slug', 'created_by_owner',
                                 'created_by_staff_member', ])
