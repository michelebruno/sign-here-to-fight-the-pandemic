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

group_by_country = group_by_relevant_location(all_pets)

stored_tags = []

for country, pets in group_by_country:

    by_month = group_petitions_by_month(pets)

    for month, l in by_month:
        groups = count_tags(l)

        for group in groups:
            tag = {
                'month': month,
                'relevant_country': country,
                **groups[group]
            }
            stored_tags.append(tag)

save_list_to_sheets_tab(stored_tags, 'tags_months_country',
                        columns=['total_count', 'relevant_country', 'id', 'locale', 'name', 'slug', 'created_by_owner',
                                 'created_by_staff_member', ])
