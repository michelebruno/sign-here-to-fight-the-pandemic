import pandas as pd
from dotenv import load_dotenv

from utils.change import get_petitions_by_tag, group_by_relevant_location, count_tags, group_petitions_by_month, \
    get_tags_through_keyword, unique_petitions
from utils.google_services import save_list_to_sheets_tab

load_dotenv()

tags = [
    "coronavirus-covid-19-fr-fr",
    "coronavirus-en-gb",
    "coronavirus-epidemic-en-us",
    "coronavirus-fr-fr",
    "covid-19-en-gb",
    "covid-19-fr-fr",
    "covid-19-it-it",
    'corona-de-de',
    'coronavirus-covid-19-fr-fr',
    'coronavirus-de-de',
    'coronavirus-en-gb',
    'coronavirus-epidemic-en-us',
    'coronavirus-es-es',
    'coronavirus-fr-fr',
    'coronavirus-it-it',
    'coronavirüs-tr-tr',
    'covid-19-en-gb',
    'covid-19-fr-fr',
    'covid-19-health-emergency-en-gb',
    'covid-19-it-it',
    'covid-fr-fr',
    'covid19-en-gb',
    'коронавирус-ru-ru',


]

all_pets = []

for tag in tags:
    petitions = get_petitions_by_tag(tag)['items']
    for pet in petitions:
        pet['origin_tag'] = tag
        all_pets.append(pet)

all_pets = unique_petitions(all_pets)


def tags_by_country(petitions):
    stored_tags_by_country = []

    for country, pets in group_by_relevant_location(petitions):

        country_tags = count_tags(pets, country=country)

        for _tag in country_tags:
            stored_tags_by_country.append({
                'country': country,
                **country_tags[_tag]
            })

    df = pd.DataFrame(stored_tags_by_country)

    df['total_count'] = df.groupby(['name', 'country'], as_index=False).total_count.transform('sum')

    df.drop_duplicates(subset=['name', 'country'], inplace=True)

    df.drop(columns=['photo_id', 'slug', 'id', 'created_by_owner', 'created_by_staff_member'], inplace=True)

    save_list_to_sheets_tab(df, 'tags_country')
    return df


def tags_by_month_by_country(petitions):
    # BY MONTH
    stored_tags_by_month = []

    for country, pets in group_by_relevant_location(petitions):

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

    df = pd.DataFrame(stored_tags_by_month)

    df['total_count'] = df.groupby(['name', 'country', 'month'], as_index=False).total_count.transform('sum')

    df.drop_duplicates(subset=['name', 'country', 'month'], inplace=True)

    df.drop(columns=['photo_id', 'slug', 'id', 'created_by_owner', 'created_by_staff_member'], inplace=True)

    save_list_to_sheets_tab(df, 'tags_months_country')


if __name__ == '__main__':
    save_list_to_sheets_tab(
        [i for i in get_tags_through_keyword('covid-19', lang='de-DE', country='DE') if i['total_count'] > 99],
        'de-tags')
    tags_by_country(all_pets)
    tags_by_month_by_country(all_pets)
