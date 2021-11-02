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

    # Related (?)
    'fermeture-lycée-fr-fr',
    'fermeture-université-fr-fr',
    'coronavirus-school-closures-en-us',
    'confinamiento-fr-fr',
    'confinement-fr-fr',
    'lockdown-it-it',
    'coprifuoco-it-it',
    'couvre-feu-fr-fr',
    'corona-fr-fr',
    'corona-de-de',
    'corona-virus-fr-fr',
    'coronavirus-fr-fr',
    'coronavirus-en-us',
    'coronavirus-en-gb',
    'coronavirus-it-it',
    'corona-virus-covid19-it-it',
    'coronavirus-covid-19-fr-fr',
    'coronavirus-commencement-en-us',
    'coronavirus-financial-relief-fr-fr',  # found 1
    'coronavirus-ticino-it-it',
    'coronavirusitalia-it-it',
    'covid-fr-fr',
    'covid-it-it',
    'covid-19-fr-fr-d7199f86-930c-4e6d-ad86-d590dfc30dd3',
    'covid-19-it-it-33339ba5-ac29-4166-bddc-ea3645fbb227',
    'covid-19-fr-fr',
    'covid-19-en-gb',
    'covid-19-et-autres-virus-fr-fr',
    'covid-19-fase2-it-it',
    'covid19-fr-fr',
    'covid19-it-it',
    'covid19-tamponi-dpi-it-it',
    'covid19-testsierologico-it-it',
    'crise-coronavirus-fr-fr',
    'crise-sanitaire-fr-fr',
    'crise-sanitaire-en-france-fr-fr',
    'dpcm-conte-it-it',
    'ehpad-fr-fr',
    'emergency-it-it',
    'emergenza-it-it',
    'urgence-fr',
    'emergenza-coronavirus-it-it',
    'emergenza-sanitaria-it-it',
    'urgence-sanitaire-fr-fr',
    'covid-19-health-emergency-en-gb',
    'épidémie-fr-fr',
    'epidemiology-fr-fr',
    'fase-2-it-it',
    'fase-due-it-it',
    'fase2-it-it',
    'greenpass-it-it',
    'infirmiers-fr-fr',
    'infermieri-it-it',
    'infermieri-covid-19-it-it',
    'masque-fr-fr',
    'masque-obligatoire-fr-fr',
    'masque-pour-tous-fr-fr',
    'masques-fr-fr',
    'pandemia-it-it',
    'pandemie-fr-fr',
    'coronavirus-pandemic-fr-fr',
    'pandemics-fr-fr',
    'covid-19-politique-fr-fr',
    'quarantena-it-it',
    'quarantine-fr-fr',
    'rsa-it-it',
    'sars-cov-2-it-it',
    'sars-cov-2-fr-fr-4ded7082-6862-4455-b437-706c8c7ea876',
    'scuola-vaccinazione-covid-it-it',
    'reste-chez-toi-fr-fr',
    'swisscovid-fr-fr',
    'tampone-it-it',
    'tampone-covid-19-it-it',
    'tamponi-it-it',
    'tamponi-a-tappeto-it-it',
    'réanimation-fr-fr',
    'terapia-intensiva-it-it',
    'traitement-covid-19-fr-fr',
    'vaccination-fr-fr',
    'vaccinazione-anti-covid19-it-it',
    'vaccination-obligatoire-fr-fr',
    'vaccinazioni-it-it',
    'vaccini',
    'vaccins',
    'vaccin-fr-fr',
    'vaccine-fr-fr',
    'vaccino-it-it',
    'zona-rossa-it-it',
    'covid-19-es-es',
    'vacunas-es-es',
    'covid-19-it-it',
    'covid-19-tr-tr',
    'covid19-en-gb',
    'covid-19-economic-impact-en-gb',
    'covid-19-infection-en-gb',
    'coronavirus-epidemic-en-gb',
    'corona-virus-en-gb',
    'coronavirus-pandemic-en-gb',
    'covid-19-nhs-en-gb',
    'lockdown-en-gb',
    'corona-en-gb',
    'covid-19-vaccination-en-gb',
    'covid-19-cure-en-gb',
    'covid-19-impact-en-gb',
    'pandemic-covid-en-gb',
    'covid-19-uk-en-gb',
    'covid-19-hyderabad-en-gb',
    'testing-for-covid-en-gb',
    'covid19-pandemic-homeless-en-gb',
    'covid-19-en-gb-66788069-a95a-46e3-8247-8ad4903e105a',
    'cornavirus-en-gb',
    'quarantine-en-gb',
    'covid-en-gb',
    'sars-cov-2-en-gb',
    'wuhan-coronavirus-en-gb',
    'passeport-vaccinal-fr-fr',
    'vaccin-obligatoire-fr-fr',
    'vaccination-covid-19-fr-fr',
    'port-du-masque-fr-fr',
    'covid19-pandemic-response-en-gb',
    'pandemic-en-gb',
    'coronovirus-en-gb',
    'covid19-grant-en-gb',
    'lucknow-en-gb',
    'corona-warriors-en-gb',
]

all_pets = []

for tag in tags:
    res = get_petitions_by_tag(tag)
    # print(f"Found for tag\t{tag}\t{res['total_count']}")
    petitions = res['items']
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

    df = df.loc[df['total_count'] > 200]

    # df['total_count'] = df.groupby(['name', 'country'], as_index=False).total_count.transform('sum')

    # df.drop_duplicates(subset=['name', 'country'], inplace=True)

    df.drop(columns=['photo_id', 'slug', 'id', 'created_by_owner', 'created_by_staff_member'], inplace=True)

    save_list_to_sheets_tab(df, 'tags_country')
    return df


def tags_by_month_by_country(petitions):
    # BY MONTH
    stored_tags_by_month = []

    for country, pets in group_by_relevant_location(petitions):

        by_month = group_petitions_by_month(pets)

        for month, l in by_month:
            groups = count_tags(l, month=month, country=country)

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
    # save_list_to_sheets_tab(
    #     [i for i in get_tags_through_keyword('covid-19', lang='de-DE', country='DE') if i['total_count'] > 99],
    #     'de-tags')
    by_country = tags_by_country(all_pets)
    # tags_by_month_by_country(all_pets)
