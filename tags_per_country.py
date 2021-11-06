import datetime

import pandas
import pandas as pd
from dotenv import load_dotenv

from utils.change import get_petitions_by_tag, count_tags, get_normalized_tags, save_all_petitions
from utils.google_services import save_list_to_sheets_tab

load_dotenv()

found_tags = [
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
    'covid-19-de-de',
    'corona-virus-de-de',
    'covid19-de-de'
]

european_countries = pandas.read_csv('country-code_dict.csv')

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

save_all_petitions(all_pets)

if __name__ == '__main__':
    # Qui tutti i conteggi dei tag per country
    stored_tags = []

    # Qui tutti i conteggi dei tag per country per mese
    stored_months_tags = []

    for c, items in all_pets.groupby('country'):
        tags = count_tags(items, country=c)
        stored_tags += tags.to_dict('records')

        for month, items2 in items.groupby('month'):
            ts = count_tags(items2, country=c, month=month)
            stored_months_tags += ts.to_dict('records')

    stored_months_tags = [t for t in stored_months_tags if t['name'] in [m for i, m in get_normalized_tags().items()]]

    save_list_to_sheets_tab(stored_tags, 'tags_country')
    save_list_to_sheets_tab(stored_months_tags, 'tags_months_country')

    # Output pivot csv
    stored_tags = pandas.DataFrame(stored_tags)
    stored_tags.loc[stored_tags['total_count'] > 5].pivot_table(values='total_count', columns='name',
                                                                index='country').fillna(0).to_csv(
        'pivot/tags_per_country.csv')
