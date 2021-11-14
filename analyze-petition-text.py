import os

import os.path

import pandas
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from dotenv import load_dotenv
from google.cloud import language_v1 as language
import csv
import google.api_core.exceptions
import utils.change as change
import utils.google_services

load_dotenv()

analysis_client = language.LanguageServiceClient()

promask_tagnames = ['alberta', 'toronto', 'ontario british columbia', 'vancouver', 'québec']

slugs = change.slugs_from_normalized_tag(promask_tagnames)

all_pets = change.get_all_petitions()

pets = all_pets.loc[all_pets['country'] == 'IT']


def analyze_petition_text(petition, **kwargs):
    jsonpath = change.get_json_path('body-analysis', f"{petition['id']}-analysis.json")

    if os.path.exists(jsonpath):
        # print(f"{petition_id} comment analysis got from cache")
        return pandas.read_json(jsonpath)

    results = []

    source_text = petition['description']

    document = language.types.Document(content=source_text,language=petition['original_locale'], type_=language.Document.Type.HTML)
    try:
        response = analysis_client.analyze_entities(document=document, encoding_type='UTF8')

        for entity in response.entities:
            results.append(
                {**kwargs, 'petition_id': petition['id'], 'petition_slug': petition['slug'], 'name': entity.name,
                 'type': str(entity.type_)[5:]})

        df = pandas.DataFrame(results)

        df.to_json(jsonpath)

        return df
    except google.api_core.exceptions.InvalidArgument:
        return


res = pandas.DataFrame()
for tag in ['lombardy', 'sicily', 'umbria', 'calabria', 'lazio', 'puglia', 'sardegna']:
    slugs = change.slugs_from_normalized_tag(tag)

    filtered_pets = pets.loc[pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

    # top_pets = top_pets.head(100)
    entities = pandas.DataFrame()

    for i, pet in filtered_pets.iterrows():
        entities = pandas.concat([entities, analyze_petition_text(pet)], ignore_index=True)

    counted = entities.groupby(by='name').size()
    counted = counted.reset_index(name='count')

    counted.sort_values(by='count', ascending=False, inplace=True)

    res = pandas.concat([res, counted.head(50).assign(tag=tag)])

utils.google_services.save_list_to_sheets_tab(res, 'italian-tags-entities')
