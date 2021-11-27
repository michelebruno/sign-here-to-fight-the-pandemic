import os

import os.path

import pandas
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from dotenv import load_dotenv
from google.cloud import language_v1 as language
import csv
import google.api_core.exceptions
from change import tags, petitions,comments
import utils.google_services

load_dotenv()

analysis_client = language.LanguageServiceClient()

promask_tagnames = ['alberta', 'toronto', 'ontario british columbia', 'vancouver', 'québec']

slugs = tags.slugs_from_normalized_tag(promask_tagnames)

all_pets = petitions.get()

pets = all_pets.loc[all_pets['country'] == 'CA']

filtered_pets = all_pets.loc[all_pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False)

top_pets = top_pets.head(100)


def analyze_petition_text(petition):
    jsonpath = change.get_json_path('body-analysis', f"{petition['id']}-analysis.json")

    if os.path.exists(jsonpath):
        # print(f"{petition_id} comment analysis got from cache")
        return pandas.read_json(jsonpath)

    results = []

    source_text = petition['description']

    document = language.types.Document(content=source_text, type_=language.Document.Type.PLAIN_TEXT)
    try:
        response = analysis_client.analyze_entities(document=document, encoding_type='UTF8')

        for entity in response.entities:
            results.append({'petition_id': petition['id'], 'petition_slug': petition['slug'], 'name': entity.name,
                            'type': str(entity.type_)[5:]})

        df = pandas.DataFrame(results)

        df.to_json(jsonpath)

        return df
    except google.api_core.exceptions.InvalidArgument:
        return


res = []
for i, pet in top_pets:
    res.append(analyze_petition_text(pet))
df = pandas.DataFrame(res)

counted = df.groupby(by=['name']).size().reset_index('count')

utils.google_services.save_list_to_sheets_tab(counted, 'text-canada')