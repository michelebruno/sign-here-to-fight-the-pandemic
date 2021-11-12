# SAMPLE REQUEST
# https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

# l'idea migliore che mi è venuta è continuare a fare richieste a blocchi di limit=100, poi offset+=100, poi continuo
# fino a quando last_page == true (last page è la penultima key del json, prima di total count)

# GOOGLE TRANSLATE API
import os.path
import plotly.express as px

import numpy
import pandas
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from google.cloud import language_v1 as language
import csv
import google.api_core.exceptions

from tqdm import tqdm

import utils.change
from utils.change import get_json_path, get_petition_comments, get_all_petitions
from utils.google_services import save_list_to_sheets_tab

analysis_client = language.LanguageServiceClient()


def analyze_comments(comments, petition_id):
    jsonpath = get_json_path('comment-analysis', f"{petition_id}-analysis.json")

    if os.path.exists(jsonpath):
        # print(f"{petition_id} comment analysis got from cache")
        return pandas.read_json(jsonpath)

    results = []

    for index, row in tqdm(comments.iterrows(), total=comments.shape[0], desc=f"Analyzing for petition {petition_id}",
                           position=1):
        source_text = row['comment']

        document = language.types.Document(content=source_text, type_=language.Document.Type.PLAIN_TEXT)
        try:
            response = analysis_client.analyze_entities(document=document, encoding_type='UTF8')

            for entity in response.entities:
                results.append({'commentable_id': row['commentable_id'], 'comment': source_text, 'name': entity.name,
                                'type': str(entity.type_)[5:]})
        except google.api_core.exceptions.InvalidArgument:
            continue

    df = pandas.DataFrame(results)

    df.to_json(jsonpath)

    return df


def analyze_petition_text(text, petition_id):
    jsonpath = get_json_path('comment-analysis', f"{petition_id}-analysis.json")

    if os.path.exists(jsonpath):
        # print(f"{petition_id} comment analysis got from cache")
        return pandas.read_json(jsonpath)

    results = []

    for index, row in tqdm(text.iterrows(), total=text.shape[0], desc=f"Analyzing for petition {petition_id}",
                           position=1):
        source_text = row['comment']

        document = language.types.Document(content=source_text, type_=language.Document.Type.PLAIN_TEXT)
        try:
            response = analysis_client.analyze_entities(document=document, encoding_type='UTF8')

            for entity in response.entities:
                results.append({'commentable_id': row['commentable_id'], 'comment': source_text, 'name': entity.name,
                                'type': str(entity.type_)[5:]})
        except google.api_core.exceptions.InvalidArgument:
            continue

    df = pandas.DataFrame(results)

    df.to_json(jsonpath)

    return df


all_pets = get_all_petitions()


def analyze_comments_from_petition_slugs(slugs, topic_name, country=None, petitions_limit=None):
    print(f"Analyzing petition comments about {topic_name}")

    filtered_pets = all_pets.loc[all_pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

    if country is not None:
        filtered_pets = all_pets.loc[all_pets['country'] == 'US']
    top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False)

    if petitions_limit:
        top_pets = top_pets.head(petitions_limit)

    comments = get_petition_comments(top_pets['id'].tolist())

    analysed_results = pandas.DataFrame()

    for petition_id, comms in tqdm(comments.groupby('commentable_id'), colour='green'):
        analysed_results = pandas.concat([analysed_results, analyze_comments(comms, petition_id)], ignore_index=True)

    analysed_results = analysed_results.loc[~analysed_results['type'].isin(['NUMBER'])]
    analysed_results = analysed_results.assign(comment_hash=lambda p: p['comment'].map(hash))

    analysed_results['name'] = analysed_results['name'].str.lower()

    find_replace = [
        ('mask mandates', 'mask mandate'),
        ('freedoms', 'freedom'),
        ('masks', 'mask'),
        ('vaccines', 'vaccine'),
        ('decisions', 'decision'),
        ('schools', 'school'),
        ('choices', 'choice'),
        ('families', 'family'),
        ('doctors', 'doctor'),
        ('reasons', 'reason'),
        ('benefits', 'benefit'),
        ('students', 'student'),
        ('face mask', 'face masks'),
        ('covid-19', 'covid19'),
        ('opinions', 'opinion',),
        ('precautions', 'precaution'),
        ('measures', 'measure'),
        ('human beings', 'human beings'),
        ('grandparents', 'grandparent'),
        ('parent', 'parents'),
        ('numbers', 'number'),
        ('teachers', 'teacher'),
        ('diseases', 'disease'),
        ('chances', 'chance'),
        ('healthcare worker', 'healthcare workers'),
        ('recommendations', 'recommendation'),
        ('districts', 'district'),
        ('residents', 'resident'),
        ('leaders', 'leader'),
        ('right', 'rights'),
        ('infection', 'infections'),
        ('hospital', 'hospitals'),
        ('classroom', 'classrooms'),
        ('systems', 'system'),
        ('concerns', 'concern'),
        ('loved one', 'loved ones'),
        ('doctors', 'doctor'),
        ('health issue', 'health issues'),
        ('quarantines', 'quarantine'),
        ('individuals', 'individual'),
        ('results', 'result'),
        ('child', 'children'),
        ('efforts', 'effort'),
        ('buildings', 'building'),
        ('variants', 'variant'),
        ('mandates', 'mandate'),
        ('physicians', 'physician'),
        ('scientists', 'scientist'),
        ('granddaughters', 'granddaughter'),
        ('americans', 'american'),
        ('reason', 'reasons'),
        ('option', 'options'),
    ]

    for (find, replace) in find_replace:
        analysed_results.replace(to_replace=find, value=replace, inplace=True)

    summedup = analysed_results.groupby(by=['name']).size()

    summedup = summedup.reset_index(name='count')

    summedup = summedup.loc[summedup['count'] > 10]

    summedup.sort_values(by='count', inplace=True, ascending=False)

    summedup = summedup.head(90)

    summedup.to_csv(
        utils.change.get_onedrive_path('csv', f'entities-{topic_name}-top-comments.csv'),
        encoding='UTF-8', quoting=csv.QUOTE_ALL, index=False)

    summedup = summedup.assign(source=topic_name)
    return summedup


nomask_tagnames = ['unmasking', 'mask choice', 'unmask our kids']
promask_tagnames = ['mask in school', 'mask mandate', 'mask to fight covid-19', 'make mask mandatory']

unmask_analyzed = analyze_comments_from_petition_slugs(set(utils.change.slugs_from_normalized_tag(nomask_tagnames)),
                                                       topic_name='unmask',
                                                       petitions_limit=100)
promask_analyzed = analyze_comments_from_petition_slugs(set(utils.change.slugs_from_normalized_tag(promask_tagnames)),
                                                        topic_name='promask', petitions_limit=100)

unmask_analyzed_total = unmask_analyzed['count'].sum()
promask_analyzed_total = promask_analyzed['count'].sum()

unmask_analyzed = unmask_analyzed.assign(
    percentage=lambda x: x['count'] * 100 / unmask_analyzed_total,
    normalized=lambda x: numpy.log10(x['count'])
)

promask_analyzed = promask_analyzed.assign(
    percentage=lambda x: x['count'] * 100 / promask_analyzed_total,
    normalized=lambda x: numpy.log10(x['count'])
)

data = []

for i, row in unmask_analyzed.iterrows():

    name = row['name']

    filtered_promask = promask_analyzed.loc[promask_analyzed['name'] == name]

    if filtered_promask.shape[0]:
        promask_data = filtered_promask.iloc[0]
        data.append(
            {'name': name, 'nomask': row['count'], 'promask': promask_data['count'],
             'nomask_%': row['percentage'], 'promask_%': promask_data['percentage'],
             'nomask_normal': row['normalized'], 'promask_normal': promask_data['normalized'],
             }
        )

scatter = pandas.DataFrame(data)

scatter = scatter.assign(summed_normal=lambda x: x['nomask_normal'] + x['promask_normal'])
scatter.sort_values(by='summed_normal', inplace=True)
scatter.to_csv(utils.change.get_onedrive_path('csv', 'scatter-boh.csv'), index=False)

scartate = pandas.concat([
    unmask_analyzed.loc[~unmask_analyzed['name'].isin(scatter['name'])],
    promask_analyzed.loc[~promask_analyzed['name'].isin(scatter['name'])],
], ignore_index=True)

scartate.to_csv(utils.change.get_onedrive_path('csv', 'scatter-scartate.csv'), index=False)

pandas.concat([promask_analyzed.assign(source='promask'), unmask_analyzed.assign(source='unmask')]).to_csv(
    utils.change.get_onedrive_path('csv', 'entietes_both.csv'))

fig = px.scatter(scatter, x='nomask', y='promask', text='name', width=1500, height=1000)
fig.update_traces(textposition='middle right', textfont=dict(
    size=16,
), marker=dict(
    size=10,
))

# fig.update_xaxes(range=[-1.2, 1])
# fig.update_yaxes(range=[-1.2, 1])

fig.write_image(utils.change.get_onedrive_path('protocol3-scatter.svg'))


def get_comments_of_pet():
    slugs = set(utils.change.slugs_from_normalized_tag(nomask_tagnames))
    filtered_pets = all_pets.loc[all_pets['tag_slugs'].map(
        lambda x: True if set(x).intersection(slugs) else False)]

    top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False).head(2)

    comments = get_petition_comments(top_pets['id'].tolist())

    comments['comment'].csv('comments.json', orient='values')

# get_comments_of_pet()
