# SAMPLE REQUEST
# https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

# l'idea migliore che mi è venuta è continuare a fare richieste a blocchi di limit=100, poi offset+=100, poi continuo
# fino a quando last_page == true (last page è la penultima key del json, prima di total count)

# GOOGLE TRANSLATE API
import json
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

    for index, row in tqdm(comments.iterrows(), total=comments.shape[0], desc=f"Analyzing for petition {petition_id}"):
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

    for index, row in tqdm(text.iterrows(), total=text.shape[0], desc=f"Analyzing for petition {petition_id}"):
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


def flatten_comments(comments: pandas.DataFrame):
    df = comments.copy()

    df['created_at'] = df['created_at'].map(lambda x: x.strftime('%Y-%m-%D'))
    df.sort_values(by=['likes'], inplace=True, ascending=False)
    df.drop_duplicates('comment', inplace=True)

    df = df.head(1000)

    df = df.assign(
        petition_link=lambda x: x['commentable_entity'].map(
            lambda y: f"https://www.change.org/p/{y['slug']}"),
        author=lambda x: x['user'].map(
            lambda y: y['display_name'])
    )
    df.drop(columns=['commentable_entity', 'user', 'deleted_at', 'parent_id'], inplace=True)

    df['comment'] = df['comment'].apply(utils.change.cleanhtml)

    return df


def get_comments_from_petition_slugs(slugs, topic_name, country=None, petitions_limit=None):
    print(f"Analyzing petition comments about {topic_name}")

    all_pets = get_all_petitions()

    filtered_pets = all_pets.loc[all_pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

    if country is not None:
        filtered_pets = all_pets.loc[all_pets['country'] == 'US']
    top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False)

    if petitions_limit:
        top_pets = top_pets.head(petitions_limit)

    return get_petition_comments(top_pets['id'].tolist())


def analyze_comments_from_petition_slugs(slugs, topic_name, country=None, petitions_limit=None):
    comments = get_comments_from_petition_slugs(slugs, topic_name, country, petitions_limit)

    save_list_to_sheets_tab(flatten_comments(comments), f"commenti-{topic_name}")

    analysed_results = pandas.DataFrame()

    for petition_id, comms in tqdm(comments.groupby('commentable_id'), colour='green'):
        analysed_results = pandas.concat([analysed_results, analyze_comments(comms, petition_id)], ignore_index=True)

    analysed_results = analysed_results.loc[~analysed_results['type'].isin(['NUMBER'])]
    analysed_results = analysed_results.assign(comment_hash=lambda p: p['comment'].map(hash))

    analysed_results['name'] = analysed_results['name'].str.lower()

    find_replace = [
        ('americans', 'american'),
        ('benefits', 'benefit'),
        ('bodies', 'body'),
        ('buildings', 'building'),
        ('chances', 'chance'),
        ('child', 'children'),
        ('choices', 'choice'),
        ('classroom', 'classrooms'),
        ('concerns', 'concern'),
        ('covid-19', 'covid19'),
        ('decisions', 'decision'),
        ('diseases', 'disease'),
        ('districts', 'district'),
        ('doctors', 'doctor'),
        ('doctors', 'doctor'),
        ('efforts', 'effort'),
        ('face mask', 'face masks'),
        ('families', 'family'),
        ('freedoms', 'freedom'),
        ('granddaughters', 'granddaughter'),
        ('grandparents', 'grandparent'),
        ('health issue', 'health issues'),
        ('healthcare worker', 'healthcare workers'),
        ('hospital', 'hospitals'),
        ('human beings', 'human beings'),
        ('individuals', 'individual'),
        ('infection', 'infections'),
        ('kid', 'kids'),
        ('leaders', 'leader'),
        ('loved one', 'loved ones'),
        ('mandates', 'mandate'),
        ('mask mandates', 'mask mandate'),
        ('masks', 'mask'),
        ('measures', 'measure'),
        ('numbers', 'number'),
        ('opinions', 'opinion',),
        ('option', 'options'),
        ('parent', 'parents'),
        ('physicians', 'physician'),
        ('precautions', 'precaution'),
        ('quarantines', 'quarantine'),
        ('reason', 'reasons'),
        ('reasons', 'reason'),
        ('recommendations', 'recommendation'),
        ('residents', 'resident'),
        ('results', 'result'),
        ('right', 'rights'),
        ('schools', 'school'),
        ('scientists', 'scientist'),
        ('students', 'student'),
        ('systems', 'system'),
        ('teachers', 'teacher'),
        ('vaccines', 'vaccine'),
        ('variants', 'variant'),
    ]

    for (find, replace) in find_replace:
        analysed_results.replace(to_replace=find, value=replace, inplace=True)

    summedup = analysed_results.groupby(by=['name']).size()

    summedup = summedup.reset_index(name='count')

    summedup = summedup.loc[summedup['count'] > 5]

    summedup.sort_values(by='count', inplace=True, ascending=False)

    # summedup.to_csv(
    #     utils.change.get_onedrive_path('csv', f'entities-{topic_name}-top-comments.csv'),
    #     encoding='UTF-8', quoting=csv.QUOTE_ALL, index=False)

    summedup = summedup.assign(source=topic_name)
    return summedup


nomask_tagnames = ['unmasking', 'mask choice', 'unmask our kids']
promask_tagnames = ['mask in school', 'mask mandate', 'mask to fight covid-19', 'make mask mandatory']

promask_slugs = set(utils.change.slugs_from_normalized_tag(promask_tagnames))
nomask_slugs = set(utils.change.slugs_from_normalized_tag(nomask_tagnames))

if __name__ == '__main__':
    all_pets = get_all_petitions()

    unmask_analyzed = analyze_comments_from_petition_slugs(nomask_slugs,
                                                           topic_name='unmask',
                                                           petitions_limit=100)
    promask_analyzed = analyze_comments_from_petition_slugs(
        promask_slugs,
        topic_name='promask', petitions_limit=100)

    unmask_analyzed_total = unmask_analyzed['count'].sum()
    promask_analyzed_total = promask_analyzed['count'].sum()

    unmask_analyzed = unmask_analyzed.head(200)
    promask_analyzed = promask_analyzed.head(200)

    unmask_analyzed = unmask_analyzed.assign(
        percentage=lambda x: x['count'] * 100 / unmask_analyzed_total,
        normalized=lambda x: numpy.log10(x['percentage']) / numpy.log10(4)
    )

    promask_analyzed = promask_analyzed.assign(
        percentage=lambda x: x['count'] * 100 / promask_analyzed_total,
        normalized=lambda x: numpy.log10(x['percentage']) / numpy.log10(4)
    )

    unmask_analyzed['normalized'] = unmask_analyzed['normalized'].apply(lambda x: x + 2)
    promask_analyzed['normalized'] = promask_analyzed['normalized'].apply(lambda x: x + 2)

    data = []

    merged = promask_analyzed.merge(unmask_analyzed, left_on='name', right_on='name', suffixes=('_promask', '_nomask'),
                                    how='outer')

    scatter = merged.loc[~merged['normalized_promask'].isna()].loc[~merged['normalized_nomask'].isna()]
    merged.fillna(0, inplace=True)

    merged = merged.assign(percentage_both=lambda x: x['percentage_nomask'] + x['percentage_promask'])
    merged.drop(columns=['source_promask', 'source_nomask'], inplace=True)
    merged.sort_values(by=['percentage_both'], ascending=False, inplace=True)

    utils.google_services.save_list_to_sheets_tab(merged, 'scatternonsopiu')

    scatter = scatter.assign(summed_percentage=lambda x: x['percentage_nomask'] + x['percentage_promask'])
    scatter.sort_values(by='summed_percentage', inplace=True, ascending=False)
    scatter.to_csv(utils.change.get_onedrive_path('csv', 'scatter-boh.csv'), index=False)

    scartate = pandas.concat([
        unmask_analyzed.loc[~unmask_analyzed['name'].isin(scatter['name'])],
        promask_analyzed.loc[~promask_analyzed['name'].isin(scatter['name'])],
    ], ignore_index=True)

    scartate.to_csv(utils.change.get_onedrive_path('csv', 'scatter-scartate.csv'), index=False)

    pandas.concat([promask_analyzed.assign(source='promask'), unmask_analyzed.assign(source='unmask')]).to_csv(
        utils.change.get_onedrive_path('csv', 'entietes_both.csv'))



    fig = px.scatter(scatter, x='normalized_nomask', y='normalized_promask', text='name', width=1500, height=1000)
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
