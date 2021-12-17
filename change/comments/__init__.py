import os

import nltk
import pandas
from tqdm import tqdm
from yaspin import yaspin

import utils
from change import petitions, tags
from utils import get_json_path
from utils.google_services import save_list_to_sheets_tab
from utils.http import http

# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from google.cloud import language_v1 as language
import google.api_core.exceptions

analysis_client = language.LanguageServiceClient()


def from_petitions(petition_id):
    if type(petition_id) is list:
        all_comments = pandas.DataFrame()

        for id in petition_id:
            comms = from_petitions(id)
            all_comments = pandas.concat([all_comments, comms], ignore_index=True)
        return all_comments

    json_filename = get_json_path('comments', f"{petition_id}.json")

    if os.path.exists(json_filename) > 0:
        # print("Got from chache.")
        return pandas.read_json(json_filename)

    is_last = False
    offset = 0
    limit = 10

    comments = []
    with yaspin(
            text=f"Looking for comments in petition {petition_id}") as spinner:
        while not is_last:
            # il while loop che aumenta di uno è mostruosamente lento obv ma altrimenti dobbiamo indovinare
            # quanti ne mancano. In alternativa incrementiamo di 100 ogni loop fino a risposta negativa,
            # poi incrementiamo di 1 fino a nuova risposta negativa

            base_url = r'https://www.change.org/api-proxy/-/comments'

            res = http.get(
                f"{base_url}?limit={limit}&offset={offset}&commentable_type=Event&commentable_id={petition_id}").json()

            offset += limit

            is_last = bool(res['last_page'])

            comments.extend(res['items'])

        spinner.ok(f"Scraped {len(comments)} comments in petition {petition_id}")

        comments_df = pandas.DataFrame(comments)

        comments_df.to_json(json_filename)
        return comments_df


def flatten(comments: pandas.DataFrame):
    df = comments.copy()

    df['created_at'] = df['created_at'].map(lambda x: x.strftime('%Y-%m-%D'))
    df.sort_values(by=['likes'], inplace=True, ascending=False)
    df.drop_duplicates('id', inplace=True)

    df = df.assign(
        petition_link=lambda x: x['commentable_entity'].map(
            lambda y: f"https://www.change.org/p/{y['slug']}"),
        petition_slug=lambda x: x['commentable_entity'].map(
            lambda y: y['slug']),
        petition_title=lambda x: x['commentable_entity'].map(
            lambda y: y['slug']),
        author=lambda x: x['user'].map(
            lambda y: y['display_name'])
    )

    df.drop(columns=['commentable_entity', 'user', 'deleted_at', 'parent_id'], inplace=True)

    df['comment'] = df['comment'].apply(utils.cleanhtml)

    return df


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
    ('opinions', 'opinion'),
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


def get_comments_from_petition_slugs(slugs, topic_name, country=None, petitions_limit=None):
    print(f"Analyzing petition comments about {topic_name}")

    all_pets = petitions.get()

    filtered_pets = all_pets.loc[all_pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]

    if country is not None:
        filtered_pets = all_pets.loc[all_pets['country'] == 'US']
    top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False)

    if petitions_limit:
        top_pets = top_pets.head(petitions_limit)

    return from_petitions(top_pets['id'].tolist())


def analyze_comments_from_petition_slugs(slugs, topic_name, country=None, petitions_limit=None):
    comments = get_comments_from_petition_slugs(slugs, topic_name, country, petitions_limit)

    save_list_to_sheets_tab(flatten(comments), f"commenti-{topic_name}")

    analysed_results = pandas.DataFrame()

    for petition_id, comms in tqdm(comments.groupby('commentable_id'), colour='green'):
        analysed_results = pandas.concat([analysed_results, analyze_comments(comms, petition_id)], ignore_index=True)

    analysed_results = analysed_results.loc[~analysed_results['type'].isin(['NUMBER'])]
    analysed_results = analysed_results.assign(comment_hash=lambda p: p['comment'].map(hash))

    analysed_results['name'] = analysed_results['name'].str.lower()

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


def analyze_comments(comments, petition_id):
    jsonpath = utils.get_json_path('comment-analysis', f"{petition_id}-analysis.json")

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
    jsonpath = utils.get_json_path('comment-analysis', f"{petition_id}-analysis.json")

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


def extract_for_glossary(comments: pandas.DataFrame, word, origin):
    """

    :param comments:
    :param word:
    :param origin:
    :return:
    :rtype: pandas.DataFrame
    """
    comments = comments.copy()

    if 'sentences' not in comments.columns.tolist():
        comments['sentences'] = comments['comment'].apply(nltk.tokenize.sent_tokenize)

    comments['sentences'] = comments['sentences'].apply(
        lambda x: [s for s in x if word in [w.lower() for w in nltk.tokenize.word_tokenize(s)]])

    comments = comments.loc[comments['sentences'].apply(lambda x: bool(len(x)))]

    comments['extracted'] = comments['sentences'].apply(lambda x: ' [...] '.join(x))

    comments.drop(columns='sentences', inplace=True)

    # TODO search also for replaced
    _replaced = [f for f, r in find_replace if r == word]

    return comments.assign(origin=origin, word=word)
