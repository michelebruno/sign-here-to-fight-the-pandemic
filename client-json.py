import os
import re

import nltk
import pandas
from tqdm import tqdm

from change import petitions, tags, comments

from utils.google_services import save_list_to_sheets_tab, get_service

save_these_ones = pandas.DataFrame(
    columns=['word', 'origin', 'extracted', 'comment', 'likes', 'petition',
             'author'])

nomask_comments = comments.from_petitions(petitions.get_nomask_petitions(100)['id'].to_list()).sample(
    frac=1).reset_index(drop=True).head(4500).assign(origin='nomask')
promask_comments = comments.from_petitions(petitions.get_promask_petitions(100)['id'].to_list()).sample(
    frac=1).reset_index(drop=True).head(nomask_comments.shape[0]).assign(origin='promask')

scatter = get_service().spreadsheets().values().get(
    spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI', range='scatter!A:L').execute().get('values', [])

entities = pandas.DataFrame(scatter[1::1], columns=scatter[0])

entities = entities.loc[entities['scelta'] == 'X']

words = entities['name'].to_list()

allWordsRegex = re.compile(fr"\b({'|'.join(words)})\b")

promask_comments['sentences'] = promask_comments['comment'] \
    .apply(nltk.tokenize.sent_tokenize) \
    .map(lambda ss: [allWordsRegex.split(s) for s in ss])

nomask_comments['sentences'] = nomask_comments['comment'] \
    .apply(nltk.tokenize.sent_tokenize) \
    .map(lambda ss: [allWordsRegex.split(s) for s in ss])


def flatten_list(_2d_list):
    flat_list = []
    # Iterate through the outer list
    for element in _2d_list:
        if type(element) is list:
            # If the element is of type list, iterate through the sublist
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list


def filter_for_json(comms: pandas.DataFrame, word: str):
    filtered = comms.copy()

    filtered.sentences = filtered.sentences.map(lambda ss: [s for s in ss if word in s])

    filtered = filtered.loc[filtered.sentences.map(bool)]

    return filtered


allComments = pandas.DataFrame()

for word in words:
    comments = pandas.concat([
        filter_for_json(promask_comments, word),
        filter_for_json(nomask_comments, word)
    ])
    comments.user = comments.user.map(lambda u: u['id'])

    comments.insert(1, 'petition', comments.commentable_entity.map(
        lambda p: dict({'title': p['title'], 'slug': p['slug'], 'created_at': p['created_at']})))

    comments.rename(columns={'comment_id': 'id'}, inplace=True)

    comments = comments.assign(word=word)

    allComments = pandas.concat([allComments, comments], ignore_index=True)

allComments = allComments[["origin", "id", "comment", "petition", "user", "sentences"]]

allComments.to_json(os.path.join('..', 'dd-phase03', 'src', 'data', 'comments.json'), orient='records')
