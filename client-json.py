import os
import re
from datetime import datetime

import nltk
import pandas

from change import petitions, tags, comments

from utils.google_services import get_service

homeCommentsId = [812047727, 811710031, 814486694, 822518213, 817756846, 821382262]

save_these_ones = pandas.DataFrame(
    columns=['word', 'origin', 'extracted', 'comment', 'likes', 'petition', 'author'])

scatter = get_service().spreadsheets().values().get(
    spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI', range='scatter!A:L').execute().get('values', [])

entities = pandas.DataFrame(scatter[1::1], columns=scatter[0])

entities = entities.loc[entities['scelta'] == 'X']

words = entities['name'].to_list()

allWordsRegex = re.compile(fr"\b({'|'.join(words)})\b")

nomask_comments = comments.from_petitions(petitions.get_nomask_petitions(100)['id'].to_list()).assign(origin='nomask')
promask_comments = comments.from_petitions(petitions.get_promask_petitions(100)['id'].to_list()).assign(
    origin='promask')

promask_comments['sentences'] = promask_comments['comment'] \
    .apply(nltk.tokenize.sent_tokenize) \
    .map(lambda ss: [allWordsRegex.split(s) for s in ss])

nomask_comments['sentences'] = nomask_comments['comment'] \
    .apply(nltk.tokenize.sent_tokenize) \
    .map(lambda ss: [allWordsRegex.split(s) for s in ss])


def parse(_df: pandas.DataFrame):
    '''

    :param _df:
    :return:
    :rtype: pandas.DataFrame
    '''
    df = _df.copy()
    df.created_at = df.created_at.map(lambda d: datetime.strftime(d, "%Y-%m-%d"))

    df.user = df.user.map(lambda u: u['id'])

    df.insert(1, 'petition', df.commentable_entity.map(
        lambda p: dict({'title': p['title'], 'slug': p['slug'], 'created_at': p['created_at']})))
    return df[["origin", "id", "comment", "petition", "user", "sentences", "created_at", "showInHome"]]


homeComments = pandas.concat([nomask_comments, promask_comments], ignore_index=True)

homeComments = homeComments.loc[homeComments.id.isin(homeCommentsId)]

homeComments = homeComments.assign(showInHome=True)

allComments = parse(pandas.concat([
    homeComments,
    nomask_comments,
    promask_comments.sample(frac=1).reset_index(drop=True).head(nomask_comments.shape[0])
], ignore_index=True))

allComments = allComments.drop_duplicates(subset=['id'])

allComments.rename(columns={'id': 'commentId', 'created_at': 'createdAt'}, inplace=True)

allComments.to_json(os.path.join('..', 'dd-phase03', 'src', 'data', 'comments.json'), orient='records')
