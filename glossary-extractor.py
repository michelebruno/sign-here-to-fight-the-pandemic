import os

import nltk
import pandas
from tqdm import tqdm

from change import petitions, tags, comments

from utils.google_services import save_list_to_sheets_tab, get_service

save_these_ones = pandas.DataFrame(
    columns=['word', 'origin', 'extracted', 'comment', 'likes', 'petition_link', 'author'])

promask_comments = comments.from_petitions(petitions.get_promask_petitions(100)['id'].to_list())
nomask_comments = comments.from_petitions(petitions.get_nomask_petitions(100)['id'].to_list())
promask_comments['sentences'] = promask_comments['comment'].apply(nltk.tokenize.sent_tokenize)
nomask_comments['sentences'] = nomask_comments['comment'].apply(nltk.tokenize.sent_tokenize)

scatter = get_service().spreadsheets().values().get(
    spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='scatter!A2:H').execute().get('values', [])

entities = pandas.DataFrame(scatter)

for _i, entity in tqdm(entities.iterrows(), total=entities.shape[0]):
    if _i > 10:
        break

    word = entity[0]

    save_these_ones = pandas.concat([save_these_ones,
                                     comments.extract_for_glossary(promask_comments, word=word, origin='promask').sort_values(by='likes', ascending=False).head(100),
                                     comments.extract_for_glossary(nomask_comments, word=word, origin='nomask').sort_values(by='likes', ascending=False).head(100)
                                     ])

save_list_to_sheets_tab(comments.flatten(save_these_ones), 'estratti',
                        spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI')
