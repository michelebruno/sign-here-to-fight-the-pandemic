import os
import re
import nltk
from nltk.stem import WordNetLemmatizer
import itertools
# nltk.download('punkt')
# nltk.download('stopwords')
import pandas
import string
from tqdm import tqdm

from change import petitions, comments
from utils.google_services import get_service, save_list_to_sheets_tab

promask_comments = comments.from_petitions(petitions.get_promask_petitions(100)['id'].to_list())
nomask_comments = comments.from_petitions(petitions.get_nomask_petitions(100)['id'].to_list())

scatter = get_service().spreadsheets().values().get(
    spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='scatter!A2:H').execute().get('values', [])

entities = pandas.DataFrame(scatter)[0].to_list()

limit = 35

entities = entities[:limit]

save_this = []


# pandas.set_option('display.max_columns', None)

def remove_punctuation(df):
    df['comment'] = df['comment'].apply(
        lambda comment: re.sub("[^-9A-Za-z ]", "", str(comment)))
    return df

def normalize_case(df):
    df['comment'] = df['comment'].apply(
        lambda comment: "".join([i.lower() for i in comment if i not in string.punctuation]))
    return df

def remove_stopwords(df):
    df['comment'] = df['comment'].apply(
        lambda comment: [i for i in comment if i not in stopwords])
    return df

def lemmatize_words(df):
    df['comment'] = df['comment'].apply(
        lambda comment: [lemmatizer.lemmatize(i) for i in comment])
    return df


for entity in entities:

    # import the commenti
    promask_this_entity = promask_comments.loc[promask_comments['comment'].str.contains(entity, case=False)]
    nomask_this_entity = nomask_comments.loc[nomask_comments['comment'].str.contains(entity, case=False)]

    # remove punctuation
    promask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: re.sub("[^-9A-Za-z ]", "", str(comment)))
    nomask_this_entity['comment'] = nomask_this_entity['comment'].apply(
        lambda comment: re.sub("[^-9A-Za-z ]", "", str(comment)))

    # normalize case
    promask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: "".join([i.lower() for i in comment if i not in string.punctuation]))
    nomask_this_entity['comment'] = nomask_this_entity['comment'].apply(
        lambda comment: "".join([i.lower() for i in comment if i not in string.punctuation]))

    # tokenize the commenti
    promask_this_entity['comment'] = promask_this_entity['comment'].apply(nltk.word_tokenize)
    nomask_this_entity['comment'] = nomask_this_entity['comment'].apply(nltk.word_tokenize)

    # remove stop words
    stopwords = nltk.corpus.stopwords.words('english')
    promask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: [i for i in comment if i not in stopwords])
    nomask_this_entity['comment'] = nomask_this_entity['comment'].apply(
        lambda comment: [i for i in comment if i not in stopwords])

    #lemmatize words to remove semi-duplicates like "mask" <-> "masks"
    lemmatizer = WordNetLemmatizer()

    promask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: [lemmatizer.lemmatize(i) for i in comment])
    nomask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: [lemmatizer.lemmatize(i) for i in comment])

    # flatten column of lists to column e basta
    promask_list_entity = promask_this_entity.explode('comment')
    nomask_list_entity = promask_this_entity.explode('comment')

    # this is done because .explode() duplicates indexes
    promask_list_entity = promask_list_entity.reset_index(drop=True)
    nomask_list_entity = promask_list_entity.reset_index(drop=True)

    # this is needed because .value_counts() does not hash lists
    promask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: str(comment))
    nomask_this_entity['comment'] = promask_this_entity['comment'].apply(
        lambda comment: str(comment))

    # count how many times a word appears
    promask_wordcount = promask_list_entity['comment'].value_counts().to_frame().reset_index().rename(
        columns={"index": "word", "comment": "count"})
    nomask_wordcount = promask_list_entity['comment'].value_counts().to_frame().reset_index().rename(
        columns={"index": "word", "comment": "count"})

    print(promask_wordcount)

    # DEBUG this is done to avoid the "unhashable type: list" error, non sono sicuro sia questa la soluzione ma intanto è qui
    # promask_this_entity['comment'] = promask_this_entity['comment'].apply(
    #    lambda comment: str(comment))
    # print(promask_list_entity['comment'])

    # Ultimo cerchio dell'inferno dove cerco di contare quante volte appare ogni parola
    # promask_list_entity['counts'] = promask_this_entity.groupby('comment')['comment'].transform('counts')
    # promask_list_entity = promask_list_entity.groupby('comment').size().reset_index(name='counts')

    # print(promask_list_entity[['comment', 'counts']])

    # promask_this_entity_list = promask_this_entity['comment'].to_list()
    # promask_this_entity_flat = list(itertools.chain(*promask_this_entity_list))

    # print(promask_this_entity_flat)

    # print(promask_this_entity['comment'])


    for second_entity in entities:
        # pro = promask_this_entity.loc[promask_this_entity['comment'].str.contains(second_entity, case=False)]
        # no = nomask_this_entity.loc[nomask_this_entity['comment'].str.contains(second_entity, case=False)]

        # promask_percentage = pro.shape[0] / promask_this_entity.shape[0]
        # nomask_percentage = no.shape[0] / nomask_this_entity.shape[0]

        promask_word = promask_wordcount.loc[promask_wordcount['word'] == second_entity]['count']
        promask_tot = promask_wordcount['count'].sum()
        nomask_word = nomask_wordcount.loc[nomask_wordcount['word'] == second_entity]['count']
        nomask_tot = nomask_wordcount['count'].sum()

        promask_percentage = promask_word / promask_tot
        nomask_percentage = nomask_word / nomask_tot

        delta_promask = promask_percentage / (nomask_percentage + promask_percentage)

        save_this.append([entity, second_entity, nomask_word, promask_word, nomask_percentage,
                          promask_percentage, (promask_percentage + nomask_percentage) / 2, delta_promask,
                          1 - delta_promask])

# save_list_to_sheets_tab(
#     pandas.DataFrame(save_this,
#                      columns=['word', 'second_word', 'nomask', 'promask', 'nomask_%', 'promaks_%', 'summed_percentage',
#                               'promask_delta', 'nomask_delta'], ),
#     'distribuzione',
#     spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI')
