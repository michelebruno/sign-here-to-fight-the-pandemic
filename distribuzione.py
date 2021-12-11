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

#Aggiunto parole usate solo da uno dei due gruppi su gentile richiesta della Dott.ssa Roncalli in data 11 Dicembre 2021
entities_addendum = ["god", "faces", "america", "studies", "bacteria", "metal health", "development", "oxygen", "viruses", "anxiety", "issues", "effects", "headaches", "flu", "abuse"
"texas", "public", "variant", "economy", "businesses", "common sense", "distancing", "hospitals", "guidelines", "texans", "customers", "employees", "delta", "precaution", "physician"]
entities.extend(entities_addendum)

print(entities)

save_this = []


# pandas.set_option('display.max_columns', None)
find_replace = [
    ('mask', 'masks'),
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
    #lemmatizer = WordNetLemmatizer()

    #promask_this_entity['comment'] = promask_this_entity['comment'].apply(
    #    lambda comment: [lemmatizer.lemmatize(i) for i in comment])
    #nomask_this_entity['comment'] = nomask_this_entity['comment'].apply(
    #    lambda comment: [lemmatizer.lemmatize(i) for i in comment])

    # flatten column of lists to column e basta
    promask_list_entity = promask_this_entity.explode('comment')
    nomask_list_entity = nomask_this_entity.explode('comment')

    # this is done because .explode() duplicates indexes
    promask_list_entity = promask_list_entity.reset_index(drop=True)
    nomask_list_entity = nomask_list_entity.reset_index(drop=True)

    # this is needed because .value_counts() does not hash lists
    promask_list_entity['comment'] = promask_list_entity['comment'].apply(
        lambda comment: str(comment))
    nomask_list_entity['comment'] = nomask_list_entity['comment'].apply(
        lambda comment: str(comment))

    for (find, replace) in find_replace:
        promask_list_entity.replace(to_replace=find, value=replace, inplace=True)
        nomask_list_entity.replace(to_replace=find, value=replace, inplace=True)

    print(promask_list_entity['comment'])

    # count how many times a word appears
    promask_wordcount = promask_list_entity['comment'].value_counts().to_frame().reset_index().rename(
        columns={"index": "word", "comment": "count"})
    nomask_wordcount = nomask_list_entity['comment'].value_counts().to_frame().reset_index().rename(
        columns={"index": "word", "comment": "count"})

    #print(promask_wordcount)

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
        #print(entity, second_entity)
        # pro = promask_this_entity.loc[promask_this_entity['comment'].str.contains(second_entity, case=False)]
        # no = nomask_this_entity.loc[nomask_this_entity['comment'].str.contains(second_entity, case=False)]

        # promask_percentage = pro.shape[0] / promask_this_entity.shape[0]
        # nomask_percentage = no.shape[0] / nomask_this_entity.shape[0]
        #print(promask_wordcount)

        try:
            promask_word = promask_wordcount.loc[promask_wordcount['word'] == second_entity]['count'].values[0]
        except IndexError:
            promask_word = 0


        try:
            nomask_word = nomask_wordcount.loc[nomask_wordcount['word'] == second_entity]['count'].values[0]
        except IndexError:
            nomask_word = 0

        promask_tot = promask_wordcount['count'].sum()
        nomask_tot = nomask_wordcount['count'].sum()

        promask_percentage = promask_word / promask_tot
        nomask_percentage = nomask_word / nomask_tot

        delta_promask = promask_percentage / (nomask_percentage + promask_percentage)

        save_this.append([entity, second_entity, nomask_word, promask_word, nomask_percentage,
                          promask_percentage, (promask_percentage + nomask_percentage) / 2, delta_promask,
                          1 - delta_promask])

save_list_to_sheets_tab(
    pandas.DataFrame(save_this,
                     columns=['word', 'second_word', 'nomask', 'promask', 'nomask_%', 'promaks_%', 'summed_percentage',
                              'promask_delta', 'nomask_delta'], ),
    'distribuzione_v2',
    spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI')
