import os

import pandas
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

for entity in entities:
    promask_this_entity = promask_comments.loc[promask_comments['comment'].str.contains(entity, case=False)]
    nomask_this_entity = nomask_comments.loc[nomask_comments['comment'].str.contains(entity, case=False)]

    for second_entity in entities:
        pro = promask_this_entity.loc[promask_this_entity['comment'].str.contains(second_entity, case=False)]
        no = nomask_this_entity.loc[nomask_this_entity['comment'].str.contains(second_entity, case=False)]

        promask_percentage = pro.shape[0] / promask_this_entity.shape[0]
        nomask_percentage = no.shape[0] / nomask_this_entity.shape[0]

        delta_promask = promask_percentage / (nomask_percentage + promask_percentage)

        save_this.append([entity, second_entity, no.shape[0], pro.shape[0], nomask_percentage,
                          promask_percentage, (promask_percentage + nomask_percentage) / 2, delta_promask,
                          1 - delta_promask])

save_list_to_sheets_tab(
    pandas.DataFrame(save_this,
                     columns=['word', 'second_word', 'nomask', 'promask', 'nomask_%', 'promaks_%', 'summed_percentage',
                              'promask_delta', 'nomask_delta'], ),
    'distribuzione',
    spreadsheetId='1kacMntgtJC2w9iuCbCACkojX5LdpLzsceR6Sw9hcVHI')
