# SAMPLE REQUEST
# https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

# l'idea migliore che mi è venuta è continuare a fare richieste a blocchi di limit=100, poi offset+=100, poi continuo
# fino a quando last_page == true (last page è la penultima key del json, prima di total count)
import csv

import utils.change
from utils.change import get_json_path, get_petition_comments, get_all_petitions
from utils.google_services import save_list_to_sheets_tab

all_pets = get_all_petitions()

us_pets = all_pets.loc[all_pets['country'] == 'US']

tags = ['unmasking', 'mask choice', 'unmask our kids']

slugs = set(utils.change.slugs_from_normalized_tag(tags))

print(slugs)

filtered_pets = us_pets.loc[us_pets['tag_slugs'].map(lambda x: True if set(x).intersection(slugs) else False)]
# save_list_to_sheets_tab(utils.change.flatten_petitions(filtered_pets), 'unmask')
top_pets = filtered_pets.sort_values(by='total_signature_count', ascending=False).head(50)

comments = get_petition_comments(top_pets['id'].tolist())

comments.drop(columns=['user', 'commentable_entity']).to_csv(utils.change.get_onedrive_path('csv', 'unmask-top50-comments.csv'),
                encoding='UTF-8', quoting=csv.QUOTE_ALL)
