import pandas

from tags_per_country import all_pets
from utils.change import tags_to_normal


target_source = []

for i, p in all_pets.iterrows():
    for t in p['tag_names']:
        target_source.append({
            'source': p['slug'],
            'target': t
        })
df = pandas.DataFrame(target_source)
allowed= [p for i, p in tags_to_normal.items()]
df.to_csv('target_source.csv', index=False, encoding='utf-8')
