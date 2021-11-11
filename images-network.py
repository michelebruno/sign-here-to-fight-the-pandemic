import os
import pandas

from utils.google_services import get_service
import utils.change

values = get_service().spreadsheets().values().get(
    spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='unmask_netwok!A2:F').execute().get('values', [])

edges = []

nodes = []

for (url, slug, estetica, *subjects) in values:
    nodes.append({
        'id': slug,
        'label': '',
        'category': 'petition',
        'image': f"{slug}.jpg"
    })

    for sub in subjects:
        if sub and sub != '':
             edges.append({'source': slug, 'target': sub})

edges = pandas.DataFrame(edges)

petition_nodes = pandas.DataFrame(nodes)

tag_nodes = pandas.DataFrame()

tag_nodes = tag_nodes.assign(id=edges['target'].unique(), label=lambda x: x['id'], category='subject')

edges.to_csv(utils.change.get_onedrive_path('csv', 'images-network', 'edges.csv'), index=False)

pandas.concat([petition_nodes, tag_nodes], ignore_index=True).to_csv(
    utils.change.get_onedrive_path('csv', 'images-network', 'nodes.csv'), index=False)
