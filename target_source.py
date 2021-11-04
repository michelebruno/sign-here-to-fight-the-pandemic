import pandas

from utils.change import get_normalized_tags, get_all_petitions


def save_target_source(petitions=get_all_petitions()):
    target_source = []

    for i, p in petitions.iterrows():
        for t in p['tag_names']:
            target_source.append({
                'source': p['slug'],
                'target': t
            })

    allowed = set([p for i, p in get_normalized_tags().items()])

    target_source = [item for item in target_source if item['target'] in allowed]

    df = pandas.DataFrame(target_source)

    df.to_csv('target_source.csv', index=False, encoding='utf-8')


if __name__ == '__main__':
    save_target_source()
