import os
import pandas
import utils.google_services
from change import petitions

service = utils.google_services.get_service()

_normalized_tags = {}


def get_normalized_tags():
    global _normalized_tags

    if not _normalized_tags:
        normalized = service.spreadsheets().values().get(
            spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='chosen_tags!A2:G').execute()

        normal_rows = normalized.get('values', [])

        _normalized_tags = {}
        for row in normal_rows:
            if len(row) > 6:
                _normalized_tags[row[5].lower()] = row[6]

    return _normalized_tags


def has_tag_been_normalized(tag):
    tags = get_normalized_tags()

    return tag.lower() in tags and tags[tag.lower()] and tags[tag.lower()] != ''


def normalize_tag(tag):
    tags = get_normalized_tags()

    if tag in tags and tags[tag] and tags[tag] != '':
        return tags.get(tag)
    else:
        return tag


def slugs_from_normalized_tag(tag):
    if type(tag) is list:
        slugs = []
        for t in tag:
            slugs.extend(slugs_from_normalized_tag(t))
        return slugs
    slugs = []

    all_tags = service.spreadsheets().values().get(
        spreadsheetId=os.environ.get('PETITION_SPREADSHEET_ID'), range='chosen_tags!A2:G'
    ).execute().get('values', [])

    for row in all_tags:
        if len(row) > 6:
            if row[6] == tag:
                slugs.append(row[4])

    return slugs


def count_tags(petitions: pandas.DataFrame, **kwargs):
    if not isinstance(petitions, pandas.DataFrame):
        petitions = pandas.DataFrame(petitions)

    found_tags = {}

    for index, petition in petitions.iterrows():

        for tag in petition['tag_names']:

            if tag not in found_tags:
                found_tags[tag] = {
                    'name': tag,
                    **kwargs,
                    'total_count': 0,

                }

            found_tags[tag]['total_count'] = found_tags[tag]['total_count'] + 1

    df = pandas.DataFrame([i for k, i in found_tags.items()])

    return df


def count_not_normalized_tags(petitions: pandas.DataFrame, **kwargs):
    if not isinstance(petitions, pandas.DataFrame):
        petitions = pandas.DataFrame(petitions)

    found_tags = {}

    for index, petition in petitions.iterrows():

        for tag in petition['tags']:
            key = tag['slug']

            if key not in found_tags:
                newtag = {
                    **kwargs,
                    **tag,
                    'total_count': 0
                }
                found_tags[key] = newtag

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    df = pandas.DataFrame([i for k, i in found_tags.items()])

    return df


def get_tags_through_keyword(keyword, lang='en-GB', country=None):
    pets = petitions.get_petitions_by_keyword(keyword, lang)

    if country:
        pets = [i for i in pets if i['country'] == country]

    found_tags = {}

    for petition in pets:

        for tag in petition['tags']:
            key = tag['slug']

            if key not in found_tags:
                found_tags[key] = {
                    'total_count': 0,
                    **tag
                }

            found_tags[key]['total_count'] = found_tags[key]['total_count'] + 1

    tags = pandas.DataFrame([i for k, i in found_tags.items()])

    return tags.to_dict('records')


def from_petitions_get_list_of_tags(petitions, normalized: bool = True, only_normalized: bool = True):
    '''

    :param normalized:
    :param petitions:
    :param filename:
    :return:
    '''
    tags = []

    for i, petition in petitions.iterrows():

        t = []

        if normalized:
            for tag in petition['tag_raw_names']:
                if has_tag_been_normalized(tag.lower()) or not only_normalized:
                    t.append(normalize_tag(tag.lower()))
        else:
            for tag in petition['tag_raw_names']:
                t.append(tag)

        for _t in set(t):
            tags.append((petition['id'], _t))

    return tags


nomask_tagnames = ['unmasking', 'mask choice', 'unmask our kids']
promask_tagnames = ['mask in school', 'mask mandate', 'mask to fight covid-19', 'make mask mandatory']

_promask_slugs = None
_nomask_slugs = None


def get_promask_slugs():
    global _promask_slugs

    if _promask_slugs is None:
        _promask_slugs = set(slugs_from_normalized_tag(promask_tagnames))

    return _promask_slugs


def get_nomask_slugs():
    global _nomask_slugs

    if _nomask_slugs is None:
        _nomask_slugs = set(slugs_from_normalized_tag(nomask_tagnames))

    return _nomask_slugs
