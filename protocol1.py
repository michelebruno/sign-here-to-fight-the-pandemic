import os.path

from utils.change import count_not_normalized_tags, from_petitions_get_list_of_tags, \
    get_all_petitions, filter_only_for_chosen_countries, normalize_tag
import pandas
from dotenv import load_dotenv

# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

sheet_cleared = False

stored_petitions = []

chosen_tags = pandas.read_csv(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'chosen_tags.csv'))

if __name__ == '__main__':

    keyword = 'covid'

    # save_list_to_sheets_tab(all_tags, 'tantissimitags')

    # all_petitions.drop_duplicates('id', inplace=True)
    filtered_by_country = filter_only_for_chosen_countries(get_all_petitions())

    for country, pets in filtered_by_country.groupby('country'):
        country_tags = chosen_tags.loc[chosen_tags['country'] == country]

        country_tags = country_tags.sort_values(by='total_count', ascending=False)

        edges = pandas.DataFrame(
            from_petitions_get_list_of_tags(pets, normalized=True, ),
            columns=['source', 'target'])

        edges = edges.loc[edges['target'].isin(country_tags['normalized'].head(75))]
        #
        edges = edges.loc[~edges['target'].isin(
            ('coronavirus', 'covid', 'covid-19', 'covid-19 epidemic', 'covid-19 pandemic', 'pandemic'))]

        petition_nodes = pandas.DataFrame()

        petition_nodes = petition_nodes.assign(id=edges['source'].unique(), label='',
                                               category='petition')

        tag_nodes = pandas.DataFrame()

        tag_nodes = tag_nodes.assign(id=edges['target'].unique(), label=lambda x: x['id'], category='tag')

        edges.to_csv(
            path_or_buf=os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'python',
                                     f"{country}-edges.csv"),
            index=False)

        pandas.concat([tag_nodes, petition_nodes], ignore_index=True).to_csv(
            path_or_buf=os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'python',
                                     f"{country}-nodes.csv"),
            index=False
        )

    # list_of_tags = from_petitions_get_list_of_tags(get_all_petitions(), with_id=True)

    # Salvo in CSV
    # pandas.DataFrame(list_of_tags).to_csv('taglist.csv', index=False)

    #         if os.environ.get('DOWNLOAD_IMAGES', False):
    #             download_images_from_petitions(petitions, os.path.join('keywords', lang, keyword))
    # store_petitions(all_pets, '')
    # save_list_to_sheets_tab(stored_petitions, 'petitions')
