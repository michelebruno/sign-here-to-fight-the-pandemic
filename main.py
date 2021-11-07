import os.path

from utils.change import get_petitions_by_keyword, count_not_normalized_tags, from_petitions_get_list_of_tags, \
    save_all_petitions, get_all_petitions
import pandas
from dotenv import load_dotenv

# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()


PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

sheet_cleared = False

stored_petitions = []



if __name__ == '__main__':

    keyword = 'covid'


    # save_list_to_sheets_tab(all_tags, 'tantissimitags')

    # all_petitions.drop_duplicates('id', inplace=True)

    list_of_tags = from_petitions_get_list_of_tags(get_all_petitions(), with_id=True)

    # Salvo in CSV
    # pandas.DataFrame(list_of_tags).to_csv('taglist.csv')

    #         if os.environ.get('DOWNLOAD_IMAGES', False):
    #             download_images_from_petitions(petitions, os.path.join('keywords', lang, keyword))
    # store_petitions(all_pets, '')
    # save_list_to_sheets_tab(stored_petitions, 'petitions')
