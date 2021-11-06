import os.path

from utils.change import get_petitions_by_tag, get_petitions_by_keyword, count_tags, count_not_normalized_tags, \
    from_petitions_get_list_of_tags
import pandas
from utils.google_services import get_service, save_list_to_sheets_tab
from dotenv import load_dotenv

# USEFUL LINKS
# https://towardsdatascience.com/how-to-import-google-sheets-data-into-a-pandas-dataframe-using-googles-api-v4-2020-f50e84ea4530

load_dotenv()

service = get_service()

PETITIONS_SPREADSHEET_ID = os.environ.get('PETITION_SPREADSHEET_ID')

sheet_cleared = False

stored_petitions = []

if __name__ == '__main__':
    langs = [
        # 'de-DE',
        # 'en-AU',
        # 'en-CA',
        # 'en-GB',
        # 'en-IN',
        # 'en-US',
        # 'es-AR',
        # 'es-ES',
        # 'id-ID',
        'it-IT',
        # 'ja-JP',
        # 'pt-BR',
        # 'ru-RU',
        # 'th-TH',
        # 'tr-TR',
        # 'hi-IN',
        # 'es-419',
        # 'fr-FR'
    ]

    keyword = 'covid'

    all_tags = pandas.DataFrame([])

    all_petitions = pandas.DataFrame([])

    limit = 50

    for lang in langs:
        print(f"\033[94mLooking for keyword {keyword} in lang {lang}\033[0m")
        petitions = get_petitions_by_keyword(keyword, lang)

        if not len(petitions['items']):
            continue

        print(f"Found {len(petitions['items'])} in keyword {keyword}")

        petitions = pandas.DataFrame(petitions['items'])

        petitions = petitions[petitions['original_locale'] == lang]

        all_petitions = pandas.concat([all_petitions, petitions], ignore_index=True)

        tags = count_not_normalized_tags(petitions, lang=lang)
        tags.sort_values(by='total_count', inplace=True, ascending=False, ignore_index=True)

        all_tags = pandas.concat([tags.head(limit), all_tags], ignore_index=True)

    # save_list_to_sheets_tab(all_tags, 'tantissimitags')

    all_petitions.drop_duplicates('id', inplace=True)

    list_of_tags = from_petitions_get_list_of_tags(all_petitions)

    # Salvo in CSV
    pandas.DataFrame(list_of_tags).to_csv('taglist.csv')

    #         if os.environ.get('DOWNLOAD_IMAGES', False):
    #             download_images_from_petitions(petitions, os.path.join('keywords', lang, keyword))
    # store_petitions(all_pets, '')
    # save_list_to_sheets_tab(stored_petitions, 'petitions')
