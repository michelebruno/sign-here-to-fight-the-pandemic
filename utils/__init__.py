import os
import re


def get_onedrive_path(*folders):
    return os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), *folders)


def get_json_path(*folders):
    return os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', *folders)


CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext
