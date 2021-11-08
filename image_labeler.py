import io
import pprint

import pandas
from tqdm import tqdm
import proto
from google.protobuf.json_format import MessageToJson

import json
import os
from dotenv import load_dotenv
from google.cloud import vision

import utils.change

load_dotenv()
client = vision.ImageAnnotatorClient()


# The name of the image file to annotate file_name = os.path.abspath(r'C:\Users\lucad\OneDrive - Politecnico di
# Milano\Density\scraped-images\normal-tags\work\nicola-zingaretti-vogliamo-rispetto-e-riconoscimento-per-il-lavoro
# -svolto-dai-medici-specializzandi.jpg')

def annotate_img_in_dir(dir_name):
    with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'), 'r') as f:
        saved_annotations = json.load(f)

        dir_name = dir_name
        dir_path = os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'normal-tags', dir_name)

        for img_name in tqdm(os.listdir(dir_path), desc=f"Labelling images in dir {dir_name}"):
            slug = img_name[0:-4]

            if slug in saved_annotations:
                continue

            file_name = os.path.join(dir_path, f'{img_name}')
            # Loads the image into memory
            with io.open(file_name, 'rb') as image_file:
                content = image_file.read()
                image = vision.Image(content=content)

            # Performs label detection on the image file
            response = client.label_detection(image=image)

            labels = [proto.Message.to_dict(tag) for tag in response.label_annotations]

            saved_annotations[slug] = labels

    with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'), 'w') as wf:
        json.dump(saved_annotations, wf)
    return saved_annotations


def annotate_images_from_petitions(petitions: pandas.DataFrame):
    with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'), 'r') as f:
        saved_annotations = json.load(f)

        last_saved = 0

        for i, pet in tqdm(petitions.iterrows(), total=petitions.shape[0],
                           desc=f"Labelling images from list of petitions"):
            slug = pet['slug']

            if slug in saved_annotations:
                continue

            try:
                image = vision.Image()

                img_url = str(f"https:{pet['photo']['sizes']['large']['url']}")

                image.source.image_uri = img_url

                # Performs label detection on the image file
                response = client.label_detection(image=image)

                labels = [proto.Message.to_dict(tag) for tag in response.label_annotations]

                saved_annotations[slug] = labels

                if last_saved - i > 50:
                    with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'),
                              'w') as wf:
                        json.dump(saved_annotations, wf)
                        last_saved = i
            except TypeError:
                continue

    with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'), 'w') as wf:
        json.dump(saved_annotations, wf)
    return saved_annotations


if __name__ == '__main__':
    all_pets = utils.change.get_all_petitions()

    chosen_country = utils.change.filter_only_for_chosen_countries(all_pets)

    for country, pets in chosen_country.groupby(by='country'):
        print(f"Annotating pics for country {country}")
        annotate_images_from_petitions(pets)
