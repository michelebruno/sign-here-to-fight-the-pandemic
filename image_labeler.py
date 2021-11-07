import io
import pprint

import json
import os
from dotenv import load_dotenv
from google.cloud import vision

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

        for img_name in os.listdir(dir_path):
            slug = img_name[:4]

            print(img_name)
            print(slug)

            if slug in saved_annotations:
                continue

            file_name = os.path.join(dir_path, f'{img_name}')
            # Loads the image into memory
            with io.open(file_name, 'rb') as image_file:
                content = image_file.read()
                image = vision.Image(content=content)

            # Performs label detection on the image file
            response = client.label_detection(image=image)

            labels = response.label_annotations

            saved_annotations[slug] = labels
            print(f'Labels for image {img_name} in folder {dir_name}')
            for label in labels:
                print(label)
        with open(os.path.join(os.environ.get('ONEDRIVE_FOLDER_PATH'), 'json', 'annotations.json'), 'w') as wf:
            json.dump(saved_annotations, wf)
        return labels


annotate_img_in_dir('work')
