import io
import pprint
from tqdm import tqdm
import proto
from google.protobuf.json_format import MessageToJson

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


r = annotate_img_in_dir('work')
