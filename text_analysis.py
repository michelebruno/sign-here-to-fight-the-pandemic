# GOOGLE TRANSLATE API
from google.cloud import translate_v2 as translate
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from google.cloud import language_v1 as language
# NEEDED TO LOAD THE .ENV FILE WITH GOOGLE API CREDENTIALS IN IT
from dotenv import load_dotenv
# NEEDED TO DECODE UTF8
import six
import pprint
import pandas as pd
import csv

# Loads google api credentials and initialises the two clients needed to perform this operation
from tqdm import tqdm

load_dotenv()
analysis_client = language.LanguageServiceClient()
translate_client = translate.Client()



# Translates text to English
# Source language is detected automatically, translation is needed to use "classifyText" request
def translate_text(source_text):
    if isinstance(source_text, six.binary_type):
        source_text = source_text.decode("utf-8")

    # Target must be an ISO 639-1 language code.
    # See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    result = translate_client.translate(source_text, target_language="en")

    return u"{}".format(result["translatedText"])


def analyse_text(translated_text):
    request = {
        "document": {
            "type_": language.Document.Type.PLAIN_TEXT,
            "language": "en",
            "content": translated_text,
        },
        "features": {
            "extract_syntax": True,
            "extract_entities": True,
            "extract_document_sentiment": True,
            "extract_entity_sentiment": True,
            "classify_text": True
        },
        "encoding_type": "UTF8"
    }

    text_analysis = analysis_client.annotate_text(request=request)
    return text_analysis


def get_analysis(input_text):
    translated_text = translate_text(f"{input_text}")
    text_analysis = analyse_text(translated_text)
    return text_analysis


r"""
    ^ Attributes of text_analysis ^ :
        sentences (Sequence[google.cloud.language_v1.types.Sentence]):
            Sentences in the input document. Populated if the user
            enables
            [AnnotateTextRequest.Features.extract_syntax][google.cloud.language.v1.AnnotateTextRequest.Features.extract_syntax].
        tokens (Sequence[google.cloud.language_v1.types.Token]):
            Tokens, along with their syntactic information, in the input
            document. Populated if the user enables
            [AnnotateTextRequest.Features.extract_syntax][google.cloud.language.v1.AnnotateTextRequest.Features.extract_syntax].
        entities (Sequence[google.cloud.language_v1.types.Entity]):
            Entities, along with their semantic information, in the
            input document. Populated if the user enables
            [AnnotateTextRequest.Features.extract_entities][google.cloud.language.v1.AnnotateTextRequest.Features.extract_entities].
        document_sentiment (google.cloud.language_v1.types.Sentiment):
            The overall sentiment for the document. Populated if the
            user enables
            [AnnotateTextRequest.Features.extract_document_sentiment][google.cloud.language.v1.AnnotateTextRequest.Features.extract_document_sentiment].
        language (str):
            The language of the text, which will be the same as the
            language specified in the request or, if not specified, the
            automatically-detected language. See
            [Document.language][google.cloud.language.v1.Document.language]
            field for more details.
        categories (Sequence[google.cloud.language_v1.types.ClassificationCategory]):
            Categories identified in the input document.
    """

# Used to debug, delete later
sample_text = "L'ornithorynque (Ornithorhynchus anatinus) est un animal semi-aquatique end??mique de l'est de l'Australie et de la Tasmanie. C'est l'une des cinq esp??ces de l'ordre des monotr??mes, seul ordre de mammif??res qui pond des ??ufs au lieu de donner naissance ?? des petits compl??tement form??s (les quatre autres esp??ces sont des ??chidn??s). C'est la seule esp??ce actuelle de la famille des Ornithorhynchidae et du genre Ornithorhynchus bien qu'un grand nombre de fragments d'esp??ces fossiles de cette famille et de ce genre aient ??t?? d??couverts. L'apparence fantasmagorique de ce mammif??re pondant des ??ufs, ?? la m??choire corn??e ressemblant au bec d'un canard, ?? queue ??voquant un castor, qui lui sert ?? la fois de gouvernail dans l'eau et de r??serve de graisse, et ?? pattes de loutre a fortement surpris les explorateurs qui l'ont d??couvert ; bon nombre de naturalistes europ??ens ont cru ?? une plaisanterie. C'est l'un des rares mammif??res venimeux2 : le m??le porte sur les pattes post??rieures un aiguillon qui peut lib??rer du venin capable de paralyser une jambe humaine ou m??me de tuer un chien. Les traits originaux de l'ornithorynque en font un sujet d'??tudes important pour mieux comprendre l'??volution des esp??ces animales et en ont fait un des symboles de l'Australie : il a ??t?? utilis?? comme mascotte pour de nombreux ??v??nements nationaux et il figure au verso de la pi??ce de monnaie de 20 cents australiens. Jusqu'au d??but du xxe si??cle, il a ??t?? chass?? pour sa fourrure mais il est prot??g?? ?? l'heure actuelle. Bien que les programmes de reproduction en captivit?? aient eu un succ??s tr??s limit?? et qu'il soit sensible aux effets de la pollution, l'esp??ce n'??tait pas consid??r??e comme en danger jusque r??cemment ; depuis 2019, elle est d??crite comme ?? quasi-menac??e ??."
#print(get_analysis(sample_text))


csv_path = r'C:\Users\lucad\OneDrive - Politecnico di Milano\Density\scraped-images\domanda 3 - mondello\data.csv'
out_path = r'C:\Users\lucad\Desktop\mondello.csv'

def text_analysis_from_csv(csv_path):
    # Semicolon because excel is a son of a bitch, #todo: change ; to ,
    df = pd.read_csv(csv_path, delimiter=';')

    entities = []
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        source_text=row['txt']
        document = language.types.Document(content=source_text, type_=language.Document.Type.PLAIN_TEXT)
        response = analysis_client.analyze_entities(document=document, encoding_type='UTF8')

        for entity in response.entities:
            entities.append({'txt': source_text, 'name': entity.name, 'type': str(entity.type_)[5:]})

    pd.DataFrame(entities).to_csv(out_path, index=True, header=True, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)


