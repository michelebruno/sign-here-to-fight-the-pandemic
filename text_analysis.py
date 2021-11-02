# GOOGLE TRANSLATE API
from google.cloud import translate_v2 as translate
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from google.cloud import language_v1 as language
# NEEDED TO LOAD THE .ENV FILE WITH GOOGLE API CREDENTIALS IN IT
from dotenv import load_dotenv
# NEEDED TO DECODE UTF8
import six

# Loads google api credentials and nitialises the two clients needed to perform this operation
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
sample_text = "L'ornithorynque (Ornithorhynchus anatinus) est un animal semi-aquatique endémique de l'est de l'Australie et de la Tasmanie. C'est l'une des cinq espèces de l'ordre des monotrèmes, seul ordre de mammifères qui pond des œufs au lieu de donner naissance à des petits complètement formés (les quatre autres espèces sont des échidnés). C'est la seule espèce actuelle de la famille des Ornithorhynchidae et du genre Ornithorhynchus bien qu'un grand nombre de fragments d'espèces fossiles de cette famille et de ce genre aient été découverts. L'apparence fantasmagorique de ce mammifère pondant des œufs, à la mâchoire cornée ressemblant au bec d'un canard, à queue évoquant un castor, qui lui sert à la fois de gouvernail dans l'eau et de réserve de graisse, et à pattes de loutre a fortement surpris les explorateurs qui l'ont découvert ; bon nombre de naturalistes européens ont cru à une plaisanterie. C'est l'un des rares mammifères venimeux2 : le mâle porte sur les pattes postérieures un aiguillon qui peut libérer du venin capable de paralyser une jambe humaine ou même de tuer un chien. Les traits originaux de l'ornithorynque en font un sujet d'études important pour mieux comprendre l'évolution des espèces animales et en ont fait un des symboles de l'Australie : il a été utilisé comme mascotte pour de nombreux événements nationaux et il figure au verso de la pièce de monnaie de 20 cents australiens. Jusqu'au début du xxe siècle, il a été chassé pour sa fourrure mais il est protégé à l'heure actuelle. Bien que les programmes de reproduction en captivité aient eu un succès très limité et qu'il soit sensible aux effets de la pollution, l'espèce n'était pas considérée comme en danger jusque récemment ; depuis 2019, elle est décrite comme « quasi-menacée »."
print(get_analysis(sample_text))
