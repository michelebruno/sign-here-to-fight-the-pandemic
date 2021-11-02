# GOOGLE TRANSLATE API
from google.cloud import translate_v2 as translate
# GOOGLE NATURAL LANGUAGE API
# this is the api that gives us sentiment, entities and categorisation of the text
from google.cloud import language_v1 as language
 # NEEDED TO LOAD THE .ENV FILE WITH GOOGLE API CREDENTIALS IN IT
from dotenv import load_dotenv

load_dotenv()

sample_text = "Lors de l'extension du Saint-Empire romain germanique vers l'Est, en particulier vers les côtes de la mer Baltique au xiiie siècle, une partie des seigneurs des Obodrites s'allièrent à des chefs allemands. En conséquence, ils renforcèrent leurs territoires. Les plus puissants d'entre eux furent les seigneurs de Mecklembourg."

annotate_features = {
    "extractSyntax": True,
    "extractEntities": True,
    "extractDocumentSentiment": True,
    "extractEntitySentiment": True,
    "classifyText": True
}


def translate_text(target, trans_text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    import six
    translate_client = translate.Client()

    if isinstance(trans_text, six.binary_type):
        trans_text = trans_text.decode("utf-8")

    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    result = translate_client.translate(trans_text, target_language=target)
    return u"{}".format(result["translatedText"])
    # print(u"Text: {}".format(result["input"]))
    # print(u"Translation: {}".format(result["translatedText"]))
    # print(u"Detected source language: {}".format(result["detectedSourceLanguage"]))


analysis_client = language.LanguageServiceClient()

# The text to analyze
text = translate_text("EN", f"{sample_text}")
document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

# Detects the sentiment of the text
text_info = analysis_client.classify_text(request={'document': document}).categories

for item in text_info:
    print(item["name"])
# print("Sentiment: {}, {}".format(sentiment.score, sentiment.magnitude))
