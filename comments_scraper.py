#SAMPLE REQUEST
#https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

#l'idea migliore che mi è venuta è continuare a fare richieste a blocchi di limit=100, poi offset+=100, poi continuo
#fino a quando last_page == true (last page è la penultima key del json, prima di total count)
from utils.http import http

petition_id = '20861011'

def get_petition_comments(petition_id):
    is_last = False
    offset = 0
    limit = 100

    while not is_last:
        #il while loop che aumenta di uno è mostruosamente lento obv ma altrimenti dobbiamo indovinare
        #quanti ne mancano. In alternativa incrementiamo di 100 ogni loop fino a risposta negativa,
        #poi incrementiamo di 1 fino a nuova risposta negativa

        base_url = r'https://www.change.org/api-proxy/-/comments'

        res = http.get(
            f"{base_url}?limit={limit}&offset={offset}&commentable_type=Event&commentable_id={petition_id}").json()

        is_last = bool(res['last_page'])

        if not is_last:
            offset += 100

        for comment in res['items']:
            print(comment['comment'])
        print(offset, 'sto nel main loop zì')


    if is_last:
        is_last = False
        limit = 1
        while not is_last:
            res = http.get(
                f"{base_url}?limit={limit}&offset={offset}&commentable_type=Event&commentable_id={petition_id}").json()

            offset += 1
            is_last = bool(res['last_page'])
            print(res['items'])
            print('sto nel secondary loop')


get_petition_comments(petition_id)

