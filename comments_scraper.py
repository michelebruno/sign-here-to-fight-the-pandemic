# SAMPLE REQUEST
# https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

# l'idea migliore che mi è venuta è continuare a fare richieste a blocchi di limit=100, poi offset+=100, poi continuo
# fino a quando last_page == true (last page è la penultima key del json, prima di total count)


from utils.change import get_json_path, get_petition_comments

petition_id = '20861011'

comments = get_petition_comments(28741684)
