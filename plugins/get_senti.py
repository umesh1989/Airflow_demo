import requests

class CommonUtil:
    def __init__(self):
        self.senti_url = 'https://sentim-api.herokuapp.com/api/v1/'
        self.senti_headers = {"Accept": "application/json", "Content-Type": "application/json"}
        self.payload={"text":"{}"}

    def get_sentiment(self,text):
        self.payload["text"] = text
        payload = self.payload
        # print(payload)
        res = requests.post(self.senti_url, json=payload, headers=self.senti_headers)
        if res.status_code == 200:
            jr = res.json()
            # print(jr)
            senti = jr['result']['type']
            # print(senti)
            return senti
        else:
            return 'NA'


if __name__ == "__main__":
    str1="greg koch is a knowledgable and charismatic host  he is seriously fun to watch  the main problem with the video is the format  the lack of on screen tab is a serious flaw  you have to watch very carefully  have a good understanding of the minor pentatonic  and  basic foundation of blues licks to even have a chance at gleening anything from this video if you re just starting out  pick up the in the style of series  while this series has its limitations  incomplete songs due to copyright  no doubt   it has on screen tab and each lick is played at a reasonably slow speed  in addition  their web site has downloadable tab however  if you can hold your own in the minor pentatonic  give this a try  it is quite a workout and you ll find yourself a better player having taken on the challenge"
    cu = CommonUtil()
    # str1 = ''
    senti = cu.get_sentiment(str1)
    print(senti)
