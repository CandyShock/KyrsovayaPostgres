import requests


def get_request(url):
    data = requests.get(url)
    return data.json()



