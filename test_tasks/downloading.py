import requests





def download_image(url: str):
    response = requests.get(url)
    name = url.split('/')[-1]
    return name, response.content

