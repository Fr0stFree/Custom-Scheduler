import logging

import requests

from components import Job, Scheduler


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.WARNING)



image_urls = [
    'https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png',
    'https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/HNL_Wiki_Wiki_Bus.jpg/220px-HNL_Wiki_Wiki_Bus.jpg',
    'https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/Ward_Cunningham_-_Commons-1.jpg/560px-Ward_Cunningham_-_Commons-1.jpg',
]


def download_image(url: str):
    response = requests.get(url)
    name = url.split('/')[-1]
    return name, response.content

def save_image(name: str, content: bytes):
    with open(f'images/{name}', 'wb') as f:
        f.write(content)
    return name

def print_result(name: str):
    print(f'Image {name} was saved')



if __name__ == '__main__':
    scheduler = Scheduler()
    for url in image_urls:
        job = Job(download_image, url=url)
        next_job = Job(print_result, name=job, dependencies=[job])
        scheduler.schedule(job)
        scheduler.schedule(next_job)
        
    
    scheduler.run()

