import logging
import datetime as dt
import random
import time

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

def download_image_error(url: str):
    time.sleep(5)
    raise TimeoutError('Timeout')


if __name__ == '__main__':
    scheduler = Scheduler()
    # for url in image_urls:
    #     job = Job(download_image, url=url, start_at=dt.datetime.now() + dt.timedelta(seconds=1), tries=3)
    #     scheduler.schedule(job)
    job = Job(download_image, url=image_urls[0])
    dependant_job = Job(download_image, url=image_urls[1], dependencies=[job])
    another_dependant_job = Job(download_image, url=image_urls[2], dependencies=[dependant_job])
    scheduler.schedule(job, dependant_job, another_dependant_job)
    
    scheduler.run()

