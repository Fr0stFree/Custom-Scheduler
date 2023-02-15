import logging

import requests

from components import Job, Scheduler


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.WARNING)



def download_image(url: str):
    response = requests.get(url)
    name = url.split('/')[-1]
    return name, response.content


if __name__ == '__main__':
    scheduler = Scheduler()

    job = Job(download_image, url='https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png')
    dependant_job = Job(
        download_image,
        url='https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/HNL_Wiki_Wiki_Bus.jpg/220px-HNL_Wiki_Wiki_Bus.jpg',
        dependencies=[job]
    )
    another_dependant_job = Job(
        download_image,
        url='https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/Ward_Cunningham_-_Commons-1.jpg/560px-Ward_Cunningham_-_Commons-1.jpg',
        dependencies=[dependant_job]
    )

    scheduler.schedule(job, dependant_job, another_dependant_job)
    scheduler.stop()
    scheduler = Scheduler.load()
    scheduler.run()

