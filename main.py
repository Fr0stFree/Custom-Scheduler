from components import Job, Scheduler
import requests

image_urls = [
    'https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png',
    'https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/HNL_Wiki_Wiki_Bus.jpg/220px-HNL_Wiki_Wiki_Bus.jpg',
    'https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/Ward_Cunningham_-_Commons-1.jpg/560px-Ward_Cunningham_-_Commons-1.jpg',
]


def download_images(urls: list[str]) -> list[bytes]:
    for url in urls:
        response = requests.get(url)
        name = url.split('/')[-1]
        with open(f'{name}', 'wb') as f:
            f.write(response.content)
        print(f'Downloaded {name}')
        yield
        
def print_numbers():
    for i in range(10):
        print(i)
        yield



if __name__ == '__main__':
    scheduler = Scheduler()
    # scheduler.schedule(Job(download_images, args=(image_urls,)))
    scheduler.schedule(Job(print_numbers))
    scheduler.run()

