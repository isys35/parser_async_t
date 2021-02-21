import asyncio
import aiohttp
from db import DB
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Thread

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
}

# LEN_ASYNC должен быть больше чем THREADS
LEN_ASYNC = 300
timeout = aiohttp.ClientTimeout(total=100)
THREADS = 5


async def fetch_content(url, session):
    try:
        async with session.get(url, headers=HEADERS) as response:
            data = await response.text()
            return data
    except Exception:
        return None


async def req(urls):
    tasks = []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i in range(0, len(urls)):
            task = asyncio.create_task(fetch_content(urls[i], session))
            tasks.append(task)
        data = await asyncio.gather(*tasks)
        return data


def parse_response(response: str):
    soup = BeautifulSoup(response, 'lxml')
    title = soup.find('title')
    if title:
        title = title.text
    description = soup.find('meta', {'name': 'description'})
    if description:
        try:
            description = description['content']
        except KeyError:
            description = ''
    return title, description


def group(iterable, count):
    """ Группировка элементов последовательности по count элементов """
    return [iterable[i:i + count] for i in range(0, len(iterable), count)]


def responses_handler(responses, urls):
    db = DB()
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    data_with_response = []
    for index, response in enumerate(responses):
        if response:
            print('[INFO] Сайт {} response: {}'.format(urls[index], 'True'))
            title, description = parse_response(response)
            data_with_response.append({'website': urls[index].replace('http://', ''),
                                       'title': title,
                                       'description': description,
                                       'date_updated': formatted_date})

        else:
            print('[INFO] Сайт {} response: {}'.format(urls[index], 'False'))
    db.update_many(data_with_response)


def get_responses(urls):
    threads = []
    delta = LEN_ASYNC // THREADS
    group_urls = group(urls, delta)
    for sub_group_url in group_urls:
        thread = MyThread(sub_group_url)
        thread.start()
        threads.append(thread)
    responses = []
    for thread in threads:
        while True:
            if not thread.isAlive():
                responses.extend(thread.responses)
                break
    return responses


def parser(init=False):
    db = DB()
    if init:
        count_domains = db.get_count()
    else:
        count_domains = db.get_null_count()
    offset = 0
    limit = LEN_ASYNC
    count_parsed_domains = 0
    while offset + limit < count_domains:
        if init:
            domains = db.get_domains(limit, offset)
        else:
            domains = db.get_null_domains(limit, offset)
        urls = ['http://{}'.format(domain) for domain in domains]
        responses = get_responses(urls)
        responses_handler(responses, urls)
        offset += limit
        count_parsed_domains += limit
        print('[INFO] {}/{} сайтов'.format(count_parsed_domains, count_domains))


class MyThread(Thread):

    def __init__(self, urls):
        Thread.__init__(self)
        self.urls = urls
        self.responses = None

    def run(self):
        self.responses = asyncio.run(req(self.urls))


if __name__ == '__main__':
    in_info = input('Обновить всю базу (д/н): ')
    if in_info == 'д':
        parser(init=True)
    else:
        parser(init=False)
