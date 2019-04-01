import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime as dt
from datetime import timedelta
import os
from urllib.request import Request, urlopen
import random
from multiprocessing import Pool, TimeoutError
import time as timef
from pathlib import Path

# proxies = []  # Will contain proxies [ip, port]

'''
1) Выясняем число страниц                                       ---DONE---
2) Сформировываем список ссылок на каждую страницу поиска       ---DONE---
3) Собираем данные с каждой страницы                            ---DONE---
4) Настраиваем прокси, чтобы не было бана                       ---DONE---
'''

headers = {
    'Accept - Language': 'ru - RU, ru; q = 0.9, en - US; q = 0.8, en; q = 0.7',
    'Host': 'kazan.cian.ru',
    'Referer': 'https://cian.ru',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.96 Safari/537.36'
}

number_of_ads = 0
proxy_check = 1
user_agents = [
    'Mozilla/5.0 (en-us) AppleWebKit/534.14 (KHTML, like Gecko; Google Wireless Transcoder) Chrome/9.0.597 Safari/534.14',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.3',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 6.0.1; SM-J700M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.80 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 8.1.0; Moto G (5)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.80 Mobile Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0']
good_proxies = []


def get_from_area(area):
    if area == 'Avia':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=258&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Vahit':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=259&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Kirov':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=260&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Moscow':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=261&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Novo':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=262&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Privol':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=263&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'
    if area == 'Sovet':
        url = 'https://kazan.cian.ru/cat.php?deal_type=sale&district%5B0%5D=264&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&wp=1'

    numb_of_pages = get_numb_of_pages(url)
    url_split = url.split('p=1')

    # full_url = url_split[0]+'p='+str(numb_of_pages)+url_split[1]
    prox = '0'
    headers = '0'
    for i in range(1, numb_of_pages + 1):  # numb_of_pages + 1):
        cure_url = url_split[0] + 'p=' + str(i) + url_split[1]
        print("Страница " + str(i) + ':')
        attemts = 0
        timef.sleep(random.randint(10, 15))
        while True:
            try:
                html = get_html(cure_url, prox, headers)
                get_data_from_html(html, area)
                break
            except:
                print("Ошибка в get_from_area (главный метод) при получении содержимого страницы. Меняем прокси")
                prox = connect_to_good_proxy()
                headers = {'User-Agent': random_ua()}
                # прокси меняется в get_html

    old = []
    new = []

    with open('.\\Data\\Old\\old' + area + '.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for data in reader:
            old.append(data)
        # print(old)
    with open('.\\Data\\Cure\\' + area + '.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for data in reader:
            new.append(data)

    temp = False
    n = 0
    for i in old:
        temp = True
        for j in new:
            # print(i)
            # print(j)
            if i[0] == j[0]:
                temp = False
                break
        if temp == True:
            n = n + 1
            print('Удаленное ' + str(n) + ': ' + str(i))
            tSplit = i[3].split(':')
            oldDate = dt(2019, int(tSplit[1]), int(tSplit[0]), int(tSplit[2]), int(tSplit[3]))
            curDate = dt.now()
            raznicaDate = str(curDate - oldDate)
            temp = raznicaDate.split(' days, ')
            if temp != '':
                temp2 = temp[1].split(':')
                time = temp[0] + ':' + temp2[0] + ':' + temp2[1]
            else:
                time = '0' + ':' + temp[0] + ':' + temp[1]

            data = {
                'ad_url': i[0],
                'square': i[1],
                'price': i[2],
                'time': time
            }
            write_csv(data, 'Del\\' + area)

    os.rename('.\\Data\\Old\\old' + area + '.csv',
              '.\\Data\\Old\\Archive\\' + 'old' + area + '_' + str(dt.now()).replace(":", "_") + '.csv')
    os.rename('.\\Data\\Cure\\' + area + '.csv', '.\\Data\\Old\\' + 'old' + area + '.csv')
    # SystemExit(1)


def random_ua():
    global user_agents
    return user_agents[random.randint(0, len(user_agents) - 1)]


def get_proxies():
    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', random_ua())
    proxies_doc = urlopen(proxies_req).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    proxies=[]
    for row in proxies_table.tbody.find_all('tr'):
        proxies.append("https://" + row.find_all('td')[0].string + ':' + row.find_all('td')[1].string)
    return proxies


def get_good_proxies():
    proxy = ''
    attemts = 0
    proxies = get_proxies()
    for current_proxy in proxies:
        # ua = UserAgent()
        headers = {'User-Agent': random_ua()}
        s = requests.session()
        delay_time = 15  # delay time in seconds
        try:
            r = s.post('https://cian.ru/', proxies=proxy, headers=headers, timeout=15)
        except:
            proxies.remove(current_proxy)
    global good_proxies
    good_proxies = proxies


def connect_to_good_proxy():
    global good_proxies
    if len(good_proxies) != 0:
        prox = good_proxies[random.randint(0, len(good_proxies) - 1)]
        proxyDict = {
            "https": prox
        }
        return proxyDict
    else:
        get_good_proxies()
        if len(good_proxies) != 0:
            prox = good_proxies[random.randint(0, len(good_proxies) - 1)]
            proxyDict = {
                "https": prox
            }
            return proxyDict
        else:
            raise ValueError("Нет прокси")


def get_html(url, curr_proxy, prox_headers):
    global headers
    code = ''
    s = requests.session()
    # while True:
    #    try:
    if curr_proxy == '0':
        r = s.get(url, headers=headers)
        code = r.text
    else:
        r = s.get(url, headers=prox_headers, proxies=curr_proxy)
        code = r.text
    #       break
    #   except:
    #    curr_proxy = connect_to_good_proxy()
    # print("В get_html неполадки ua либо proxy - меняем прокси")
    return code


def get_numb_of_pages(url):  # перебираем ссылки от n-страницы, пока не дойдем до существубщей страницы
    global curr_proxy
    url_split = url.split(
        'p=1')  # (если страница не существует, то нас перебрасывает на 1 страницу, что и проверяется в этом цикле)
    pages = ''
    n = int(61)
    cure_page = 0
    print("Подсчет числа страниц...")
    prox = '0'
    headers = '0'
    while True:
        try:
            while n != cure_page:
                n = n - 1
                html = get_html(url_split[0] + 'p=' + str(n) + url_split[1], prox, headers)
                soup = BeautifulSoup(html, 'lxml')

                pages = soup.find('ul', class_='_93444fe79c-list--35Suf').find('li',
                                                                               class_='_93444fe79c-list-item--2QgXB _93444fe79c-list-item--active--2-sVo').find(
                    'span')  # .find_all('li', class_='_93444fe79c-list-item--2QgXB')[-1]
                cure_page = str(pages).split('<span>')[1].split('</span>')[0]
                cure_page = int(cure_page)
                n = int(n)
                print("Страниц меньше, чем " + str(n))
                timef.sleep(random.randint(10, 16))
            break
        except:
            print("Капча при подсчета числа страниц, меняем прокси и пытаемся еще")
            prox = connect_to_good_proxy()
            headers = {'User-Agent': random_ua()}
            n = n + 1
    print("Всего " + str(n) + " страниц для данного района")
    return n


def get_data_from_html(html, area):
    global number_of_ads

    soup = BeautifulSoup(html, 'lxml')
    ads = soup.find('div', class_='_93444fe79c-wrapper--1Z8Nz').find_all('div', class_='_93444fe79c-card--2Jgih')

    temp_list = []
    for i in range(0, len(ads)):
        minimass = []
        info = ads[i].find('div', class_='c6e8ba5398--single_title--22TGT')
        if info == None:
            info = ads[i].find('div', class_='c6e8ba5398--title--2CW78')
        try:
            square = str(info).split('кв., ')[1].split(' м²')[0]
        except IndexError:
            try:
                square = str(info).split('апарт., ')[1].split(' м²')[0]
            except IndexError:
                try:
                    square = str(info).split('Студия, ')[1].split(' м²')[0]
                except IndexError:
                    try:
                        square = str(info).split('Своб. планировка, ')[1].split(' м²')[0]
                    except IndexError:
                        info = ads[i].find('div', class_='c6e8ba5398--subtitle--UTwbQ')
                        try:
                            square = str(info).split('кв., ')[1].split(' м²')[0]
                        except IndexError:
                            try:
                                square = str(info).split('апарт., ')[1].split(' м²')[0]
                            except IndexError:
                                try:
                                    square = str(info).split('Студия, ')[1].split(' м²')[0]
                                except IndexError:
                                    try:
                                        square = str(info).split('Своб. планировка, ')[1].split(' м²')[0]
                                    except IndexError:
                                        print(info)
                                        print(ads[i])
        info2 = ads[i].find('div', class_='undefined c6e8ba5398--main-info--1SXZr')
        if info2 == None:
            info2 = ads[i].find('div', class_='c6e8ba5398--info-section--QBF61 c6e8ba5398--main-info--1SXZr')
        ad_url = str(info2).split('href="')[1].split('" target=')[0]
        info3 = ads[i].find('div', class_='c6e8ba5398--header--1dF9r')
        if info3 == None:
            info3 = ads[i].find('div', class_='c6e8ba5398--header--1df-X')
        price = str(info3).split('₽</div>')[0].split('>')[1].replace(" ", "").replace("₽", "")
        info4 = ads[i].find('div', class_='c6e8ba5398--absolute--9uFLj')
        time = str(info4).split('</div>')[0].split('>')[1]
        time = normalize_time(time)
        data = {
            'ad_url': ad_url,
            'square': square,
            'price': price,
            'time': time
        }
        print(data)
        write_csv(data, 'Cure\\' + area)
        number_of_ads = number_of_ads + 1
        print(number_of_ads)


def write_csv(data, area):
    with open('.\\Data\\' + area + '.csv', 'a', newline='') as f:
        writer = csv.writer(f, delimiter=' ', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([data['ad_url'],
                         data['square'],
                         data['price'],
                         data['time']])


def normalize_time(time):
    if time.find("сегодня") != -1:
        time = time.replace("сегодня, ",
                            str(dt.now().day) + ":" + str(dt.now().month) + ":")
    if time.find("вчера") != -1:
        time = time.replace("вчера, ",
                            str(dt.now().day - 1) + ":" + str(dt.now().month) + ":")
    if time.find("янв") != -1:
        time = time.replace(" янв, ", ":1" + ":")
    if time.find("фев") != -1:
        time = time.replace(" фев, ", ":2" + ":")
    if time.find("мар") != -1:
        time = time.replace(" мар, ", ":3" + ":")
    if time.find("апр") != -1:
        time = time.replace(" апр, ", ":4" + ":")
    if time.find("мая") != -1:
        time = time.replace(" мая, ", ":5" + ":")
    if time.find("июн") != -1:
        time = time.replace(" июн, ", ":6" + ":")
    if time.find("июл") != -1:
        time = time.replace(" июл, ", ":7" + ":")
    if time.find("авг") != -1:
        time = time.replace(" авг, ", ":8" + ":")
    if time.find("сен") != -1:
        time = time.replace(" сен, ", ":9" + ":")
    if time.find("окт") != -1:
        time = time.replace(" окт, ", ":10" + ":")
    if time.find("ноя") != -1:
        time = time.replace(" ноя, ", ":11" + ":")
    if time.find("дек") != -1:
        time = time.replace(" дек, ", ":12" + ":")
    return time


def everyday_process():
    get_good_proxies()

    params = ['Avia', 'Vahit', 'Kirov', 'Moscow', 'Novo', 'Privol', 'Sovet']
    pool = Pool(processes=7)
    processes = [pool.apply_async(get_from_area, [i]) for i in params]
    results = []
    for process in processes:
        results.append(process.get(timeout=3600))


def main():
    home = str(Path.home())
    print(home)
    pereriv = 0
    while True:
        x = 0
        with open('app\\last_parse.csv') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for data in reader:
                x = data[0]
        x = dt.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
        x = x + timedelta(hours=pereriv)
        # y = x.replace(day=x.day, hour=x.hour+pereriv, minute=x.minute + 21, second=x.second, microsecond=x.microsecond)
        if dt.now() > x:
            try:
                everyday_process()
                print('hah')
                with open('.\\last_parse.csv', 'w+', newline='') as f:
                    writer = csv.writer(f, delimiter=' ', quotechar='"')
                    writer.writerow([dt.now()])
                pereriv = 5
            except TimeoutError:
                pereriv = 1


if __name__ == '__main__':
    # print(str(datetime.now().day))
    try:
        main()

    except Exception as e:
        print(e)
