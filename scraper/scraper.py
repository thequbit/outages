
import datetime
import json
import time

import requests
from bs4 import BeautifulSoup

import pymongo

root_url = 'http://ebiz1.rge.com/OutageReports/'
base_url = root_url + 'RGE.html'

# zomg i hate that this is global but it's too 
# late to give a fuck
all_streets = []


def get_county_counts():

    #print("Getting county counts .", end='')

    counts = []

    resp = requests.get(base_url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find_all('tr')
    i = 0
    for tr in trs:
        tds = tr.find_all('td')
        if i > 2 and len(tds) > 2:
            url = tds[0].find('a').get('href')
            county = tds[0].find('a').text
            county_proper = county[0] + county.lower()[1:]
            customers = float(tds[1].text.replace(',',''))
            without_power = float(tds[2].text.replace(',',''))
            percent = (customers-without_power)/customers
            counts.append(
                dict(
                    url='%s%s' % (root_url, url),
                    county=county,
                    county_proper=county_proper,
                    customers=customers,
                    customers_without_power=without_power,
                    percent_customers_without_power=percent,
                    town_counts=[],
                    datetime=datetime.datetime.utcnow(),
                )
            )
        i += 1
        #print(".", end='')
    #print(" done")
    return counts

def get_streets_from_sub_town_url(county_name, town_name, url):

    streets = []

    resp = requests.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find_all('tr')

    i = 0
    for tr in trs:
        if i > 2:
            tds = tr.find_all('td')
            if len(tds) > 0:
                #print(tds[0])
                if '<a href' in str(tds[0]):
                    _url = tds[0].find('a').get('href')
                    _streets = get_streets_from_sub_town_url(county_name, town_name, '%s%s' %(root_url, _url))
                    for _street in _streets:
                        all_streets.append(
                            dict(
                                county=county_name,
                                town=town_name,
                                street=_street
                            )
                        )
                else:
                    _street = tds[0].text
                    print('%s, %s, %s' % (county_name, town_name, _street))
                    #streets.append(street)
                    all_streets.append(
                        dict(
                            county=county_name,
                            town=town_name,
                            street=_street
                        )
                    )
        i += 1

    return streets

def get_streets_from_town_url(county_name, town_name, url):

    streets = []

    resp = requests.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a')
    
    for link in links:
        _url = link.get('href')
        _streets = get_streets_from_sub_town_url(county_name, town_name, '%s%s' %(root_url, _url)) 

    return streets

def get_town_count(county, url):

    #print("Getting town count .", end='')

    counts = []

    resp = requests.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find_all('tr')
    i = 0
    for tr in trs:
        tds = tr.find_all('td')
        if i > 2 and len(tds) > 2:
            _url = tds[0].find('a').get('href')
            town = tds[0].find('a').text
            town_proper = town[0] + town.lower()[1:]
            streets = get_streets_from_town_url(county, town_proper, '%s%s' %(root_url, _url)) 
            customers = float(tds[1].text.replace(',',''))
            without_power = float(tds[2].text.replace(',',''))
            percent = (customers-without_power)/customers
            counts.append(
                dict(
                    county=county,
                    url='%s%s' % (root_url, _url),
                    town=town,
                    town_proper=town_proper,
                    customers=customers,
                    customers_without_power=without_power,
                    percent_customers_without_power=percent,
                    datetime=datetime.datetime.utcnow(),
                )
            )
        i += 1
        #print(".", end='')

    #print(" done.")

    return counts

def get_town_counts(county_counts):

    town_counts = []
    #for county_count in county_counts:
    for i in range(0, len(county_counts)):
        url = county_counts[i]['url']
        county = county_counts[i]['county']
        town_count = get_town_count(county, url)
        county_counts[i]['town_counts'].append(town_count)
        town_counts.append(town_count)
    return county_counts, town_counts

def push_to_mongo(county_counts, town_counts):

    print("Pushing to mongo ... ", end='')

    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.outages
    county_counts_db = db['county_counts']
    town_counts_db = db['town_counts']

    for county_count in county_counts:
        county_counts_db.insert(county_count)

    for town_count in town_counts:
        town_counts_db.insert(town_count)

    print("done.")

if __name__ == '__main__':

    i = 0
    #while(1):
    if True:

        county_counts = get_county_counts()

        county_counts, town_counts = get_town_counts(county_counts)

        #for street in all_streets:
        #    print

        with open('streets.csv', 'w') as f:
            for street in all_streets:
                f.write('%s\t %s\t %s\r\n' % (street['county'], street['town'], street['street']))

        #print(json.dumps(all_streets, indent=4))

        #push_to_mongo(county_counts, town_counts)

        #print("success")

        #time.sleep(30)

        #print(i)

        #i += 1
