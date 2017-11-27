
import datetime
import json
import time

import requests
from bs4 import BeautifulSoup

import pymongo

root_url = 'http://ebiz1.rge.com/OutageReports/'
base_url = root_url + 'RGE.html'

def get_county_counts():

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
    return counts

def get_town_count(county, url):

    counts = []

    resp = requests.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find_all('tr')
    i = 0
    for tr in trs:
        tds = tr.find_all('td')
        if i > 2 and len(tds) > 2:
            url = tds[0].find('a').get('href')
            town = tds[0].find('a').text
            town_proper = town[0] + town.lower()[1:]
            customers = float(tds[1].text.replace(',',''))
            without_power = float(tds[2].text.replace(',',''))
            percent = (customers-without_power)/customers
            counts.append(
                dict(
                    county=county,
                    url='%s%s' % (root_url, url),
                    town=town,
                    town_proper=town_proper,
                    customers=customers,
                    customers_without_power=without_power,
                    percent_customers_without_power=percent,
                    datetime=datetime.datetime.utcnow(),
                )
            )
        i += 1
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

    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.outages
    county_counts_db = db['county_counts']
    town_counts_db = db['town_counts']

    for county_count in county_counts:
        county_counts_db.insert(county_count)

    for town_count in town_counts:
        town_counts_db.insert(town_count)

if __name__ == '__main__':

    i = 0
    #while(1):
    if True:

        county_counts = get_county_counts()

        county_counts, town_counts = get_town_counts(county_counts)

        push_to_mongo(county_counts, town_counts)

        print("success")

        time.sleep(30)

        print(i)

        i += 1
