import requests
from bs4 import BeautifulSoup
import time
import csv
import re
import csv
import schedule
from datetime import date

# reads last id and date
def read_last_entry(csv_file):
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            last_row = None
            for row in reader:
                if row:
                    last_row = row
            if last_row is None:
                return None, None
            last_id = int(last_row[0])
            last_month_year_str = last_row[1]
            return last_id, last_month_year_str
    except FileNotFoundError:
        print(f"File {csv_file} not found.")
        return None, None

# saves to csv
def append_to_csv(csv_file, data, start_id):
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        for row in data:
            writer.writerow([start_id] + row)
            start_id += 1

# inflation data parser
def inflation_parser(csv_file):
    
    if date.today().day != 7: #if not 7th of the month, doesnt parse
        return

    url = 'https://stat.gov.kz/ru/industries/economy/prices/publications/'

    response = requests.get(url)
    html_content = response.content
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    release_items = soup.select('.release-list > li')
    if not release_items:
        print("No items found on the page.")
        return

    data = []

    for item in release_items:
        release_title = item.select_one('.release-title').text.strip()
        link = item.select_one('.release-title')['href']#take the publication id and use it to follow through each
        detail_url = url + link
        try:
            detail_response = requests.get(detail_url, timeout=10)
            detail_content = detail_response.content
        except requests.exceptions.RequestException as e:
            print(f"Error fetching detail page: {e}")
            continue

        if detail_response.status_code != 200:
            print(f"Failed to retrieve the detail page. URL: {detail_url} Status code: {detail_response.status_code}")
            continue

        detail_soup = BeautifulSoup(detail_content, 'html.parser')
        inflation_rate = 'N/A'
        for info_item in detail_soup.select('.info-number-item'):
            desc = info_item.select_one('.info-number-item__desc').text.strip()
            if "Инфляция в Республике Казахстан" in desc:
                inflation_rate = info_item.select_one('.info-number-item__title').text.strip().replace(',', '.').replace('%', '')
                break
        
        match = re.search(r'\((\D+)\s+(\d{4})', release_title)
        if match:
            release_title_month = match.group(1).strip().lower()
            release_title_year = match.group(2).strip()
        else:
            print(f"Failed to extract month and year from title: {release_title}")
            continue
        
        months = {
            'январь': '01',
            'февраль': '02',
            'март': '03',
            'апрель': '04',
            'май': '05',
            'июнь': '06',
            'июль': '07',
            'август': '08',
            'сентябрь': '09',
            'октябрь': '10',
            'ноябрь': '11',
            'декабрь': '12',
        }

        month = months[release_title_month]
        year = release_title_year

        data.append([f'{year}-{month}', inflation_rate])
        
        time.sleep(1)

    if not data:
        print("No data was collected.")
        return
    last_id, last_month_year_str = read_last_entry(csv_file)

    if not last_month_year_str:
        last_month_year_str = "0000-00"

    new_data = []
    for entry in data:
        entry_month_year = entry[0]
        if entry_month_year > last_month_year_str:
            new_data.append(entry)

    new_data = sorted(new_data, key=lambda x: int(x[0].replace('-', '')))

    if new_data:
        last_id = last_id if last_id is not None else 0
        append_to_csv(csv_file, new_data, last_id + 1)
        print(f'Appended {len(new_data)} new entries to {csv_file}')
    else:
        print('No new data to append.')


def usd_kzt_rate_parser(csv_file):
    url = "https://www.google.com/finance/quote/USD-KZT?sa=X&ved=2ahUKEwidvuHpwYqHAxVECBAIHYNNDzkQmY0JegQIIBAp"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to retrieve the webpage: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    rate = soup.select_one('.YMlKec.fxKbKc').text
    today_date = date.today()

    date_formatted = today_date.strftime('%Y-%m-%d')
    data = [[date_formatted, rate.replace(',', '')]]
    last_id, _ = read_last_entry(csv_file)
    last_id = last_id if last_id is not None else 0

    append_to_csv(csv_file, data, last_id + 1)
    print(f'Appended new entry to {csv_file}')

if __name__ == "__main__":

    csv_file1 = 'inflation_data.csv'
    csv_file2 = 'usd_kzt_rate.csv'
    
    schedule.every().day.at("00:01").do(inflation_parser, csv_file1)
    schedule.every().day.at("00:01").do(usd_kzt_rate_parser, csv_file2)

    while True:
        schedule.run_pending()
        time.sleep(1)