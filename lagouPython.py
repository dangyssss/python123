import requests
import pymysql
import time
import logging
from pymysql.err import MySQLError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(
    filename='lagou_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration
config = {
    'host': 'your_host',
    'user': 'your_username',
    'password': 'your_password',
    'db': 'your_database',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


def get_session(position):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': f'https://www.lagou.com/jobs/list_{position}?city=%E5%8C%97%E4%BA%AC&cl=false&fromSearch=true&labelWords=&suginput='
    })

    try:
        # Initial GET request to set cookies
        response = session.get(f'https://www.lagou.com/jobs/list_{position}?city=%E5%8C%97%E4%BA%AC', timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to initialize session: {e}")

    return session


def insert_into_db(data, db_config):
    if not data:
        logging.info("No data to insert into the database.")
        return

    try:
        connection = pymysql.connect(**db_config)
        with connection:
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO lagou (
                    positionName, workYear, salary, companyShortName, companyIdInLagou,
                    education, jobNature, positionIdInLagou, createTimeInLagou, city,
                    industryField, positionAdvantage, companySize, score, positionLables,
                    industryLables, publisherId, financeStage, companyLabelList, district,
                    businessZones, companyFullName, firstType, secondType, isSchoolJob,
                    subwayline, stationname, linestaion, resumeProcessRate, createByMe, keyByMe
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                cursor.executemany(sql, data)
            connection.commit()
        logging.info(f"Successfully inserted {len(data)} records into the database.")
    except MySQLError as e:
        logging.error(f"Database error during insertion: {e}")


def lagou(page, position):
    session = get_session(position)

    payload = {
        'first': 'true',
        'pn': page,
        'kd': position
    }
    url = 'https://www.lagou.com/jobs/positionAjax.json?city=%E5%8C%97%E4%BA%AC&needAddtionalResult=false&isSchoolJob=0'

    try:
        resp = session.post(url, data=payload, timeout=10)
        resp.raise_for_status()  # Raise HTTPError for bad responses
    except requests.RequestException as e:
        logging.error(f"HTTP request failed: {e}")
        return

    try:
        data = resp.json()
        result = data['content']['positionResult']['result']
    except (ValueError, KeyError) as e:
        logging.error(f"Error parsing JSON response: {e}")
        return

    processed_data = []
    time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    for item in result:
        try:
            record = (
                item.get('positionName', ''),
                item.get('workYear', ''),
                item.get('salary', ''),
                item.get('companyShortName', ''),
                item.get('companyId', ''),
                item.get('education', ''),
                item.get('jobNature', ''),
                item.get('positionId', ''),
                item.get('createTime', ''),
                item.get('city', ''),
                item.get('industryField', ''),
                item.get('positionAdvantage', ''),
                item.get('companySize', ''),
                item.get('score', ''),
                ",".join(item.get('positionLables', [])),
                ",".join(item.get('industryLables', [])),
                item.get('publisherId', ''),
                item.get('financeStage', ''),
                ",".join(item.get('companyLabelList', [])),
                item.get('district', ''),
                ",".join(item.get('businessZones', [])),
                item.get('companyFullName', ''),
                item.get('firstType', ''),
                item.get('secondType', ''),
                item.get('isSchoolJob', ''),
                item.get('subwayline', ''),
                item.get('stationname', ''),
                item.get('linestaion', ''),
                item.get('resumeProcessRate', ''),
                time_now,
                position
            )
            processed_data.append(record)
        except Exception as e:
            logging.error(f"Error processing item: {e}")
            continue  # Skip to the next item in case of error

    insert_into_db(processed_data, config)


def scrape_lagou_pages(position, max_pages):
    for page in range(1, max_pages + 1):
        logging.info(f"Scraping page {page} for position '{position}'.")
        lagou(page, position)
        sleep_time = random.uniform(1, 3)  # Sleep between 1 to 3 seconds
        logging.info(f"Sleeping for {sleep_time:.2f} seconds to respect rate limits.")
        time.sleep(sleep_time)


if __name__ == "__main__":
    position_to_search = 'Data Scientist'  # Example position
    total_pages = 5  # Number of pages to scrape
    scrape_lagou_pages(position_to_search, total_pages)
