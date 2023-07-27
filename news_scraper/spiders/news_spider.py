import json
import os
from datetime import datetime
from urllib.parse import urlparse
import re
import tldextract
from psycopg2.pool import SimpleConnectionPool
from dateutil import parser
import scrapy
from scrapy import Request
from langdetect import detect
from unidecode import unidecode
import string


def load_json(filename):
    """
    Load a JSON file from the disk.

    Parameters:
    filename (str): The path to the file.

    Returns:
    dict: A dictionary representation of the JSON file.
    """    
    with open(filename) as json_file:
        return json.load(json_file)

SELECTORS = load_json("selectors.json")["SELECTORS"]
DOMAIN_DICT = load_json("domains.json")

UNWANTED_SUBDOMAINS = ['sponsored-content', 'about-us','privacy-policy','terms-of-use']

DB_POOL = SimpleConnectionPool(
    1, 10,
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASS'],
    host=os.environ['DB_HOST'],
    port=os.environ['DB_PORT'],
)

class NewsSpider(scrapy.Spider):
    """
    The main spider class. Responsible for crawling and parsing websites for information.
    """

    name = "news_spider"
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'CONCURRENT_REQUESTS': 1,
        'WAYBACK_MACHINE_TIME_RANGE': (datetime(1995, 1, 1), datetime(2023, 7, 24)),
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_wayback_machine.WaybackMachineMiddleware': 5,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
        },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_BACKOFF': True,
        'RETRY_BACKOFF_MAX': 320,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': 'log.txt',
        'JOBDIR': 'jobs/news_spider'
    }

    def start_requests(self):
        """
        A generator function that yields Scrapy requests for all the domains in the domain dictionary.

        Yields:
        scrapy.Request: The request to the domain.
        """

        for domain, outlet in DOMAIN_DICT.items():
            yield Request(f"http://{domain}", self.parse, meta={"outlet": outlet})

    def parse(self, response):
        """
        Parses a response from a domain.

        Parameters:
        response (scrapy.http.Response): The response from the domain.

        Yields:
        scrapy.Request: A request to follow a URL.
        """

        base_url = urlparse(response.url).scheme + "://" + urlparse(response.url).netloc
        for href in response.css("a::attr(href)").getall():
            url = response.urljoin(href)
            if self.is_same_domain(url, response.url) and (url.startswith('http://') or url.startswith('https://')):
                yield response.follow(url, self.parse_article, meta={"outlet": response.meta["outlet"]})

    def parse_article(self, response):
        """
        Parses an article from a response.

        Parameters:
        response (scrapy.http.Response): The response from the article.
        """

        wayback_machine_time = response.meta.get('wayback_machine_time')
        if wayback_machine_time:
            wayback_machine_time = wayback_machine_time.strftime('%Y-%m-%d')
        else:
            wayback_machine_time = datetime.now().strftime('%Y-%m-%d')
        
        wayback_url = response.meta.get('wayback_machine_url', response.url)
        self.logger.info(f"Crawled Article: {wayback_url}")

        article = {
            'title': self.extract_data(response, SELECTORS['title']),
            'description': self.extract_data(response, SELECTORS['description']),
            'date_published': self.extract_date(response),
            'author': self.extract_data(response, SELECTORS['author']),
            'keywords': self.extract_data(response, SELECTORS['keywords']),
            'text': self.extract_data(response, SELECTORS['text']),
            'editor': self.extract_data(response, SELECTORS['editor']),
            'url': response.url,
            'outlet': response.meta["outlet"],
            'date_scraped': wayback_machine_time,
        }

        if self.should_save_article(response, article, response.url):
            self.save_to_db(article)

    def extract_data(self, response, selector_list):
        """
        Extracts data from a response using a list of selectors.

        Parameters:
        response (scrapy.http.Response): The response to extract data from.
        selector_list (list): A list of CSS or XPath selectors to use for extraction.

        Returns:
        str: The extracted data.
        """

        for selector in selector_list:
            if selector.startswith("//"):
                upper_case = string.ascii_uppercase
                lower_case = string.ascii_lowercase
                selector = selector.replace(
                    "text()", f"translate(text(), '{upper_case}', '{lower_case}')"
                )
                data = response.xpath(selector).getall()
            else:
                data = response.css(selector).xpath('.//text()').getall()
            if data:
                data = ' '.join([item.strip() for item in data if item.strip()])
                return self.clean_data(data, response.url, response.encoding)
        return None

    def extract_date(self, response):
        """
        Extracts the date from a response.

        Parameters:
        response (scrapy.http.Response): The response to extract the date from.

        Returns:
        str: The extracted date in 'YYYY-MM-DD' format.
        """

        date_str = self.extract_data(response, SELECTORS['date'])
        date = self.parse_date(date_str)
        if not date:
            date_obj = self.get_date_from_url(response.url)
            if date_obj:
                date = date_obj.strftime('%Y-%m-%d')
        return date

    def is_same_domain(self, url1, url2):
        """
        Checks if two URLs are from the same domain.

        Parameters:
        url1 (str): The first URL.
        url2 (str): The second URL.

        Returns:
        bool: True if the URLs are from the same domain, False otherwise.
        """

        return tldextract.extract(url1).domain == tldextract.extract(url2).domain

    def parse_date(self, date_str):
        """
        Parses a date string into a date.

        Parameters:
        date_str (str): The date string to parse.

        Returns:
        str: The parsed date in 'YYYY-MM-DD' format.
        """

        try:
            date = parser.parse(date_str)
            return date.strftime('%Y-%m-%d')
        except (TypeError, ValueError):
            return None

    def get_date_from_url(self, url):
        """
        Extracts a date from a URL.

        Parameters:
        url (str): The URL to extract the date from.

        Returns:
        datetime: The extracted date.
        """

        path = urlparse(url).path
        date_pattern = re.compile(r'\b(\d{1,4}[/-]?\d{1,2}[/-]?\d{1,4})\b')
        date_match = date_pattern.findall(path)
        if date_match:
            for date_str in date_match:
                try:
                    parsed_date = parser.parse(date_str, yearfirst=True)
                    if parsed_date.year < 1990 or parsed_date.date() > datetime.now().date():
                        continue
                    return parsed_date
                except ValueError:
                    pass
        return None

    def should_save_article(self, response, article, url):
        """
        Checks if an article should be saved.

        Parameters:
        response (scrapy.http.Response): The response from the article.
        article (dict): The article data.
        url (str): The URL of the article.

        Returns:
        bool: True if the article should be saved, False otherwise.
        """

        lang = response.xpath('/html/@lang').get()
        if not lang:
            lang = response.xpath('/html/@xml:lang').get()
            
        path = urlparse(url).path.strip()
        
        return (
            article['text'] and
            len(article['text']) >= 300 and
            not any(unwanted in urlparse(url).path for unwanted in UNWANTED_SUBDOMAINS) and
            path and  # Add this line to check if the path is not empty
            self.is_english(article['text'], lang)  # Language check comes last
        )


    def is_english(self, text, lang):
        """
        Checks if a text is in English.

        Parameters:
        text (str): The text to check.
        lang (str): The language code.

        Returns:
        bool: True if the text is in English, False otherwise.
        """

        if lang:  # If language metadata is present, it checks if 'en' is in it
            return 'en' in lang  
        else:  # If language metadata is not present, then use language detection library
            try: 
                return detect(text) == 'en'
            except Exception as e:
                self.logger.error(f"Error occurred while detecting language: {e}")
                return False


    def clean_data(self, data, url, encoding):
        """
        Cleans and normalizes data.

        Parameters:
        data (str): The data to clean.
        url (str): The URL of the data.
        encoding (str): The encoding of the data.

        Returns:
        str: The cleaned data.
        """

        # Handle decoding issues by replacing non-UTF8 characters
        try:
            data = data.encode('utf-8').decode(encoding)
        except UnicodeDecodeError:
            data = data.encode('utf-8', 'replace').decode(encoding, 'replace')

        # Unidecode the text
        data = unidecode(data)

        data = re.sub(r'\s+', ' ', data)
        data = re.sub(r'^https?:\/\/.*[\r\n]*', '', data, flags=re.MULTILINE)

        return data


def save_to_db(self, article):
    """
    Saves an article to the database.

    Parameters:
    article (dict): The article data.
    """
    connection = DB_POOL.getconn()
    cursor = connection.cursor()
    check_query = "SELECT 1 FROM articles WHERE url = %s"
    cursor.execute(check_query, (article['url'],))
    if cursor.fetchone():
        self.logger.info(f"Skipping duplicate article: {article['url']}")
        return
    insert_query = """INSERT INTO articles (title, description, date_published, author, keywords, text, editor, url, outlet, date_scraped) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cursor.execute(insert_query, (article['title'], article['description'], article['date_published'], article['author'], article['keywords'], 
                                      article['text'], article['editor'], article['url'], article['outlet'], article['date_scraped']))
        connection.commit()
        self.logger.info(f"Article saved: {article['url']}")
    except Exception as e:
        self.logger.error(f"Error occurred while saving article: {e}")
    finally:
        DB_POOL.putconn(connection)

