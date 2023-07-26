# News Spider

News Spider is a web scraper designed to crawl and extract information from news websites. It uses the Scrapy framework and is capable of handling different types of websites.It takes a list of domains in the form of "nytimes.com": "The New York Times" and then scrapes all of the articles from that domain from the Wayback Machine for the period of time configured in the code, scraping the article title, description/header, date published, author, keywords, text, editor,url, and date it was scraped by the Wayback Machine, then the outlet name given to the domain is saved into the database for the article along with the scraped fields. All fields are saved into the database configured in the environmental variables, be sure to set up a database prior to scraping.

## Installation

1. Clone this repository.
2. Install the requirements using `pip install -r requirements.txt`.
3. Set the following environment variables: `DB_NAME`, `DB_USER`, `DB_PASS`, `DB_HOST`, `DB_PORT`.

## Usage

To run the spider, use the command `scrapy crawl news_spider`.

## Configuration

The spider is configured via two JSON files:

- `selectors.json`: This file contains CSS or XPath selectors used to extract information from the websites.
- `domains.json`: This file contains a list of domains to crawl.

Unwanted subdomains can be added to the `UNWANTED_SUBDOMAINS` list in `news_spider.py`.
