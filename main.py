import yaml
import requests
import hashlib
import sqlite3
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin


# Load URLs from a YAML file
def load_urls_from_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data['urls']


# Initialize SQLite database
def initialize_database(db_name="scraper_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS scraped_data (
                        id INTEGER PRIMARY KEY,
                        url TEXT UNIQUE,
                        response_body TEXT,
                        response_headers TEXT,
                        status_code INTEGER,
                        http_protocol TEXT,
                        checksum TEXT
                      )''')
    conn.commit()
    return conn


# Save data to SQLite
def save_to_database(conn, url, response):
    cursor = conn.cursor()

    # Calculate checksum (MD5 hash of the response body)
    checksum = hashlib.md5(response.text.encode('utf-8')).hexdigest()

    # Determine HTTP protocol
    http_protocol = response.raw.version
    if http_protocol == 10:
        http_protocol = "HTTP/1.0"
    elif http_protocol == 11:
        http_protocol = "HTTP/1.1"
    elif http_protocol == 20:
        http_protocol = "HTTP/2.0"

    try:
        cursor.execute('''INSERT OR IGNORE INTO scraped_data (url, response_body, response_headers, status_code, http_protocol, checksum)
                          VALUES (?, ?, ?, ?, ?, ?)''',
                       (url, response.text, str(response.headers), response.status_code, http_protocol, checksum))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error saving data to database: {e}")


# Recursive scraper
def scrape_recursive(url, conn, visited_urls, max_depth=2, current_depth=0):
    if current_depth > max_depth or url in visited_urls:
        return

    try:
        # Make the HTTP request
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()

        # Save the response to the database
        save_to_database(conn, url, response)
        visited_urls.add(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)

        # Recursively scrape each link
        for link in links:
            absolute_link = urljoin(url, link['href'])
            scrape_recursive(absolute_link, conn, visited_urls, max_depth, current_depth + 1)

    except requests.RequestException as e:
        print(f"Failed to scrape {url}: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Main function
def main():
    yaml_file = "config.yaml"  # Specify the path to your YAML file
    urls = load_urls_from_yaml(yaml_file)

    # Initialize database connection
    conn = initialize_database()

    # Set to track visited URLs
    visited_urls = set()

    # Start scraping from the URLs provided in the YAML file
    for url in urls:
        scrape_recursive(url, conn, visited_urls)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
