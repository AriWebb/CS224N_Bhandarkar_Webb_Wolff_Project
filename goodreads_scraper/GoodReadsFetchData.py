from datetime import datetime
import json
import os
import time
# Allowed genres just has one set containing all genres we will allow
import allowed_genres

from urllib.request import urlopen
from urllib.error import HTTPError
import bs4
import pandas as pd
import pickle

""" Goodreads Website Scraper - Ari Webb - 2/12/2023
    Based off code found here: https://github.com/cl3080/Auto-tagging-Books-Using-BERT
    Scrapes Goodreads website and puts JSON objects containing book titles, description and genre tags into
    output files.

"""

OUTPUT_DIR_PATH = '/home/arijwebb/cs224n_final/minbert-default-final-project/goodreads_scraper/data'
NUM_TO_SCRAPE = 10000

def get_from_doc(soup, arg1, arg2, arg3):
    return soup.find(arg1, {arg2: arg3})

def get_genres(soup):
    genres = []
    s = get_from_doc(soup, 'div', 'data-testid', 'genresList')
    if not s:
        return None
    s = s.text
    # Currently we check if each individual genre is in string, which is
    # O(n) where n is # of allowed genres. Could instead tokenize string
    # which would go faster.
    for genre in allowed_genres.all:
        if genre in s:
            genres.append(genre)
    return genres

def get_description(soup):
    s = get_from_doc(soup, 'div', 'data-testid', 'description')
    if not s:
        return None
    return ' '.join(s.text.split())

def get_title(soup):
    s = get_from_doc(soup, 'h1', 'data-testid', 'bookTitle')
    if not s:
        return None
    return ' '.join(s.text.split())


def scrape_book(book_id):
    url = 'https://www.goodreads.com/book/show/' + book_id
    source = urlopen(url)
    soup = bs4.BeautifulSoup(source, 'html.parser')
    title = get_title(soup)
    if title == None:
        return None
    genres = get_genres(soup)
    description = get_description(soup)

    return {'book_id':        book_id,
            'book_title':           title,
            'genres':               genres,
            'description':       description,
            }

def condense_books(books_directory_path):

    books = []
    for file_name in os.listdir(books_directory_path):
        if file_name.endswith('.json') and not file_name.startswith('.') and file_name != "all_books.json":
            _book = json.load(open(books_directory_path + '/' + file_name, 'r')) #, encoding='utf-8', errors='ignore'))
            books.append(_book)

    return books

def main():
    start_time = datetime.now()
    book_ids  = [str(i) for i in range(1,NUM_TO_SCRAPE)]
    books_already_scraped = []
    # No way to add books we skipped to already scraped yet, could build later.
    books_already_scraped =  [file_name.replace('.json', '') for file_name in os.listdir(OUTPUT_DIR_PATH) if file_name.endswith('.json') and not file_name.startswith('all_books')]
    books_to_scrape       = [book_id for book_id in book_ids if book_id not in books_already_scraped]
    condensed_books_path   = OUTPUT_DIR_PATH + '/all_books'

    for i, book_id in enumerate(books_to_scrape):
        try:
            print(str(datetime.now()) + ' ' +': Scraping ' + book_id + '...')
            print(str(datetime.now()) + ' ' +': #' + str(i+1+len(books_already_scraped)) + ' out of ' + str(len(book_ids)) + ' books')

            book = scrape_book(book_id)
            if book == None:
                print(str(datetime.now()) + ' ' +': Not enough info on ' + book_id + ', moving on')
            else:
                json.dump(book, open(OUTPUT_DIR_PATH + '/' + book_id + '.json', 'w'))

            print('=============================')

        except HTTPError as e:
            print(e)
            # if for whatever reason scrape_book throws an exception wait 60 seconds then run main again.
            time.sleep(60)
            main()


    books = condense_books(OUTPUT_DIR_PATH)

    json.dump(books, open(f"{condensed_books_path}.json", 'w'))
    book_df = pd.read_json(f"{condensed_books_path}.json")
    book_df.to_csv(f"{condensed_books_path}.csv", index=False, encoding='utf-8')

    print(str(datetime.now()) + ' ' + f':\n\n🎉 Success! All book metadata scraped. 🎉\n\nMetadata files have been output to /{OUTPUT_DIR_PATH}\nGoodreads scraping run time = ⏰ ' + str(datetime.now() - start_time) + ' ⏰')



if __name__ == '__main__':
    main()
