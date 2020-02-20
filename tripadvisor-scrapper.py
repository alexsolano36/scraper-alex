import argparse
import requests
import time
import os
import csv
from bs4 import BeautifulSoup

# Get all urls of the city
def parse_pagination_urls_of_city(city_default_url, city_url, offset, header):
    # Initialize the list for the resulting urls
    pagination_urls = list()

    # Retrieve url content of city (first page)
    content = get_request_with_retry(city_url, header)

    # Define parser
    soup = BeautifulSoup(content, 'html.parser')

    # Scrape number of pages (pagination of hotels in the city)
    try:
        number_of_pages_in_city = soup.find('a', attrs={'class': 'last'}).contents[0]
    except:
        number_of_pages_in_city = 1

    for i in range(0, int(number_of_pages_in_city)):
        if i == 0:
            # Append the already available first page url
            pagination_urls.append(city_default_url)
            logger.info('PROCESSED: ' + city_url)
        else:
            # Calculate the dash positions
            occurences_of_dash = [j for j in range(len(city_default_url)) if city_default_url.startswith('-', j)]

            # Get the second dash position
            second_dash_index = occurences_of_dash[1]

            # Each page contains 30 hotels
            city_pagination = i * offset

            # Build the current page url and append it to the list
            current_city_pagination_url = city_default_url[:second_dash_index] + '-oa' + str(city_pagination) + city_default_url[second_dash_index:] + '#ACCOM_OVERVIEW'
            pagination_urls.append(current_city_pagination_url)
            logger.info('PROCESSED: ' + current_city_pagination_url)

    return pagination_urls


# Get all hotel urls of the city
def parse_hotel_urls_of_city(base_url, pagination_urls, header):
    # Initialize the list for the resulting urls
    hotel_urls = list()

    for pagination_url in pagination_urls:
        # Build url out of base and current page url
        city_pagination_url = base_url + pagination_url

        # Retrieve url content of the page url
        content = get_request_with_retry(city_pagination_url, header)

        # Define parser
        soup = BeautifulSoup(content, 'html.parser')

        # Store each hotel url in the list
        for j, city_hotel_url in enumerate(soup.find_all('a', attrs={'class': 'property_title'})):
            hotel_urls.append(base_url + soup.find_all('a', attrs={'class': 'property_title'})[j]['href'][1:])
            logger.info('PROCESSED: ' + base_url + soup.find_all('a', attrs={'class': 'property_title'})[j]['href'][1:])

    # Remove duplicates
    hotel_urls = set(hotel_urls)
    hotel_urls = list(hotel_urls)

    return hotel_urls


# Get all pagination urls for all given hotels
def parse_pagination_urls_of_hotel(hotel_urls, header):
    # Initialize the list for the resulting urls
    pagination_urls = list()

    for hotel_url in hotel_urls:
        # Retrieve url content of the page url
        content = get_request_with_retry(hotel_url, header)

        # Define parser
        soup = BeautifulSoup(content, 'html.parser')

        # Scrape the highest pagination value of a hotel's pages
        pagination_items = soup.find_all('a', attrs={'class': 'pageNum'})
        
        try:
            maximum_pagination_of_hotel = int(pagination_items[-1].contents[0])
        except:
            maximum_pagination_of_hotel = 1

        # Calculate all pagination urls of the hotel
        for i in range(0, maximum_pagination_of_hotel):
            if i == 0:
                # Append the already available first page url
                pagination_urls.append(hotel_url + '#REVIEWS')
                logger.info('PROCESSED: ' + hotel_url + '#REVIEWS')
            else:
                # Calculate the dash positions
                occurrences_of_dash = [j for j in range(len(hotel_url)) if hotel_url.startswith('-', j)]

                # Get the fourth dash position
                fourth_dash_index = occurrences_of_dash[3]

                # Each page contains 10 hotels
                hotel_pagination = i * 10

                # Build the current page url and append it to the list
                hotel_page_url = hotel_url[:fourth_dash_index] + '-or' + str(hotel_pagination) + hotel_url[fourth_dash_index:] + '#REVIEWS'
                pagination_urls.append(hotel_page_url)
                logger.info('PROCESSED: ' + hotel_page_url)

    return pagination_urls


# Get all review urls of all given hotels
def parse_review_urls_of_hotel(base_url, pagination_urls, header):
    # Initialize the list for the resulting urls
    review_urls = list()

    for pagination_url in pagination_urls:
        # Retrieve url content of the hotel pagination url
        content = get_request_with_retry(pagination_url, header)

        # Define parser
        soup = BeautifulSoup(content, 'html.parser')

        # Get all review containers of the current page
        hotel_review_containers = soup.find_all('div', attrs={'class': 'basic_review'})

        # Retrieve each review url of the current hotel pagination page
        for hotel_review_container in hotel_review_containers:
            quote = hotel_review_container.find('div', attrs={'class': 'quote'})

            # Get the review url without base url
            review_url = quote.find('a')['href'][1:]

            # Append the complete review url to the list
            review_urls.append(base_url + review_url)
            logger.info('PROCESSED: ' + base_url + review_url)

    return review_urls


# Parse all reviews of a city
def parse_reviews_of_city(review_urls, city_default_url, user_base_url, session_timestamp, header):
    processed_hotels = list()
    hotel_information = dict()

    # Create a directory for the current scrapping session
    city_directory_path = create_session_directory(city_default_url, session_timestamp)

    hotel_directory_path = ''
    rating_directory_paths = []
    headline_exists = False

    for i, review_url in enumerate(review_urls):
        logger.info('STARTED: Processing of ' + review_url + ' (Review ' + str(i + 1) + ' of ' + str(len(review_urls)) + ')')

        # Calculate the dash positions
        occurrences_of_dash = [j for j in range(len(review_url)) if review_url.startswith('-', j)]

        # Get the hotel name out of the url
        hotel_name = review_url[occurrences_of_dash[3] + 1:occurrences_of_dash[4]].replace(' ', '_').lower()

        # Only process hotel information once
        if hotel_name not in processed_hotels:
            try:
                headline_exists = False
                rating_directory_paths = []
                processed_hotels.append(hotel_name)
                hotel_information = parse_hotel_information(review_url, header)
                hotel_directory_path = create_hotel_directory(hotel_name, city_directory_path)
                rating_directory_paths = create_rating_directories(hotel_directory_path)
                store_hotel_data_in_csv(hotel_name, hotel_information, hotel_directory_path)
            except:
                logger.warning('WARNING: Processing of ' + review_url + ' was skipped due to an unexpected error!')
                continue

        # Parse review information
        try:
            review_information = parse_review_information(review_url, user_base_url, header)
        except ValueError:
            logger.warning('WARNING: Processing of ' + review_url + ' was skipped due to missing of essential information!')
            continue
        except:
            logger.warning('WARNING: Processing of ' + review_url + ' was skipped due to an unexpected error!')
            continue

        # Store review information in csv file
        try:
            store_review_data_in_csv(review_url, hotel_name, review_information, hotel_directory_path, headline_exists)
        except:
            logger.warning('WARNING: Processing of ' + review_url + ' was skipped due to an unexpected error!')
            continue

        if not headline_exists:
            headline_exists = True

        # Store review text in textfile
        try:
            store_review_data_in_txt(review_url, rating_directory_paths, review_information)
        except:
            logger.warning('WARNING: Processing of ' + review_url + ' was skipped due to an unexpected error!')
            continue		

        logger.info('FINISHED: Processing of ' + review_url + ' (Review ' + str(i + 1) + ' of ' + str(len(review_urls)) + ')')

# Creates a txt file for a hotel's reviews and stores the reviews inside
def store_review_data_in_txt(review_url, rating_directory_paths, review_information):
    rating = int(review_information[0]['rating'].replace(' stars', ''))
    rating_path = rating_directory_paths[rating - 1]
	
    # Calculate the dash positions
    occurences_of_dash = [j for j in range(len(review_url)) if review_url.startswith('-', j)]

    logger.info('STARTED: Storing of review text from ' + review_url + ' into ' + rating_path.replace('\\\\?\\', '') + '\\review_' + review_url[occurences_of_dash[0] + 1:occurences_of_dash[3]] + '.txt')
	
    # Write review text to file
    with open(rating_path + '\\review_' + review_url[occurences_of_dash[0] + 1:occurences_of_dash[3]] + '.txt', 'wb') as file:
        file.write(bytes(review_information[0]['text'], encoding='ascii', errors='ignore'))

    logger.info('FINISHED: Storing of review text from ' + review_url + ' into ' + rating_path.replace('\\\\?\\', '') + '\\review_' + review_url[occurences_of_dash[0] + 1:occurences_of_dash[3]] + '.txt')

# Creates a csv file for a hotel's reviews and stores the reviews inside
def store_review_data_in_csv(review_url, hotel_name, review_data, hotel_directory_path, headline_exists):
    logger.info('STARTED: Storing of review data from ' + review_url + ' into ' + hotel_directory_path.replace('\\\\?\\', '') + '\\' + hotel_name + '-reviews.csv')

    with open(hotel_directory_path + '\\' + hotel_name + '-reviews.csv', 'a') as file:
        # Setup a writer
        csvwriter = csv.writer(file, delimiter='|', dialect='excel')

        # Write headlines into the file
        if not headline_exists:
            # Write headlines into the file
            csvwriter.writerow(
                [
                    'Title', 'Text', 'Room Tip', 'Publication Date',
                    'Overall Rating', 'Value Rating', 'Location Rating'
                ]
            )

        # Write the data into the file
        csvwriter.writerow(
            [
                review_data[0]['title'], review_data[0]['text'], review_data[0]['room-tip'], review_data[0]['date'],
                review_data[0]['rating'], review_data[0]['value-rating'], review_data[0]['location-rating'],
                review_data[1]['url']
            ]
        )

    logger.info('FINISHED: Storing of review data from ' + review_url + ' into ' + hotel_directory_path.replace('\\\\?\\', '') + '\\' + hotel_name + '-reviews.csv')

# Creates a csv file for a hotel and stores the hotel information inside
def store_hotel_data_in_csv(hotel_name, hotel_data, hotel_directory_path):
    logger.info('STARTED: Storing of hotel data ' + hotel_name + ' into ' + hotel_directory_path.replace('\\\\?\\', '') + '\\' + hotel_name + '-information.csv')

    with open(hotel_directory_path + '\\' + hotel_name + '-information.csv', 'w') as csvfile:
        # Setup a writer
        csvwriter = csv.writer(csvfile, delimiter='|', dialect='excel')

        # Write headlines into the file
        csvwriter.writerow(['Name', 'Address', 'Description', 'Stars', 'TripAdvisor City Rank', 'Overall Rating' , 'Review Count', 'Review Rating Count'])

        # Write the data into the file
        csvwriter.writerow(
            [
                hotel_data['name'], hotel_data['address'], hotel_data['description'], hotel_data['stars'],
                hotel_data['rank'], hotel_data['overall-rating'], hotel_data['review-count'],
                hotel_data['star-filter']
            ]
        )

    logger.info('FINISHED: Storing of ' + hotel_name + ' into ' + hotel_directory_path.replace('\\\\?\\', '') + '\\' + hotel_name + '-information.csv')

# Creates a directory for each rating category (e.g. 5 stars, 4 stars)
def create_rating_directories(hotel_path):
    stars = [1, 2, 3, 4, 5]

    paths = list()

    for star in stars:
        # Build directory name
        directory_path = hotel_path + '\\' + str(star) + '-star'

        logger.info('STARTED: Creation of directory ' + hotel_path.replace('\\\\?\\', '') + '\\' + str(star) + '-star')

        # Create the folder
        os.makedirs(directory_path)

        logger.info('FINISHED: Creation of directory ' + hotel_path.replace('\\\\?\\', '') + '\\' + str(star) + '-star')

        paths.append(directory_path)

    return paths

# Creates a directory for a hotel
def create_hotel_directory(hotel_name, city_directory_name):
    # Build directory name
    directory_path = city_directory_name + '\\' + hotel_name

    logger.info('STARTED: Creation of directory ' + city_directory_name.replace('\\\\?\\', '') + '\\' + hotel_name)

    # Create the folder
    os.makedirs(directory_path)

    logger.info('FINISHED: Creation of directory ' + city_directory_name.replace('\\\\?\\', '') + '\\' + hotel_name)

    return directory_path

# Creates a directory for a session
def create_session_directory(city_default_url, session_timestamp):
    # Get the city name from the url
    occurrences_of_dash = [j for j in range(len(city_default_url)) if city_default_url.startswith('-', j)]
    city_name = city_default_url[occurrences_of_dash[1] + 1:occurrences_of_dash[2]].lower()

    # Build directory name
    directory_path = '\\\\?\\' + os.getcwd() + '\\data\\' + session_timestamp + '-' + city_name

    logger.info('STARTED: Creation of directory ' + os.getcwd() + '\\data\\' + session_timestamp + '-' + city_name)

    # Create the folder
    os.makedirs(directory_path)

    logger.info('FINISHED: Creation of directory ' + os.getcwd() + '\\data\\' + session_timestamp + '-' + city_name)

    return directory_path


def parse_hotel_information(review_url, header):
    # Initialize the dictionary for the hotel
    hotel = dict()

    # Retrieve url content of the review url
    content = get_request_with_retry(review_url, header)

    # Define parser
    soup = BeautifulSoup(content, 'html.parser')

    logger.info('STARTED: Parsing of hotel data from ' + review_url)

    hotel['name'] = soup.find('a', attrs={'class': 'HEADING'}).text.replace('|', '').strip()
    hotel['overall-rating'] = soup.find('img', attrs={'class': 'sprite-rating_no_fill'})['alt'][0:1] + ' stars'
    hotel['rank'] = soup.find('div', attrs={'class': 'slim_ranking'}).text.strip()

    review_count = soup.find('h3', attrs={'class': 'reviews_header'}).text
    occurrences_of_spaces = [j for j in range(len(review_count)) if review_count.startswith(' ', j)]
    hotel['review-count'] = str(review_count[0:occurrences_of_spaces[0]])

    review_filter = soup.find('fieldset', attrs={'class': 'review_filter_lodging'})

    hotel['address'] = soup.find('span', attrs={'class': 'format_address'}).text.replace('|', '-')

    try:
        star_filter_items = review_filter.find('div', attrs={'class': 'col2of2'}).find_all('div', attrs={'class': 'wrap'})
        star_filter_string = ''
        for star_filter_item in star_filter_items:
            description = star_filter_item.find('span', attrs={'class': 'text'}).text
            count = star_filter_item.find('span', attrs={'class': 'compositeCount'}).text
            star_filter_string += description + ' (' + count + ') - '
        hotel['star-filter'] = star_filter_string[0:-3]
    except:
        hotel['star-filter'] = 'n.a.'

    try:
        hotel['stars'] = str(soup.find('div', attrs={'class': 'stars'}).text.replace('Hotel Class:', '').strip()[0:1])
    except:
        hotel['stars'] = 'n.a.'

    try:
        hotel['room-count'] = str(soup.find('span', attrs={'class': 'tabs_num_rooms'}).text.strip())
    except:
        hotel['room-count'] = 'n.a.'

    try:
        hotel['description'] = soup.find('span', attrs={'class': 'descriptive_text'}).text.strip() + soup.find('span', attrs={'class': 'descriptive_text_last'}).text.replace('|', '').strip()
    except:
        hotel['description'] = 'n.a.'

    logger.info('FINISHED: Parsing of hotel data from ' + review_url)

    return hotel


# Main
if __name__ == '__main__':
    # Setup commandline handler
    parser = argparse.ArgumentParser(description='scrape the reviews of a whole city on tripadvisor' , usage='python tripadvisor-scrapper 60763 New_York_City_New_York')
    parser.add_argument('id', help='the geolocation id of the city')
    parser.add_argument('name', help='the name of the city')
    parser.add_argument('--pickle', choices=['load', 'store'], help='[load] store a scraped reviews list as pickle for later parsing,[load] load a scraped reviews list for parsing')
    parser.add_argument('--filename', help='the filename of the pickle file placed in pickle directory')
    args = parser.parse_args()

    # Setup logger
    session_timestamp = time.strftime('%Y%m%d-%H%M%S')
    logging.basicConfig(filename='./logs/' + session_timestamp + '-' + args.name.lower() + '.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logging.getLogger().addHandler(logging.StreamHandler())

    # Define user agent
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0'}

    # Define base urls of TripAdvisor
    BASE_URL = 'http://www.tripadvisor.com/'
    CITY_DEFAULT_URL = 'Hotels-g' + args.id + '-' + args.name + '-Hotels.html'
    CITY_URL = BASE_URL + CITY_DEFAULT_URL
    USER_BASE_URL = 'https://www.tripadvisor.com/members/'

    # Define items per page
    number_of_hotels_per_page = 30
    number_of_reviews_per_page = 10

    # Parse all needed urls
    if not args.url or args.url == 'store':
        logger.info('STARTED: Scraping of ' + args.name + ' review urls. Build tree "city-pagination-urls--city-hotel-urls--hotel-pagination-urls--hotel-review-urls".')
        city_pagination_urls = parse_pagination_urls_of_city(CITY_DEFAULT_URL, CITY_URL, number_of_hotels_per_page, headers)
        city_hotel_urls = parse_hotel_urls_of_city(BASE_URL, city_pagination_urls, headers)
        hotel_pagination_urls = parse_pagination_urls_of_hotel(city_hotel_urls, headers)
        city_review_urls = parse_review_urls_of_hotel(BASE_URL, hotel_pagination_urls, headers)