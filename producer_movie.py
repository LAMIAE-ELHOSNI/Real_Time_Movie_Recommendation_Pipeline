from confluent_kafka import Producer
import requests
import json
import time

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Your Kafka broker(s)
    'client.id': 'python-producer',
    'enable.idempotence': True
}

# Create a Kafka producer instance
producer = Producer(producer_config)

kafka_topic = 'movie_data'

def fetch_movie_data(api_key, page):
    MOVIE_ENDPOINT = f"https://api.themoviedb.org/3/movie/popular?api_key={api_key}&language=en-US&page={page}"
    response = requests.get(MOVIE_ENDPOINT)
    return response

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

# Deduplication variable to keep track of the last sent message
last_sent_message = None

# Pagination settings
max_pages = 2000  # Maximum number of pages to fetch
current_page = 1

# Produce movie data to Kafka
while current_page <= max_pages:
    # Fetch popular movie data from TMDb API
    API_KEY = 'd8bcb8f65f58af4646d3ecaab13be613'
    response = fetch_movie_data(API_KEY, current_page)

    if response.status_code == 200:
        movies = response.json()['results']
        for movie in movies:
            movie_value = json.dumps(movie)

            # Check if the message is the same as the last sent message
            if movie_value != last_sent_message:
                # Print the movie data before producing it
                print("Producing movie data:", movie_value)

                producer.produce(kafka_topic, value=movie_value, callback=delivery_report)

                # Update the last sent message
                last_sent_message = movie_value
                time.sleep(5)  # Retry every 30 seconds


        producer.flush()
        print(f"Data from page {current_page} produced to Kafka topic.")
        current_page += 1
    else:
        print(f"Error fetching data from page {current_page}. Retrying in 30 seconds.")
        time.sleep(30)  # Retry every 30 seconds