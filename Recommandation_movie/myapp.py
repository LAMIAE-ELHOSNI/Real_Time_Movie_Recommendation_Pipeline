from elasticsearch import Elasticsearch
from flask import Flask,jsonify,render_template

app = Flask(__name__, template_folder='templates')

client = Elasticsearch(
    "http://localhost:9200",  # Elasticsearch endpoint
    )

def get_movies(size=0):  # You can adjust the size as needed
    query = {
        "query": {
            "bool": {
                "must": {"match_all": {}}
            }
        },
        "size": size
    }
    result = client.search(index='moviesdatabase', body=query)
    return result['hits']['hits']


@app.route('/api/movies/<string:title>', methods=['GET'])
def get_movie(title):
    movies = get_movies(size=100)
    return jsonify(movies)

@app.route('/', methods=['GET'])
def index():
    movies = get_movies(size=100)
    return render_template("index.html",movies=movies)

if __name__ == '__main__':
    app.run(debug=True)
