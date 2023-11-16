from elasticsearch import Elasticsearch
from flask import Flask,jsonify,render_template

app = Flask(__name__, template_folder='templates')

client = Elasticsearch(
    "http://localhost:9200",  # Elasticsearch endpoint
    )

def get_movies():
    query = {
        "query": {
             "match_all": {
            }
        }
    }
    result = client.search(index='moviesdatabase', body=query)
    return result['hits']['hits']


@app.route('/api/movies/<string:title>', methods=['GET'])
def get_movie(title):
    movies = get_movies()
    return jsonify(movies)

@app.route('/', methods=['GET'])
def index():
    movies = get_movies()
    return render_template("index.html",movies=movies)

if __name__ == '__main__':
    app.run(debug=True)
