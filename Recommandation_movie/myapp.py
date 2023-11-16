from elasticsearch import Elasticsearch
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder='templates')

client = Elasticsearch(
    "http://localhost:9200",  # Elasticsearch endpoint
)

def get_movies(size=0, search_query=None):  
    query = {
        "query": {
            "bool": {
                "must": [{"match_all": {}}]
            }
        },
        "size": size
    }

    if search_query:
        query["query"]["bool"]["must"].append({"multi_match": {"query": search_query, "fields": ["title^3", "overview"]}})

    result = client.search(index='moviesdatabase', body=query)
    return result['hits']['hits']

@app.route('/api/movies/<string:title>', methods=['GET'])
def get_movie(title):
    movies = get_movies(size=100)
    return jsonify(movies)

@app.route('/', methods=['GET'])
def index():
    search_query = request.args.get('q', '')  # Get the search query from the URL parameter 'q'
    
    if search_query:
        movies = get_movies(size=100, search_query=search_query)
    else:
        movies = get_movies(size=100)
    
    return render_template("index.html", movies=movies, search_query=search_query)

if __name__ == '__main__':
    app.run(debug=True)
