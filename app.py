"""
Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
or more contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch B.V. licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""


import os
from elasticsearch import Elasticsearch
import quart
import quart_cors
from quart import request, abort

app = quart_cors.cors(quart.Quart(__name__), allow_origin="*")

# Connect to Elastic Cloud cluster
def es_connect(cid, user, passwd):
  es = Elasticsearch(cloud_id=cid, http_auth=(user, passwd))
  return es

# Search ElasticSearch index and return body and URL of the result
def ESSearch(query_text):
  cid = os.environ['cloud_id']
  cp = os.environ['cloud_pass']
  cu = os.environ['cloud_user']
  es = es_connect(cid, cu, cp)

  # Elasticsearch query (BM25) and kNN configuration for hybrid search
  query = {
    "bool": {
      "must": [{
        "match": {
          "title": {
            "query": query_text,
            "boost": 1
          }
        }
      }],
      "filter": [{
        "exists": {
          "field": "title"
        }
      }]
    }
  }

  knn = {
    "field": "ml.inference.title.predicted_value",
    "k": 1,
    "num_candidates": 20,
    "query_vector_builder": {
      "text_embedding": {
        "model_id": "sentence-transformers__all-distilroberta-v1",
        "model_text": query_text
      }
    },
    "boost": 24
  }

  fields = ["title", "body_content", "url"]
  index = 'search-elastic-docs'
  resp = es.search(index=index,
                   query=query,
                   knn=knn,
                   fields=fields,
                   size=1,
                   source=False)

  body = resp['hits']['hits'][0]['fields']['body_content'][0]
  url = resp['hits']['hits'][0]['fields']['url'][0]

  return body, url

@app.get("/search")
async def search():
  api_key_header = request.headers.get('API_KEY')
  if not api_key_header or api_key_header != os.getenv('API_KEY'):
    abort(401)  # Unauthorized access
  query = request.args.get("query")
  resp, url = ESSearch(query)
  return quart.Response(response=resp + '\n\n' + url)


def main():
  port = int(os.environ.get("PORT", 5001))
  app.run(debug=False, host="0.0.0.0", port=port)


if __name__ == "__main__":
  main()
