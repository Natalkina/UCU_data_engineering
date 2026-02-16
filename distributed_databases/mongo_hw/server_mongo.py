from flask import Flask
from waitress import serve
from pymongo import MongoClient, ReturnDocument
app = Flask(__name__)


class MongoCounter:
    def __init__(self):
        self.client = MongoClient(
            "mongodb://admin:admin123@localhost:27017/?authSource=admin"
        )
        self.db = self.client.web_counter
        self.collection = self.db.counters

        self.collection.update_one(
            {"_id": "main"},
            {"$setOnInsert": {"value": 0}},
            upsert=True
        )

    def increment(self):
        doc = self.collection.find_one_and_update(
            {"_id": "main"},
            {"$inc": {"value": 1}},
            return_document=ReturnDocument.AFTER
        )
        return doc["value"]

    def get_count(self):
        doc = self.collection.find_one({"_id": "main"})
        return doc["value"]

    def reset(self):
        self.collection.update_one(
            {"_id": "main"},
            {"$set": {"value": 0}}
        )

counter = MongoCounter()

@app.route('/inc')
def inc():
    counter.increment()
    return "OK", 200


@app.route('/count')
def count():
    return str(counter.get_count()), 200


@app.route('/reset')
def reset():
    counter.reset()
    return "OK", 200


if __name__ == '__main__':
    serve(app, host="127.0.0.1", port=8080, threads=20)
