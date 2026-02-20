from flask import Flask
from waitress import serve
from neo4j import GraphDatabase
app = Flask(__name__)


class Neo4jCounter:
    def __init__(self):
        self.uri = "bolt://localhost:7687"
        self.driver = GraphDatabase.driver(self.uri, auth=("neo4j", "miy_parol"))

        with self.driver.session() as session:
            session.run("""
                MERGE (i:Item {id: 1})
                ON CREATE SET i.name = 'Hat', i.price = 1000, i.likes = 0
                ON MATCH SET i.likes = 0
            """)

    def close(self):
        self.driver.close()

    def increment(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (i:Item {id: 1})
                SET i.likes = i.likes + 1
                RETURN i.likes as current_likes
            """)
            return result.single()["current_likes"]

    def get_count(self):
        with self.driver.session() as session:
            result = session.run("MATCH (i:Item {id: 1}) RETURN i.likes as likes")
            record = result.single()
            return record["likes"] if record else 0

    def reset(self):
        with self.driver.session() as session:
            session.run("MATCH (i:Item {id: 1}) SET i.likes = 0")


counter = Neo4jCounter()


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
    try:
        print("Starting server on port 8080...")
        serve(app, host="127.0.0.1", port=8080, threads=20)
    finally:
        counter.close()