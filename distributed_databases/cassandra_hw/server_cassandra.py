from flask import Flask
from waitress import serve
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
app = Flask(__name__)


class CassandraCounter:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('web_counter')

        # Ініціалізація лічильника
        stmt = "UPDATE counters SET value = value + 0 WHERE id='main';"
        self.session.execute(stmt)

    def increment(self):
        stmt = "UPDATE counters SET value = value + 1 WHERE id='main';"
        self.session.execute(stmt)

    def get_count(self):
        stmt = "SELECT value FROM counters WHERE id='main';"
        row = self.session.execute(stmt).one()
        return row.value if row else 0

    def reset(self):
        # У Cassandra counter не можна встановити значення напряму, треба "скинути" через декремент
        current = self.get_count()
        if current:
            stmt = f"UPDATE counters SET value = value - {current} WHERE id='main';"
            self.session.execute(stmt)


counter = CassandraCounter()

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
