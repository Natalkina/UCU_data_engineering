from flask import Flask
from waitress import serve
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy, AddressTranslator
app = Flask(__name__)

SELECTED_CONSISTENCY = ConsistencyLevel.ONE

class AnyToLocalhostTranslator(AddressTranslator):
    def translate(self, addr):
        return '127.0.0.1'

class CassandraCounter:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'],
                               load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
                               address_translator=AnyToLocalhostTranslator())
        self.session = self.cluster.connect('web_counter')

    def increment(self):
        stmt = SimpleStatement(
            "UPDATE counters SET value = value + 1 WHERE id='main';",
            consistency_level=SELECTED_CONSISTENCY
        )
        self.session.execute(stmt)

    def get_count(self):
        stmt = SimpleStatement(
            "SELECT value FROM counters WHERE id='main';",
            consistency_level=SELECTED_CONSISTENCY
        )
        row = self.session.execute(stmt).one()
        return row.value if row else 0

    def reset(self):
        stmt = SimpleStatement(
            "DELETE FROM counters WHERE id='main';",
            consistency_level=SELECTED_CONSISTENCY
        )
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
