import psycopg2
import threading
import time

DB_PARAMS = {
    "dbname": "test_db",
    "user": "postgres",
    "password": "password",
    "host": "localhost"
}

THREADS = 10
ITERATIONS = 10000


def reset_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS user_counter;")
    cur.execute("CREATE TABLE user_counter (user_id INT PRIMARY KEY, counter INT, version INT);")
    cur.execute("INSERT INTO user_counter VALUES (1, 0, 0);")
    conn.commit()
    cur.close()
    conn.close()


def run_test(target_func, name):
    reset_db()
    threads = []
    start_time = time.time()

    for _ in range(THREADS):
        t = threading.Thread(target=target_func)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    final_val = cur.fetchone()[0]
    print(f"[{name}] Time: {end_time - start_time:.2f}s, Final Value: {final_val}")
    cur.close()
    conn.close()


# methods to do

def lost_update():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for _ in range(ITERATIONS):
        cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        val = cur.fetchone()[0]
        cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (val + 1,))
        conn.commit()
    conn.close()


def serializable_update():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
    cur = conn.cursor()
    for _ in range(ITERATIONS):
        while True:
            try:
                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                val = cur.fetchone()[0]
                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (val + 1,))
                conn.commit()
                break
            except psycopg2.errors.SerializationFailure:
                conn.rollback()
                continue
    conn.close()


def in_place_update():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for _ in range(ITERATIONS):
        cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()
    conn.close()


def row_level_locking():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for _ in range(ITERATIONS):
        cur.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        val = cur.fetchone()[0]
        cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (val + 1,))
        conn.commit()
    conn.close()


def optimistic_control():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for _ in range(ITERATIONS):
        while True:
            cur.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            val, ver = cur.fetchone()
            cur.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = 1 AND version = %s",
                        (val + 1, ver + 1, ver))
            conn.commit()
            if cur.rowcount > 0:
                break
    conn.close()


if __name__ == "__main__":
    run_test(lost_update, "Lost Update")
    run_test(serializable_update, "Serializable (with Retries)")
    run_test(in_place_update, "In-place Update")
    run_test(row_level_locking, "Row-level Locking")
    run_test(optimistic_control, "Optimistic Control")