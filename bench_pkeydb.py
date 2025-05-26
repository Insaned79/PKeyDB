import requests
import time
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

SERVER_URL = "http://127.0.0.1:8080"
DBNAME = "db1"
AUTH = {"dbname": DBNAME, "password": "secret1"}
THREADS = 1
N_KEYS = 1000

def get_token():
    resp = requests.post(f"{SERVER_URL}/auth", json=AUTH)
    resp.raise_for_status()
    return resp.json()["token"]

def random_key(n=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def bench_set(token, keys):
    def do_set(key):
        r = requests.post(f"{SERVER_URL}/db/{DBNAME}/set",
                          headers={"X-Auth-Token": token},
                          json={"key": key, "value": "v_" + key})
        return r.status_code
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        t0 = time.time()
        futs = [pool.submit(do_set, k) for k in keys]
        results = [f.result() for f in as_completed(futs)]
        t1 = time.time()
    return t1-t0, results

def bench_get(token, keys):
    def do_get(key):
        r = requests.get(f"{SERVER_URL}/db/{DBNAME}/get",
                         headers={"X-Auth-Token": token},
                         params={"key": key})
        return r.status_code
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        t0 = time.time()
        futs = [pool.submit(do_get, k) for k in keys]
        results = [f.result() for f in as_completed(futs)]
        t1 = time.time()
    return t1-t0, results

def bench_del(token, keys):
    def do_del(key):
        r = requests.delete(f"{SERVER_URL}/db/{DBNAME}/del",
                            headers={"X-Auth-Token": token},
                            params={"key": key})
        return r.status_code
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        t0 = time.time()
        futs = [pool.submit(do_del, k) for k in keys]
        results = [f.result() for f in as_completed(futs)]
        t1 = time.time()
    return t1-t0, results

def main():
    print(f"== PKeyDB Performance Test: {N_KEYS} keys, {THREADS} threads ==")
    token = get_token()
    # Генерируем ключи
    keys = [random_key() for _ in range(N_KEYS)]

    # Очистка базы
    requests.post(f"{SERVER_URL}/db/{DBNAME}/clear", headers={"X-Auth-Token": token})

    # Вставка
    print('--- SET (insert) ---')
    t_set, res_set = bench_set(token, keys)
    ok_set = res_set.count(200)
    print(f"SET: {N_KEYS} keys in {t_set:.2f} sec, RPS={N_KEYS/t_set:.1f}, ok={ok_set}, errors={N_KEYS-ok_set}")

    # Чтение
    print('--- GET (read) ---')
    t_get, res_get = bench_get(token, keys)
    ok_get = res_get.count(200)
    notfound_get = res_get.count(404)
    other_get = N_KEYS - ok_get - notfound_get
    print(f"GET: {N_KEYS} keys in {t_get:.2f} sec, RPS={N_KEYS/t_get:.1f}, ok={ok_get}, notfound={notfound_get}, errors={other_get}")
    if notfound_get > 0:
        print('Not found keys:')
        for k, code in zip(keys, res_get):
            if code == 404:
                print(k)

    # Удаление
    print('--- DEL (delete) ---')
    t_del, res_del = bench_del(token, keys)
    ok_del = res_del.count(200)
    notfound_del = res_del.count(404)
    other_del = N_KEYS - ok_del - notfound_del
    print(f"DEL: {N_KEYS} keys in {t_del:.2f} sec, RPS={N_KEYS/t_del:.1f}, ok={ok_del}, notfound={notfound_del}, errors={other_del}")

if __name__ == '__main__':
    main() 