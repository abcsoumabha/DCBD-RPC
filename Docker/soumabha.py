import re
import time
import requests
from collections import Counter
from multiprocessing import Pool, cpu_count

BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "BMC202329"

TARGET_GLOBAL_RPS = 80
REQUEST_TIMEOUT = 10
MAX_RETRIES = 8
BACKOFF_BASE = 0.25

WORKER_SECRET_KEY = None
WORKER_BASE_URL = None
WORKER_DELAY = 0.0


def login(student_id):
    url = f"{BASE_URL}/login"
    payload = {"student_id": student_id}

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)

            if r.status_code == 200:
                data = r.json()
                if "secret_key" not in data:
                    raise RuntimeError(f"Login response missing secret_key: {data}")
                return data["secret_key"]

            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue

            raise RuntimeError(f"Login failed: {r.status_code} | {r.text}")

        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Login request failed: {e}")
            time.sleep(BACKOFF_BASE * (2 ** attempt))

    raise RuntimeError("Unable to obtain secret key after retries.")


def init_worker(secret_key, base_url, delay):
    global WORKER_SECRET_KEY, WORKER_BASE_URL, WORKER_DELAY
    WORKER_SECRET_KEY = secret_key
    WORKER_BASE_URL = base_url
    WORKER_DELAY = delay


def normalize_first_word(title):
    if not title:
        return None

    title = title.strip()
    if not title:
        return None

    first_token = title.split()[0]

    # keep case same, just remove punctuation around the token
    first_token = re.sub(r"^[^\w]+|[^\w]+$", "", first_token, flags=re.UNICODE)

    if not first_token:
        return None

    return first_token


def get_publication_title(student_id, filename):
    global WORKER_SECRET_KEY, WORKER_BASE_URL, WORKER_DELAY

    if WORKER_BASE_URL is None:
        WORKER_BASE_URL = BASE_URL

    if WORKER_SECRET_KEY is None:
        WORKER_SECRET_KEY = login(student_id)

    url = f"{WORKER_BASE_URL}/lookup"
    payload = {
        "secret_key": WORKER_SECRET_KEY,
        "filename": filename
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)

            if r.status_code == 200:
                data = r.json()
                if "title" not in data:
                    raise RuntimeError(f"Lookup response missing title for {filename}: {data}")

                if WORKER_DELAY > 0:
                    time.sleep(WORKER_DELAY)

                return data["title"]

            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue

            if r.status_code in (401, 403):
                WORKER_SECRET_KEY = login(student_id)
                payload["secret_key"] = WORKER_SECRET_KEY
                time.sleep(BACKOFF_BASE)
                continue

            if r.status_code == 404:
                raise FileNotFoundError(f"{filename} not found on server.")

            raise RuntimeError(f"Lookup failed for {filename}: {r.status_code} | {r.text}")

        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Network error while fetching {filename}: {e}")
            time.sleep(BACKOFF_BASE * (2 ** attempt))

    raise RuntimeError(f"Unable to retrieve title for {filename} after retries.")


def verify_top_10(secret_key, top_10_list):
    url = f"{BASE_URL}/verify"
    payload = {
        "secret_key": secret_key,
        "top_10": top_10_list
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)

            if r.status_code == 200:
                data = r.json()
                print("\nVerification Result")
                print("-------------------")
                print(f"Score   : {data.get('score')}/{data.get('total')}")
                print(f"Correct : {data.get('correct')}")
                print(f"Message : {data.get('message')}")
                return data

            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue

            raise RuntimeError(f"Verify failed: {r.status_code} | {r.text}")

        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Verification request failed: {e}")
            time.sleep(BACKOFF_BASE * (2 ** attempt))

    raise RuntimeError("Unable to verify after retries.")


def mapper(filename_chunk):
    local_counter = Counter()

    for filename in filename_chunk:
        try:
            title = get_publication_title(STUDENT_ID, filename)
            first_word = normalize_first_word(title)
            if first_word:
                local_counter[first_word] += 1
        except Exception as e:
            print(f"[WARN] Skipping {filename}: {e}")

    return local_counter


def reducer(counter_list):
    total = Counter()
    for c in counter_list:
        total.update(c)
    return total


def chunkify(items, n_chunks):
    chunk_size = (len(items) + n_chunks - 1) // n_chunks
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


if __name__ == "__main__":
    filenames = [f"pub_{i}.txt" for i in range(1000)]

    # IMPORTANT: use one secret key for both lookup and verify
    secret_key = login(STUDENT_ID)

    num_workers = min(8, cpu_count())
    worker_delay = num_workers / TARGET_GLOBAL_RPS
    chunks = chunkify(filenames, num_workers)

    with Pool(
        processes=num_workers,
        initializer=init_worker,
        initargs=(secret_key, BASE_URL, worker_delay)
    ) as pool:
        mapped_results = pool.map(mapper, chunks)

    final_counts = reducer(mapped_results)

    top_10 = [word for word, _ in sorted(final_counts.items(), key=lambda x: (-x[1], x[0]))[:10]]

    print("Top 10 most frequent first words:")
    for i, word in enumerate(top_10, start=1):
        print(f"{i:2d}. {word} ({final_counts[word]})")

    verify_top_10(secret_key, top_10)
    
    