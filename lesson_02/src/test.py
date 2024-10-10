import os
import time
import requests
from util import get_base_dir

BASE_DIR = get_base_dir()

if not BASE_DIR:
    print("BASE_DIR environment variable must be set")
    exit(1)

# Job ports
JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", "2022-08-09")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales", "2022-08-09")


def run_job1(port, host):
    print("Starting job1:")
    resp = requests.post(
        url=f'http://{host}:{port}/',
        json={
            "date": "2022-08-09",
            "raw_dir": RAW_DIR
        }
    )
    assert resp.status_code == 201
    print("job1 completed!")


def run_job2(port, host):
    print("Starting job2:")
    resp = requests.post(
        url=f'http://{host}:{port}/',
        json={
            "raw_dir": RAW_DIR,
            "stg_dir": STG_DIR
        }
    )
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == '__main__':
    host = 'localhost'

    run_job1(JOB1_PORT, host)
    time.sleep(3)
    run_job2(JOB2_PORT, host)
