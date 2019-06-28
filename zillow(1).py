
import time
import redis


if __name__ == '__main__':
    re_queue = redis.Redis(host='47.106.140.94', port='6486')
    count = 0
    while True:
        count += 1

        time.sleep(4000)
