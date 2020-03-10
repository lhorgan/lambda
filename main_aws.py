from urllib3 import PoolManager
import random
import sys
import time
import math

def lambda_handler(event, context):
    start = time.time() * 1000

    if "url" in event:
        user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"]

        url = event["url"]
        headers = {"User-Agent": random.choice(user_agents), "Connection": "keep-alive", "Accept-Language": "en-US", "Accept": "*/*"}

        try:
            pool = PoolManager()
            r = pool.request("HEAD", url, headers=headers, timeout=3.5)

            if r.status < 400:
                diff = int((time.time() * 1000 - start))
                return {"error": "false", "url": r.geturl(), "orig_url": url, "diff": diff}
            else:
                diff = int((time.time() * 1000 - start))
                return {"error": "true", "message": r.status, "orig_url": url, "url": r.geturl(), "diff": diff}
        except:
            e = sys.exc_info()[0]
            diff = int((time.time() * 1000 - start))
            return {"error": "true", "message": str(e), "orig_url": url, "diff": diff}
    elif "ip" in event:
        try:
            pool = PoolManager()
            r = pool.request("GET", "http://checkip.amazonaws.com/")
            if r.status == 200:
                diff = int((time.time() * 1000 - start))
                return {"error": "false", "ip": r.data.decode("utf-8"), "time": diff}
            else:
                diff = int((time.time() * 1000 - start))
                return {"error": "true", "message": r.status, "time": diff}
        except:
            e = sys.exc_info()[0]
            diff = int((time.time() * 1000 - start))
            return {"error": "true", "message": str(e), "diff": diff}