from urllib3 import PoolManager
from urllib3.util.retry import Retry
import random
import sys
import time
import math

from collections import defaultdict
from lxml import html
from io import StringIO

def lambda_handler(event, context):
    start = time.time() * 1000

    if "url" in event:
        user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"]

        url = event["url"]
        headers = {"User-Agent": random.choice(user_agents), "Connection": "keep-alive", "Accept-Language": "en-US", "Accept": "*/*"}
        timeout = 3.5
        method = "GET"
        retry = Retry(total=2)
        max_bytes = 10**6
        
        if "headers" in event:
            headers = event["headers"]
        if "timeout" in event:
            timeout = event["timeout"]
        if "method" in event:
            method = event["method"]
        if "retries" in event:
            retry = Retry(event["retry"])
        if "max_bytes" in event:
            max_bytes = event["max_bytes"]
        retries = 3

        pool = PoolManager()
        try:
            r = pool.request(method, url, headers=headers, preload_content=False, timeout=timeout, retries=retries)
        except:
            e = sys.exc_info()[0]
            diff = int((time.time() * 1000 - start))
            return {"error": "true", "orig_url": url, "message": str(e), "diff": diff}
        if r.status >= 400:
            diff = int((time.time() * 1000 - start))
            return {"error": "true", "orig_url": url, "message": r.status, "diff": diff}
        
        amount_read = 0
        cutoff = "false"
        chunks = []
        for chunk in r.stream():
            amount_read += len(chunk)
            try:
                chunks.append(chunk.decode('utf-8'))
            except UnicodeDecodeError as e:
                diff = int((time.time() * 1000 - start))
                return {"error": "true", "orig_url": url, "message": "Unicode decode error!", "status": r.status, "bytes": amount_read, "diff": diff, "json": {}}
                
            if amount_read > max_bytes:
                cutoff = "true"
                break

        data = "".join(chunks)
        #print(data)
        blurb=None
        diff = int((time.time() * 1000 - start))
        blurb = get_blurb(data)
        
        return {"error": "false", "orig_url": url, "bytes": amount_read, "status": r.status, "cutoff": cutoff, "diff": diff, "json": blurb}
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

def get_blurb(content):
    doc = html.parse(StringIO(content))
    data = defaultdict(dict)
    props = doc.xpath('//meta[re:test(@name|@property, "^twitter|og:.*$", "i")]',
                  namespaces={"re": "http://exslt.org/regular-expressions"})


    for prop in props:
        if prop.get('property'):
            key = prop.get('property').split(':')
        else:
            key = prop.get('name').split(':')
        
        if prop.get('content'):
            value = prop.get('content')
        else:        
            value = prop.get('value')
        
        if not value:
            continue
        value = value.strip()
        
        if value.isdigit():
            value = int(value)
        
        ref = data[key.pop(0)]
        
        for idx, part in enumerate(key):
            if not key[idx:-1]: # no next values
                ref[part] = value
                break
            if not ref.get(part):
                ref[part] = dict()
            else:
                if isinstance(ref.get(part), str):
                    ref[part] = {'url': ref[part]}
            ref = ref[part]
    
     
    blurb = {}
    for prop in props:
        blurb[prop.get("property")] = prop.get("content")

    return blurb