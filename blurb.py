from lxml import etree, html
from collections import defaultdict
from io import StringIO

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