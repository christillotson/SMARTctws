import requests
"""
Functions to assist with making requests
"""
def make_request(in_url, out_path):
    URL = “json url”
    Out = “name.json”

    With open(out, ‘wb’) as f:
    R = requests.get(url)
    f.write(r.content)



def make_request(request_url = )