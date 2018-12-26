import io

import requests


def download_file(url):
    r = requests.get(url, allow_redirects=True)
    if (r.status_code == 200):
        return io.BytesIO(r.content)
    else:
        raise ConnectionError("Could not download file from %s" % (url))
