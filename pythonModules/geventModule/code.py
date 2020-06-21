import gevent
from gevent import monkey; monkey.patch_all()
import requests


def get_data(link):
    project = requests.get(link)

links = ['link 1, link 2, link 3, link 4, link 5']
pool = []
for link in links:
    g = gevent.Greenlet(get_data, link)
    g.start()
    pool.append(g)
gevent.joinall(pool)

# Above 5 links will be processed almost concurrently without much CPU overhead.
