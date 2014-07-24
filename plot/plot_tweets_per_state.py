__author__ = 'igobrilhante'


import db_utils

import matplotlib.pyplot as plt
import numpy as np
from mpltools import style
import brewer2mpl
from mpltools import layout
import matplotlib.dates as mdates

style.use('ggplot')

figsize = layout.figaspect(scale=1.2)
fig, axes = plt.subplots(figsize=figsize)

markers = ['o', '^', 's', 'D', 'o', "p", '8', '*']
marker_size = 10
line_width = 0.0
alpha = 0.8

colors = brewer2mpl.get_map('YlGnBu', 'sequential', 3).mpl_colors
i = 0

query = "select uf, count(distinct id_tweet_str) " \
        "from twitter.brasil_geo_ref_tweets t, brasil.estados e " \
        "where " \
        "st_contains(e.the_geom, st_setsrid(st_makepoint(longitude, latitude), 4326)) = True " \
        "and created_at < '2014-06-11'::timestamp " \
        "and created_at >= '2014-06-10'::timestamp " \
        "group by uf " \
        "order by uf"

res = db_utils.query(query)


x = np.arange(0, len(res), 1)

y = [ r[1] for r in res ]
labels = [ r[0] for r in res ]

axes.bar(x, y, 0.5, alpha=alpha)
axes.set_xticks(x)
axes.set_xticklabels( labels )
plt.xlabel('State')
plt.ylabel('#tweets')

fig.tight_layout()

plt.show()






