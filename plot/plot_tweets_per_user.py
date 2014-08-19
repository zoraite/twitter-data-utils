__author__ = 'igobrilhante'


import db_utils

import matplotlib.pyplot as plt
import numpy as np
from mpltools import style
import brewer2mpl
from mpltools import layout
import matplotlib.dates as mdates

style.use('ggplot')

figsize = layout.figaspect(scale=0.8)
fig, axes = plt.subplots(figsize=figsize)


axes.xaxis.major.formatter._useMathText = True
axes.yaxis.major.formatter._useMathText = True

axes.set_yscale('log')
axes.set_xscale('log')

markers = ['o', '^', 's', 'D', 'o', "p", '8', '*']
marker_size = 10
line_width = 0.0
alpha = 0.8

colors = brewer2mpl.get_map('YlGnBu', 'sequential', 3).mpl_colors
i = 0

query = "select tweets, count(*) " \
        "from " \
        "(select t.user_id_str, count(distinct id_tweet_str) tweets " \
        "from twitter.hpc_brasil_geo_ref_tweets t, brasil.estados e " \
        "where " \
        "st_contains(e.the_geom, st_setsrid(st_makepoint(longitude, latitude), 4326)) = True " \
        "and created_at < '2014-06-11'::timestamp " \
        "and created_at >= '2014-06-10'::timestamp " \
        "group by t.user_id_str ) a " \
        "group by tweets " \
        "order by count"

res = db_utils.query(query)


x = [ r[0] for r in res ]
y = [ r[1] for r in res ]

plt.plot(x, y, marker='o', markersize=8, linewidth=0.4, markeredgewidth=0.0)
plt.xlabel('#tweets')
plt.ylabel('#users')

#
fig.tight_layout()
#
plt.show()






