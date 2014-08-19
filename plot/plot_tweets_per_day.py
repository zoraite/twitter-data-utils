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

fig.autofmt_xdate()

fmt = mdates.DateFormatter('%Y-%m-%d')
loc = mdates.WeekdayLocator(byweekday=mdates.SUNDAY)

axes.xaxis.major.formatter._useMathText = True
axes.yaxis.major.formatter._useMathText = True
axes.xaxis.set_major_formatter(fmt)
axes.xaxis.set_major_locator(loc)


markers = ['o', '^', 's', 'D', 'o', "p", '8', '*']
marker_size = 10
line_width = 0.0
alpha = 0.8

colors = brewer2mpl.get_map('YlGnBu', 'sequential', 3).mpl_colors
i = 0

query = "select d, count(*) " \
        "from " \
        "(" \
        "select extract(year from created_at) " \
        "	|| '-' || extract(month from created_at) " \
        "	|| '-' || extract(day from created_at) as d  from twitter.pisa_geo_ref_tweets_all " \
        "where place_name = 'Lucca' " \
        ") a " \
        "group by d " \
        "order by d::timestamp "

res = db_utils.query(query)


x = [  mdates.strpdate2num('%Y-%m-%d')(r[0]) for r in res ]

y = [ r[1] for r in res ]

plt.bar(x, y, 0.5, alpha=alpha)

fig.tight_layout()

plt.show()






