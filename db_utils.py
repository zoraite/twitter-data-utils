__author__ = 'igobrilhante'

import psycopg2
import numpy as np

DSN = "dbname=igo"

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def query(q):
    conn = psycopg2.connect(DSN)

    curs = conn.cursor()
    curs.execute(q)
    res = curs.fetchall()
    r = []
    for e in res:
        r.append(e)

    return r
