import os

import numpy as np
import psycopg2


class opDB:
    host = 'db-ics'

    @staticmethod
    def passwd():
        pwpath = os.path.join(os.environ['ICS_SPSACTOR_DIR'],
                              "etc", "dbpasswd.cfg")
        try:
            file = open(pwpath, "r")
            return file.read()
        except:
            raise RuntimeError(f"could not get db password from {pwpath}")

    @staticmethod
    def connect():
        return psycopg2.connect(dbname='opdb', user='pfs', password=opDB.passwd(), host=opDB.host, port=5432)

    @staticmethod
    def fetchall(query):
        with opDB.connect() as conn:
            with conn.cursor() as curs:
                curs.execute(query)
                return np.array(curs.fetchall())

    @staticmethod
    def fetchone(query):
        with opDB.connect() as conn:
            with conn.cursor() as curs:
                curs.execute(query)
                return np.array(curs.fetchone())

    @staticmethod
    def update(query, kwargs):
        with opDB.connect() as conn:
            with conn.cursor() as curs:
                curs.execute(query, kwargs)
            conn.commit()

    @staticmethod
    def insert(table, **kwargs):
        fields = ', '.join(kwargs.keys())
        values = ', '.join(['%%(%s)s' % v for v in kwargs])
        query = f'INSERT INTO {table} ({fields}) VALUES ({values})'
        return opDB.update(query, kwargs)
