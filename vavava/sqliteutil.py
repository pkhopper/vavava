# -*- coding: utf-8 -*-

import os
import sqlite3


class Sqlite3Helper:
    def __init__(self, db_path):
        self.dbpath = db_path
        self.conn = None

    def get_connection(self):
        if os.path.isfile(self.dbpath) or os.path.islink(self.dbpath):
            pass
        else:
            with open(self.dbpath, 'w'):
                pass
        self.conn = sqlite3.connect(self.dbpath, timeout=3)

    def execute(self, sql, parameters=(), rollback=True):
        try:
            self.conn.execute(sql, parameters)
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)
        except Exception as e:
            raise e
        finally:
            self.conn.rollback()

    def fetch_one(self, sql, parameters=()):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.fetchone()
        except Exception as e:
            raise e
        finally:
            if cursor:
                cursor.close()

    def fetch_all(self, sql, parameters=()):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.fetchall()
        except Exception as e:
            raise e
        finally:
            if cursor:
                cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()
