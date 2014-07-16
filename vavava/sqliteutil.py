#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import Queue
import sqlite3
import threading


class DBBase:
    def __init__(self, db_path):
        self.db_path = db_path
        if not os.path.isfile(db_path):
            open(db_path, 'w')

    def get_connection(self):
        self.conn = sqlite3.connect(self.db_path, timeout=3)

    def excute(self, sql):
        self.conn.execute(sql)

    def fetch_one(self, sql):
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            re = cursor.fetchone()
            cursor.close()
            return re
        except Exception as e:
            if cursor: cursor.close()
            print e.message

    def fetch_all(self, sql):
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            re = cursor.fetchall()
            cursor.close()
            return re
        except Exception as e:
            if cursor: cursor.close()
            print e.message

class WorkBase:
    def handle(self, db):
        raise NotImplementedError()

class dbpool(threading.Thread):
    def __init__(self, path, cls=DBBase):
        threading.Thread.__init__(self)
        self.daemon = True
        self.db = None
        self.path = path
        self.cls = cls
        self.que = Queue.Queue()
        self.ev = threading.Event()
        self.start()

    def queue_work(self, work):
        self.que.put(work)

    def run(self):
        self.db = self.cls(self.path)
        while not self.ev.isSet():
            if self.que.qsize() == 0:
                self.ev.clear()
                continue
            dbop = self.que.get(timeout=3)
            if dbop:
                dbop.handle(self.db)

    def stop(self):
        self.ev.set()

if __name__ == "__main__":
    import time
    class testwork(WorkBase):
        def __init__(self):
            self.name = 'aaa'
        def handle(self):
            print 'yes ', self.name
    pool = dbpool(None)
    for i in range(10):
        pool.queue_work(testwork())
        time.sleep(1)
