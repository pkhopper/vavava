#!/usr/bin/env python
# coding=utf-8

import threading
import Queue
from time import sleep as _sleep, time as _time

class ThreadBase:
    def __init__(self, log=None):
        self.__event = threading.Event()
        self.thread = threading.Thread(target=self.__run)
        self.running = False
        self.log = log

    def run(self):
        raise NotImplementedError()

    def isSetToStop(self):
        return self.__event.isSet()

    def __run(self, *_args, **_kwargs):
        self.running = True
        self.run()
        self.running = False

    def start(self):
        self.__event.clear()
        self.thread.start()

    def stop(self):
        self.__event.set()

    def isAlive(self):
        return self.running and self.thread.isAlive()

    def join(self, timeout=None):
        self.thread.join(timeout)

class ThreadManager:
    def __init__(self, log=None):
        self.threads = []
        self.started = False
        self.log = log
        self.mutex = threading.Lock()
        self.msg_queue = Queue.Queue()

    def addThreads(self, threads):
        assert threads
        with self.mutex:
            for th in threads:
                assert th
                self.threads.append(th)

    def length(self):
        with self.mutex:
            return len(self.threads)

    def startAll(self):
        for th in self.threads:
            if not th.isAlive():
                th.start()

    def stopAll(self):
        for th in self.threads:
            if th.isAlive():
                th.stop()

    def joinAll(self, timeout=None):
        for th in self.threads:
            if th.isAlive():
                th.join(timeout)

    def isWorking(self):
        for th in self.threads:
            if th.isAlive():
                return True
        return False

    def reset(self):
        self.stopAll()
        self.joinAll()
        self.threads = []


class WorkBase:
    def __init__(self):
        pass

    def work(self, log):
        raise NotImplementedError('WorkBase')


class WorkerThread(ThreadBase):
    def __init__(self, log=None):
        ThreadBase.__init__(self, log=log)
        self.mutex = threading.Lock()
        self.works = Queue.Queue()

    def add_work(self, work):
        self.works.put(work)

    def idel(self):
        return self.works.empty()

    def run(self):
        while not self.isSetToStop():
            try:
                if not self.works.empty():
                    worker = self.works.get(timeout=1)
                    if worker:
                        worker.work(log=self.log)
            except Exception as e:
                if self.log:
                    self.log.exception(e)
                else:
                    print e


class WorkShop:
    def __init__(self, tmin, tmax, log=None):
        self.tmin = tmin
        self.tmax = tmax
        self.log =log
        self.mgr = ThreadManager()
        self.mutex = threading.Lock()
        for i in xrange(self.tmin):
            self.mgr.addThreads([WorkerThread(log=log)])

    def __get_th(self):
        with self.mutex:
            for th in self.mgr.threads:
                if th.idel():
                    self.log.debug('__get_th() 1')
                    return th
            th_len = self.mgr.length()
            if th_len < self.tmax:
                new_th = WorkerThread(log=self.log)
                self.mgr.addThreads([new_th])
                new_th.start()
                self.log.debug('__get_th() 2')
                return new_th
            else:
                self.log.debug('__get_th() 3')
                return self.mgr.threads[(th_len % int(_time())) - 1]

    def add_work(self, work):
        if isinstance(work, WorkBase):
            self.__get_th().add_work(work=work)
        else:
            raise ValueError('not a Work class')

    def serve(self):
        self.log.debug('[shop] serve 1')
        self.mgr.startAll()

    def stop_service(self, timeout=None):
        self.log.debug('[shop] stop_service 1')
        self.mgr.stopAll()
        self.mgr.joinAll(timeout=timeout)
        self.log.debug('[shop] stop_service 2')


class TestWork(WorkBase):
    def __init__(self, name):
        WorkBase.__init__(self)
        self.name = name

    def work(self, log=None):
        print 'am work ', self.name
        _sleep(1)


def test_thread():
    import time, sys
    class TestThread(ThreadBase):
        def run(self):
            while not self.isSetToStop():
                sys.stdout.write(self.thread.name + '\n')
                sys.stdout.flush()
                time.sleep(1)

    mgr = ThreadManager()
    print '=============== start'
    for i in xrange(10):
        mgr.addThreads([TestThread()])
    print '=========== ', len(mgr.threads)
    try:
        mgr.startAll()
        while mgr.isWorking():
            mgr.joinAll(timeout=1)
            print 'main ========='
            time.sleep(1)
    except KeyboardInterrupt as e:
        print 'stopping'
        mgr.stopAll()
        mgr.joinAll()
        print 'stopped'
    print '=============== end'


def test_workshop(log):
    ws = WorkShop(tmin=5, tmax=10, log=log)
    ws.serve()
    i = 0
    try:
        while True:
            wk = TestWork(name='work_%05d' % i)
            ws.add_work(wk)
            # _sleep(0.5)
            i += 1
            print 'workers =', ws.mgr.length()
    except:
        ws.stop_service()
        raise


if __name__ == "__main__":
    try:
        from vavava import util
        import logging
        log = util.get_logger(level=logging.INFO)
        test_workshop(log)
    except KeyboardInterrupt as e:
        print 'stop by user'
        exit(0)
