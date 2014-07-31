#!/usr/bin/env python
# coding=utf-8

import threading
import Queue
from time import sleep as _sleep, time as _time


class ThreadBase:

    def __init__(self, log=None):
        self.__event = threading.Event()
        self.thread = threading.Thread(target=self.__run)
        self.getName = self.thread.getName
        self.ident = self.thread.ident
        self.isDaemon = self.thread.isDaemon
        self.setDaemon = self.thread.setDaemon
        self.setName = self.thread.setName
        self.__running = False
        self.__mutex = threading.Lock()
        self.log = log

    def start(self):
        self.__event.clear()
        with self.__mutex:
            self.__running = True
        self.thread.start()

    def run(self):
        raise NotImplementedError()

    def isRunning(self):
        with self.__mutex:
            return self.__running

    def isAlive(self):
        return self.isRunning() and self.thread.isAlive()

    def setToStop(self):
        self.__event.set()

    def isSetToStop(self):
        return self.__event.isSet()

    def join(self, timeout=None):
        self.thread.join(timeout)

    def __run(self, *_args, **_kwargs):
        with self.__mutex:
            self.__running = True
        self.run()
        with self.__mutex:
            self.__running = False


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
                th.setToStop()

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

    def work(self, this_thread, log):
        raise NotImplementedError('WorkBase')


class WorkerThread(ThreadBase):
    def __init__(self, log):
        ThreadBase.__init__(self, log=log)
        self.mutex = threading.Lock()
        self.works = Queue.Queue()

    def add_work(self, work):
        assert isinstance(work, WorkBase)
        self.works.put(work)

    def idel(self):
        return self.works.empty()

    def run(self):
        while not self.isSetToStop():
            start_at = _time()
            try:
                if not self.works.empty():
                    self.log.debug('[wt] get a work')
                    worker = self.works.get()
                    if worker:
                        worker.work(this_thread=self, log=self.log)
            except Exception as e:
                self.log.exception(e)
            duration = _time() - start_at
            if duration < 1:
                _sleep(1)

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
                    return th
            th_len = self.mgr.length()
            if th_len < self.tmax:
                new_th = WorkerThread(log=self.log)
                self.mgr.addThreads([new_th])
                new_th.start()
                return new_th
            else:
                return self.mgr.threads[(th_len % int(_time())) - 1]

    def addWork(self, work):
        if not isinstance(work, WorkBase):
            raise ValueError('not a Work class')
        self.__get_th().add_work(work=work)

    def addWorks(self, works):
        for work in works:
            self.addWork(work)

    def serve(self):
        self.mgr.startAll()

    def setShopClose(self):
        self.mgr.stopAll()

    def waitShopClose(self, timeout=None):
        self.mgr.joinAll(timeout=timeout)

    def isShopClosed(self):
        with self.mutex:
            for th in self.mgr.threads:
                if th.isAlive():
                    return False
        return True


class TestWork(WorkBase):
    def __init__(self, name):
        WorkBase.__init__(self)
        self.name = name

    def work(self, this_thread, log=None):
        log.debug('am work %s, am in %s', self.name, this_thread.getName())
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
    for i in xrange(0,1):
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
    i = 0
    try:
        ws.serve()
        while not ws.isShopClosed():
            wk = TestWork(name='work_%05d' % i)
            ws.addWork(wk)
            _sleep(0.5)
            i += 1
            log.debug('workers = %d', ws.mgr.length())
    except Exception as e:
        log.exception(e)
    finally:
        ws.setShopClose()
        ws.waitShopClose()
        raise


if __name__ == "__main__":
    try:
        from vavava import util
        import logging
        log = util.get_logger(level=logging.DEBUG)
        test_workshop(log)
        # test_thread()
    except KeyboardInterrupt as e:
        print 'stop by user'
        exit(0)
