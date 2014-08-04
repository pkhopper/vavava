#!/usr/bin/env python
# coding=utf-8

import threading
import Queue
from random import randint
from time import sleep as _sleep


class ThreadBase:

    def __init__(self, log=None):
        self._thread = threading.Thread(target=self.__run)
        self.getName = self._thread.getName
        self.ident = self._thread.ident
        self.isDaemon = self._thread.isDaemon
        self.setDaemon = self._thread.setDaemon
        self.setName = self._thread.setName
        self.__stop_ev = threading.Event()
        self.__not_pause_ev = threading.Event()
        self.__not_pause_ev.set()
        self.__running = False
        self.__mutex = threading.Lock()
        self.log = log

    def start(self):
        self._thread.start()
        self.__stop_ev.clear()

    def run(self):
        raise NotImplementedError()

    def isRunning(self):
        return self.__running

    def isAlive(self):
        return self.__running and self._thread.isAlive()

    def pause(self):
        self.__not_pause_ev.clear()

    def isPaused(self):
        return not self.__not_pause_ev.isSet()

    def waitForResume(self, timeout=None):
        return self.__not_pause_ev.wait(timeout)

    def resume(self):
        self.__not_pause_ev.set()

    def setToStop(self):
        self.__stop_ev.set()
        if self.isPaused():
            self.resume()

    def isSetStop(self):
        return self.__stop_ev.isSet()

    def join(self, timeout=None):
        self._thread.join(timeout)

    def __run(self, *_args, **_kwargs):
        self.__running = True
        try:
            self.run()
        finally:
            self.__running = False


class ThreadManager:
    def __init__(self, log=None):
        self.__threads = []
        self.__mutex = threading.Lock()
        self.__err_ev = threading.Event()
        self.msg_queue = Queue.Queue()
        self.log = log

    def addThreads(self, threads):
        assert threads
        with self.__mutex:
            for th in threads:
                self.__threads.append(th)

    def addThread(self, thread):
        with self.__mutex:
            self.__threads.append(thread)

    def count(self):
        with self.__mutex:
            return len(self.__threads)

    def startAll(self):
        with self.__mutex:
            for th in self.__threads:
                if not th.isAlive():
                    th.start()

    def pauseAll(self):
        with self.__mutex:
            for th in self.__threads:
                if not th.isPaused():
                    th.pause()

    def resumeAll(self):
        with self.__mutex:
            for th in self.__threads:
                if th.isPaused():
                    th.resume()
                if not th.isAlive():
                    th.start()

    def stopAll(self):
        with self.__mutex:
            for th in self.__threads:
                if th.isAlive():
                    th.setToStop()

    def joinAll(self, timeout=None):
        for th in self.__threads:
            if th.isAlive():
                th.join(timeout)

    def allAlive(self):
        for th in self.__threads:
            if th.isAlive():
                return True
        return False

    def reset(self):
        self.stopAll()
        self.joinAll()
        self.__threads = []

    def getIdleThread(self):
        for th in self.__threads:
            if th.idel():
                return th

    def getThread(self, seq):
        return self.__threads[seq]


class WorkBase:
    def __init__(self, name='_work_'):
        self.name = name
        self.__stop_ev = threading.Event()
        self.__stop_ev.clear()
        # 0/1/2/3/4 = init/working/finish/cancel/error
        self.__wkstatus = 0

    def work(self, this_thread, log):
        raise NotImplementedError('WorkBase')

    @property
    def status(self):
        return self.__wkstatus

    def isProcessing(self):
        return self.__wkstatus < 2 and not self.isSetStop()

    def setToStop(self):
        # print 'this work set to stop'
        self.__stop_ev.set()

    def isSetStop(self):
        return self.__stop_ev.isSet()

    def cancel(self):
        self.__stop_ev.set()

    @property
    def canceled(self):
        return self.__stop_ev.isSet()

    def waitForFinish(self, timeout=10):
        while not timeout and self.__wkstatus < 2:
            _sleep(0.5)
            if timeout < 0:
                raise RuntimeError('HttpDLSubWork timeout')
            timeout -= 0.5

    def _call_by_work_thread_run(self, this_thread, log):
        self.__stop_ev.clear()
        self.__wkstatus = 1
        self.work(this_thread, log)
        self.__stop_ev.set()
        self.__wkstatus = 2

    def _call_by_work_thread_set_status(self, status):
        """
        @param status: 0/1/2/3/4 = init/working/finish/cancel/error
        """
        self.__wkstatus = status


class TaskBase:
    def __init__(self, log=None, callback=None):
        self.callback = callback
        self.log = log

        self.__subworks = []
        self.__err_ev = threading.Event()
        # self.__err_ev.clear()
        # 0/1/2/3 init/processing/finish/error
        self.__status = 0

    def makeSubWorks(self):
        raise NotImplementedError()
        # return self.__subworks

    def getSubWorks(self):
        self.makeSubWorks()
        self.__status = 0

    def isArchived(self):
        if self.__status < 2 or self.isErrorHappen():
            return False
        for work in self.__subworks:
            if work.isProcessing():
                return False
        return True

    def setError(self):
        self.__err_ev.set()

    def isErrorHappen(self):
        return self.__err_ev.isSet()

    def setToStop(self):
        for work in self.__subworks:
            work.setToStop()

    def waitForFinish(self):
        for work in self.__subworks:
            work.waitForFinish()

    def cleanup(self):
        if self.callback:
            self.callback(self)


class WorkerThread(ThreadBase):
    def __init__(self, log):
        ThreadBase.__init__(self, log=log)
        # self.mutex = threading.Lock()
        self.__wk_qu = Queue.Queue()
        self.__ev = threading.Event()
        self.__curr_wk = None

    def add_work(self, work):
        assert isinstance(work, WorkBase)
        self.__wk_qu.put(work)
        self.__ev.set()

    def idel(self):
        return self.__wk_qu.empty()

    def setToStop(self):
        ThreadBase.setToStop(self)
        if self.__curr_wk:
            self.__curr_wk.setToStop()

    def run(self):
        while not self.isSetStop():
            if not self.waitForResume(1):
                continue
            if not self.__ev.wait(1):
                continue
            if self.__wk_qu.empty():
                self.__ev.clear()
                continue
            self.__curr_wk = self.__wk_qu.get()
            try:
                if self.__curr_wk.canceled:
                    self.log.debug('[wkth] canceled a work')
                    continue
                if self.__curr_wk:
                    self.__curr_wk._call_by_work_thread_run(this_thread=self, log=self.log)
                    self.__curr_wk._call_by_work_thread_set_status(2)
                    self.__curr_wk = None
            except Exception as e:
                self.log.exception(e)
                self.__curr_wk._call_by_work_thread_set_status(4)

        self.cleanup()

    def cleanup(self):
        while not self.__wk_qu.empty():
            # ???? add_work() will make this unstoppable
            wk = self.__wk_qu.get()
            wk._call_by_work_thread_set_status(3)


class WorkShop:
    def __init__(self, tmin, tmax, log=None):
        self.tmin = tmin
        self.tmax = tmax
        self.log =log
        self.mgr = ThreadManager()
        self.__mutex = threading.Lock()
        for i in range(self.tmin):
            self.mgr.addThreads([WorkerThread(log=log)])
        self.serve = self.mgr.startAll
        self.setToStop = self.mgr.stopAll
        self.join = self.mgr.joinAll
        self.isAlive = self.mgr.allAlive

    def __get_th(self):
        with self.__mutex:
            th = self.mgr.getIdleThread()
            if th:
                return th
            th_len = self.mgr.count()
            if th_len < self.tmax:
                self.log.warn('[ws] new work-line')
                new_th = WorkerThread(log=self.log)
                self.mgr.addThread(new_th)
                new_th.start()
                return new_th
            else:
                self.log.warn('[ws] all work-lines are busy')
                return self.mgr.getThread(randint(0, th_len-1))

    def addWork(self, work):
        if not isinstance(work, WorkBase):
            raise ValueError('not a Work class')
        self.__get_th().add_work(work=work)

    def addWorks(self, works):
        for work in works:
            self.addWork(work)


class WorkTest(WorkBase):
    TOTAL = 0
    MUTEX = threading.Lock()
    @staticmethod
    def addme():
        with WorkTest.MUTEX:
            WorkTest.TOTAL += 1

    def __init__(self, name):
        WorkBase.__init__(self)
        self.name = name

    def work(self, this_thread, log=None):
        WorkTest.addme()
        log.debug('am work %s, am in %s', self.name, this_thread.getName())
        _sleep(1)


def thread_test():
    import time, sys
    class TestThread(ThreadBase):
        def run(self):
            while not self.isSetStop():
                sys.stdout.write(self._thread.name + '\n')
                sys.stdout.flush()
                time.sleep(1)

    mgr = ThreadManager()
    print '=============== start'
    for i in range(0,1):
        mgr.addThreads([TestThread()])
    print '=========== ', len(mgr.__threads)
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


def workshop_test(log):
    ws = WorkShop(tmin=5, tmax=10, log=log)
    i = 0
    total = 0
    works = []
    try:
        ws.serve()
        while True:
            wk = WorkTest(name='work_%05d' % i)
            # wk.cancel()
            ws.addWork(wk)
            works.append(wk)
            if i > 50:
                ws.mgr.pauseAll()
            if i > 100:
                ws.mgr.resumeAll()
            i += 1
            total += 1
            log.debug('workers = %d', ws.mgr.count())
            if i > 200:
                break
            if i < 190:
                _sleep(0.3)
    except Exception as e:
        log.exception(e)
        raise
    finally:
        # _sleep(1)
        ws.setToStop()
        ws.join()
        for wk in works:
            log.debug('[%s] status=%d', wk.name, wk.status)
        log.debug('total=%d, count=%d', total, WorkTest.TOTAL)


if __name__ == "__main__":
    try:
        from vavava import util
        import logging
        log = util.get_logger(level=logging.DEBUG)
        workshop_test(log)
        # thread_test()
    except KeyboardInterrupt as e:
        print 'stop by user'
        exit(0)
