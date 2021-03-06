#!/usr/bin/env python
# coding=utf-8

import threading
import sys
if sys.version >= '3':
    from queue import Queue
else:
    from Queue import Queue
from random import randint
from time import sleep as _sleep, time as _time
# from .util import Monitor as _Moniter

# 0/1/2/3/4 = init/working/finish/cancel/error
ST_INIT = 0
ST_WORKING = 1
ST_FINISHED = 2
ST_CANCEL = 3
ST_ERROR = 4

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
        self.__mutex = threading.RLock()
        self.log = log
        if self.log is None:
            self.log = _get_logger()

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


class ServeThreadBase(ThreadBase):
    def __init__(self, log=None):
        ThreadBase.__init__(self, log=log)
        self.__started = False  # set in self.run(), when service is available

    def serve(self, timeout=None):
        if not self.isAlive():
            self.start()
        if not timeout:
            while not self.isAvailable() and not self.isSetStop():
                pass
            return self.isAvailable()
        else:
            while timeout  > 0 and not self.isSetStop():
                if self.isAvailable():
                    return True
                timeout -= 0.5
                _sleep(0.5)
            return self.isAvailable()

    def isAvailable(self):
        return self.__started

    def _set_server_available(self, flag=True):
        self.__started = flag


class ThreadManager:
    def __init__(self, log=None):
        self.__threads = []
        self.__mutex = threading.RLock()
        self.__err_ev = threading.Event()
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
                th.setToStop()
                # if th.isAlive():
                #     th.setToStop()

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

    def info(self):
        info = dict()
        for th in self.__threads:
            if th.idel():
                info[th.getName()] = 'idel'
            else:
                info[th.getName()] = 'busy'
        return info


class WorkBase:
    def __init__(self, name='_work_', parent=None):
        self.name = name
        self.parent = parent
        self.__stop_ev = threading.Event()
        self.__stop_ev.clear()
        # 0/1/2/3/4 = init/working/finish/cancel/error
        self.__status = ST_INIT

    def work(self, this_thread, log):
        raise NotImplementedError('WorkBase')

    def setParent(self, parent):
        self.parent = parent

    @property
    def status(self):
        return self.__status

    def isProcessing(self): # working or just initialised
        return self.__status < ST_FINISHED and not self.isSetStop()

    def setToStop(self):
        self.__stop_ev.set()

    def isSetStop(self):
        return self.__stop_ev.isSet()

    def cancel(self):
        self.__stop_ev.set()

    @property
    def canceled(self):
        return self.__stop_ev.isSet()

    def waitForStop(self, timeout=None):
        while not timeout and self.__status < ST_FINISHED:
            _sleep(0.5)
            if timeout < 0:
                raise RuntimeError('Work timeout')
            timeout -= 0.5

    def _call_by_work_thread_run(self, this_thread, log):
        self.__stop_ev.clear()
        self.work(this_thread, log)
        self.__stop_ev.set()

    def _call_by_work_thread_set_status(self, status):
        self.__status = status


class WorkerThread(ServeThreadBase):
    def __init__(self, standalone=None, log=None):
        ServeThreadBase.__init__(self, log=log)
        self.__wk_qu = Queue()
        self.__ev = threading.Event()
        self.__curr_wk = None
        self.__busy = False
        self.__standalone = standalone

    def add_work(self, work):
        assert isinstance(work, WorkBase)
        self.__wk_qu.put(work)
        self.__ev.set()

    def idel(self):
        return self.isAvailable() and self.__wk_qu.empty() and not self.__busy

    def size(self):
        return self.__wk_qu.qsize()

    @property
    def isStandalone(self):
        return self.__standalone

    def setToStop(self):
        ThreadBase.setToStop(self)
        if self.__curr_wk:
            self.__curr_wk.setToStop()

    def run(self):
        self._set_server_available()
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
                    self.__curr_wk._call_by_work_thread_set_status(ST_WORKING)
                    self.__busy = True
                    self.__curr_wk._call_by_work_thread_run(this_thread=self, log=self.log)
                    self.__busy = False
                    self.__curr_wk._call_by_work_thread_set_status(ST_FINISHED)
                    self.__curr_wk = None
            except Exception as e:
                self.log.exception(e)
                self.__curr_wk._call_by_work_thread_set_status(ST_ERROR)
            finally:
                if self.__standalone and self.__wk_qu.empty():
                    break

        self._set_server_available(False)
        while not self.__wk_qu.empty():
            wk = self.__wk_qu.get()
            wk._call_by_work_thread_set_status(ST_CANCEL)
        # self.log.debug('%s, stop', self.getName())


class WorkDispatcher:
    def __init__(self, tmin, tmax, standalone_works=None, log=None):
        self.tmin = tmin
        self.tmax = tmax
        self.log =log
        self.mgr = ThreadManager()
        self.__mutex = threading.RLock()
        self.setToStop = self.mgr.stopAll
        self.isAlive = self.mgr.allAlive
        self.info = self.mgr.info
        self.__is_serving = False
        if standalone_works:
            self.addWorks(standalone_works, standalone=True)
        else:
            for i in range(self.tmin):
                self.mgr.addThreads([WorkerThread(log=log)])

    def __get_th(self, standalone=False):
        with self.__mutex:
            th = self.mgr.getIdleThread()
            if th:
                return th
            th_len = self.mgr.count()
            if th_len < self.tmax:
                self.log.warn('[ws] new work-line')
                new_th = WorkerThread(standalone=standalone, log=self.log)
                self.mgr.addThread(new_th)
                if self.__is_serving:
                    new_th.start()
                return new_th
            else:
                self.log.warn('[ws] all work-lines are busy')
                return self.mgr.getThread(randint(0, th_len-1))

    def addWork(self, work, standalone=False):
        if not isinstance(work, WorkBase):
            raise ValueError('not a Work class')
        self.__get_th(standalone=standalone).add_work(work=work)

    def addWorks(self, works, standalone=False):
        for work in works:
            self.addWork(work, standalone=standalone)

    def serve(self):
        self.mgr.startAll()
        self.__is_serving = True

    def joinAll(self, timeout=None):
        self.mgr.joinAll(timeout)
        self.__is_serving = False


class TaskBase:
    def __init__(self, parent=None, name='<task>',log=None):
        self.parent = parent
        self.name = name
        self.log = log
        self.__subworks = None
        self.__err_ev = threading.Event()
        # 0/1/2/3/4 init/processing/finish/canceled/error
        self.__status = ST_INIT

    # def makeSubWorks(self):
    #     """  """
    #     raise NotImplementedError()

    # def getSubWorks(self):
    #     if not self.__subworks:
    #         self.__subworks = self.makeSubWorks()
    #     return self.__subworks

    def addSubWorks(self, subworks):
        if self.__subworks is None:
            self.__subworks = []
        for sw in subworks:
            self.__subworks.append(sw)
        if not self.__subworks:
            pass

    @property
    def subWorks(self):
        return self.__subworks

    def cleanup(self):
        """ optional, server will do some cleanup work """
        pass

    def setParent(self, parent):
        self.parent = parent

    @property
    def status(self):
        return self.__status

    def _call_by_ws_set_status(self, status):
        self.__status = status
        # if self.__subtasks:
        #     for stsk in self.__subtasks:
        #         stsk._call_by_ws_set_status(status)

    def isArchived(self):
        if self.status == ST_INIT: # for sub tasks, witch status will not be set auto
            for sbwk in self.__subworks:
                if sbwk.status != ST_FINISHED:
                    return False
            return True
        else:
            return self.status == ST_FINISHED

    def isError(self):
        return self.status == ST_ERROR

    def setToStop(self):
        if self.__subworks:
            for work in self.__subworks:
                work.setToStop()

    def waitForStop(self):
        if self.__subworks:
            for work in self.__subworks:
                work.waitForStop()


# TODO: needs add setStop(force) or shutdown(), to finish all tasks before shutdown
class WorkShop(ServeThreadBase):
    def __init__(self, tmin=10, tmax=20, log=None):
        ServeThreadBase.__init__(self, log=log)
        self.__task_buff = Queue()
        self.__curr_tasks = []
        self.__wd = WorkDispatcher(tmin=tmin, tmax=tmax, log=log)
        self.__clean = WorkDispatcher(tmin=1, tmax=5, log=log)

    def addTasks(self, tasks):
        for task in tasks:
            self.addTask(task)

    def addTask(self, task):
        assert isinstance(task, TaskBase)
        if task.subWorks is None:
            raise ValueError('[wd] can not add task, subwork is None (task not initialised)')
        if not self.isAvailable():
            raise ValueError('[wd] can not add task, server is not available')
        if len(task.subWorks) == 0:
            # empty task
            task._call_by_ws_set_status(ST_FINISHED)
            self.__clean.addWork(WorkShop.SerWork(task))
            return
        self.__task_buff.put(task)
        self.log.debug('[ws] add a work: %s', task.name)

    def info(self):
        info = dict()
        info['buffering'] = self.__task_buff.qsize()
        info['running'] = len(self.__curr_tasks)
        info['threads'] = self.__wd.info()
        info['sys'] = self.__clean.info()
        return info

    def run(self):
        self.log.debug('[ws] start serving')
        self.__wd.serve()
        self.__clean.serve()
        self._set_server_available()
        # monitor = _Moniter(self.log)
        while not self.isSetStop():
            # monitor.report(self.info())
            curr_task = None
            start_at = _time()
            try:
                if not self.__task_buff.empty():
                    curr_task = self.__task_buff.get()
                    self.__curr_tasks.append(curr_task)
                    curr_task._call_by_ws_set_status(ST_WORKING)
                    self.__wd.addWorks(curr_task.subWorks)
                    self.log.debug('[ws] pop a Task: %s', curr_task.name)
                    curr_task = None

                for i, tk in enumerate(self.__curr_tasks):
                    wkarchvied = True
                    for wk in tk.subWorks:
                        if wk.status != ST_FINISHED:
                            wkarchvied = False
                        elif wk.status == ST_ERROR:
                            tk._call_by_ws_set_status(ST_ERROR)
                            tk.setToStop()
                            tk.waitForStop()
                            self.log.debug('[ws] Task err: %s', tk.name)
                            del self.__curr_tasks[i]
                        elif wk.status == ST_CANCEL:
                            tk._call_by_ws_set_status(ST_CANCEL)
                            tk.setToStop()
                            tk.waitForStop()
                            self.log.debug('[ws] Task canceled: %s', tk.name)
                            del self.__curr_tasks[i]
                    if wkarchvied:
                        tk._call_by_ws_set_status(ST_FINISHED)
                        self.log.debug('[ws] Task done: %s', tk.name)
                        del self.__curr_tasks[i]
                    if tk.status > ST_WORKING:
                        self.log.debug('[ws] cleanup')
                        self.__clean.addWork(WorkShop.SerWork(tk))
            except Exception as e:
                # TODO: fetal err, need handle and report
                if curr_task:
                    curr_task._call_by_ws_set_status(ST_ERROR)
                self.log.exception(e)
            finally:
                duration = _time() - start_at
                if duration < 0.8:
                    _sleep(0.5)

        self._set_server_available(flag=False)
        self.__wd.setToStop()
        self.__wd.joinAll()
        self.__cleanUp()
        self.__clean.setToStop()
        self.__clean.joinAll()
        self.log.debug('[ws] stop serving')

    def __cleanUp(self):
        for i, tk in enumerate(self.__curr_tasks):
            if tk.isError():
                tk._call_by_ws_set_status(ST_ERROR)
                tk.setToStop()
                tk.waitForStop()
                self.log.debug('[ws] Task err: %s', tk.name)
            elif tk.isArchived():
                tk._call_by_ws_set_status(ST_FINISHED)
                self.log.debug('[ws] Task done: %s', tk.name)
            else:
                tk._call_by_ws_set_status(ST_CANCEL)
                tk.setToStop()
                tk.waitForStop()
                self.log.debug('[ws] Task not finish: %s', tk.name)

            self.__clean.addWork(WorkShop.SerWork(tk))

        while not self.__task_buff.empty():
            tk = self.__task_buff.get()
            tk._call_by_ws_set_status(ST_CANCEL)
            self.__clean.addWork(WorkShop.SerWork(tk))
            self.log.debug('[ws] cleanup')

    def allTasksDone(self):
        return self.__task_buff.empty() and len(self.__curr_tasks) == 0

    def idel(self):
        return self.isAvailable() \
               and self.__task_buff.empty() \
               and len(self.__curr_tasks) == 0

    @property
    def currTaskSize(self):
        return len(self.__curr_tasks)

    class SerWork(WorkBase):
        def __init__(self, task):
            WorkBase.__init__(self, name='ws_ser')
            self.task = task

        def work(self, this_thread, log):
            self.task.cleanup()
            log.debug('[SerWork] cleanup')



# local test code

class WorkTest(WorkBase):
    TOTAL = 0
    EXEC_TOTAL = 0
    MUTEX = threading.RLock()
    @staticmethod
    def addme():
        with WorkTest.MUTEX:
            WorkTest.EXEC_TOTAL += 1

    def __init__(self, name):
        WorkBase.__init__(self, parent=None)
        self.name = name
        with WorkTest.MUTEX:
            WorkTest.TOTAL += 1

    def work(self, this_thread, log=None):
        WorkTest.addme()
        log.error('am work %s, am in %s', self.name, this_thread.getName())
        _sleep(1)


def wd_test(log):
    ws = WorkDispatcher(tmin=5, tmax=10, log=log)
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
            log.error('workers = %d', ws.mgr.count())
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
        ws.joinAll()
        for wk in works:
            log.error('[%s] status=%d', wk.name, wk.status)
        log.error('total=%d, count=%d', total, WorkTest.TOTAL)

def wd_test_1(log):
    wks = [WorkTest(name='work_%05d' % i) for i in range(20)]
    wd = WorkDispatcher(tmin=5, tmax=10, standalone_works=wks, log=log)
    try:
        wd.serve()
        wd.joinAll()
    except Exception as e:
        log.exception(e)
        raise
    finally:
        # _sleep(1)
        wd.setToStop()
        wd.joinAll()
        for wk in wks:
            log.error('[%s] status=%d', wk.name, wk.status)
        log.error('total=%d', WorkTest.TOTAL)


class TaskTest(TaskBase):
    TOTAL = 0
    EXEC_TOTAL = 0
    CLEANUP = 0
    MUTEX = threading.RLock()
    @staticmethod
    def addme():
        with TaskTest.MUTEX:
            TaskTest.EXEC_TOTAL += 1
    @staticmethod
    def clean():
        with TaskTest.MUTEX:
            TaskTest.CLEANUP += 1

    def __init__(self, sub_size, name, log):
        TaskBase.__init__(self, name=name, log=log)
        self.sub_size = sub_size
        with TaskTest.MUTEX:
            TaskTest.TOTAL += 1

    def makeSubWorks(self):
        TaskTest.addme()
        subworks = []
        for i in range(self.sub_size):
            subworks.append(WorkTest('t_%s_%d' % (self.name, i)))
        self.addSubWorks(subworks)
        _sleep(0.1)
        self.log.error(' ........... am Task %s, subworks=%d', self.name, len(subworks))

    def cleanup(self):
        TaskTest.clean()
        self.log.error(' ========== Task cleanup: %s, %d, %d' % (self.name, self.status, self.sub_size))


def ws_test(log=None):
    if log is None:
        import util
        log = util.get_logger()
    ws = WorkShop(tmin=5, tmax=20, log=log)
    i = 0
    total = 0
    tasks = []
    try:
        ws.serve()
        while True:
            task = TaskTest(randint(0, 10), name='T_%05d' % i, log=log)
            task.makeSubWorks()
            assert task.subWorks is not None
            # wk.cancel()
            ws.addTask(task)
            tasks.append(task)
            i += 1
            total += 1
            log.error(' ||||||||||||||| tasks = %d', ws.currTaskSize)
            if i < 190:
                _sleep(0.3)
            else:
                _sleep(0.6)
            if i > 200:
                break
    except Exception as e:
        log.exception(e)
        raise
    finally:
        # _sleep(1)
        ws.setToStop()
        ws.join()
        canceled_total = unknow_total = archived_total = err_total = 0
        for task in tasks:
            log.error('[%s] status=%d', task.name, task.status)
            if task.isArchived():
                archived_total += 1
            elif task.isError():
                err_total += 1
            elif task.status == 3:
                canceled_total += 1
            else:
                unknow_total += 1
            # if task.isArchived() == task.isError():
            #     _sleep(0.3)
            #     for wk in task.subworks:
            #         print wk.status
        log.error('TASK: total=%d, exec=%d, arc=%d, canc=%d, err=%d, un=%d, clean=%d',
                  total, TaskTest.EXEC_TOTAL, archived_total, canceled_total,
                  err_total, unknow_total, TaskTest.CLEANUP)
        log.error('WORK: total=%d, exec=%d', WorkTest.TOTAL, WorkTest.EXEC_TOTAL)
        assert unknow_total == 0
        assert TaskTest.CLEANUP == total
        assert archived_total + err_total + canceled_total == TaskTest.TOTAL
        # assert canceled_total == TaskTest.TOTAL - TaskTest.EXEC_TOTAL


if __name__ == "__main__":
    from .util import get_logger as _get_logger
    try:
        from vavava import util
        import logging
        log = util.get_logger(level=logging.DEBUG)
        # wd_test(log)
        # wd_test_1(log)
        ws_test(log)
    except KeyboardInterrupt as e:
        print('stop by user')
        exit(0)
