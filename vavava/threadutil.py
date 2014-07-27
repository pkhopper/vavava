#!/usr/bin/env python
# coding=utf-8

import threading

class ThreadBase:
    def __init__(self):
        self.__event = threading.Event()
        self.thread = threading.Thread(target=self.__run)
        self.running = False

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
    def __init__(self, threads=None):
        self.min, self.max = min, max
        self.threads = threads
        self.started = False

    def addThreads(self, threads):
        if self.threads:
            self.threads += threads
        else:
            self.threads = threads

    def startAll(self):
        for th in self.threads:
            if not th.isAlive():
                th.start()

    def stopAll(self):
        for th in self.threads:
            if th.isAlive():
                th.stop()

    def joinAll(self, timeout=1):
        if timeout:
            timeout = 1.0*timeout/len(self.threads)
        for th in self.threads:
            th.join(timeout)

    def isWorking(self):
        for th in self.threads:
            if th.isAlive():
                return True
        return False


def main():
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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        print 'stop by user'
        exit(0)
