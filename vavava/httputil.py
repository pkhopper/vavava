#!/usr/bin/env python
# coding=utf-8

import sys
import os
import gzip
import zlib
import urllib
import urllib2
import cookielib
from io import BytesIO
from time import time as _time
from threading import Event as _Event
from threading import Lock as _Lock
from socket import timeout as _socket_timeout
import threadutil

DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ' \
                     'en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
DEFAULT_REFERER = "http://www.baidu.com/"
DEFAULT_BUFFER_SIZE = 1024*512
DEFAULT_CHARSET = "utf8"
DEFAULT_TIMEOUT = 30  # MS
DEFAULT_DEBUG_LVL = 0


class HttpUtil(object):
    """ a simple client of http"""
    def __init__(self, timeout=DEFAULT_TIMEOUT, debug_lvl=DEFAULT_DEBUG_LVL, proxy=None):
        self._cookie = cookielib.CookieJar()
        self._timeout = timeout
        self._proxy = proxy
        self._opener = None
        self._buffer_size = DEFAULT_BUFFER_SIZE
        self._charset = DEFAULT_CHARSET
        self._set_debug_level(debug_lvl)
        self._headers = {'Referer': DEFAULT_REFERER}
        self._headers['User-Agent'] = DEFAULT_USER_AGENT
        self._headers['Connection'] = 'keep-alive'
        self._headers['Accept'] = r'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        self._headers['Cache-Control'] = 'max-age=0'

    def get(self, url, headers=None):
        self.response = self._request(url, headers=headers)
        return self.response.read()

    def post(self, url, post_dic, headers=None):
        post_data = urllib.urlencode(post_dic).encode(self._charset)
        self.response = self._request(url, headers=headers, post_data=post_data)
        return self.response.read()

    def fetch(self, url, handler, post_data=None, headers=None):
        if handler is None:
            raise ValueError("need handler")
        handler.set_parent(self)
        resp = self._request(url, post_data=post_data, headers=headers)
        handler.handle(req=None, resp=resp)

    def parse_charset(self):
        if self.response:
            import re
            charset = re.search(r'charset=([\w-]+)', self.response.headers['content-type'])
            if not charset:
                raise ValueError('content-type filed not found.')
            return charset.group(1)

    def add_header(self, key, value):
        self._headers[key] = value

    def set_proxy(self, proxy):
        self._proxy = proxy

    def _set_debug_level(self, level=0):
        from httplib import HTTPConnection
        HTTPConnection.debuglevel = level
        self._debug_lvl = level

    def _request(self, url, post_data=None, headers=None):
        if self._opener is None:
            self._opener = urllib2.build_opener(ContentEncodingProcessor())
        if self._cookie is not None:
            self._opener.add_handler(urllib2.HTTPCookieProcessor(self._cookie))
        if self._proxy is not None:
            self._opener.add_handler(urllib2.ProxyHandler(self._proxy))
        if headers:
            for k, v in self._headers.items():
                if not headers.has_key(k):
                    headers[k] = v
        else:
            headers = self._headers
        req = urllib2.Request(url, data=post_data, headers=headers)
        return self._opener.open(req, timeout=self._timeout)


class DownloadStreamHandler:
    def __init__(self, fp, duration=0, start_at=None, end_at=None,
                 mutex=None, callback=None):
        self.parent = None
        self.start_at = start_at
        self.end_at = end_at
        self.fp = fp
        self.duration = duration
        self.ev = _Event()
        self.stop = self.syn_stop = lambda: self.ev.set()
        self.mutex = mutex
        self.callback = callback
        if duration > 0:
            self.stop_time = duration + _time()

    def set_parent(self, p):
        self.parent = p

    def handle(self, req, resp):
        self.ev.clear()
        while not self.ev.is_set():
            if self.duration > 0 and self.stop_time <= _time():
                break
            if self.start_at and self.start_at >= self.end_at:
                break
            data = resp.read(DEFAULT_BUFFER_SIZE)
            if not data:
                break
            if self.mutex:
                with self.mutex:
                    if not self.fp.closed:
                        self.fp.seek(self.start_at)
                        self.start_at += len(data)
                        self.fp.write(data)
            elif not self.fp.closed:
                self.fp.write(data)
            if self.callback:
                self.callback(self.start_at - len(data), data)


class ContentEncodingProcessor(urllib2.BaseHandler):
    """A handler to add gzip capabilities to urllib2 requests """
    # add headers to requests

    def __init__(self):
        try:
            self.deflate = lambda data: zlib.decompress(data, -1*zlib.MAX_WBITS)
        except zlib.error:
            self.deflate = lambda data: zlib.decompress(data)

    def http_request(self, req):
        req.add_header("Accept-Encoding", "gzip,deflate")
        return req

    def http_response(self, req, resp):
        old_resp = resp
        if resp.headers.get("content-encoding") == "gzip":
            gz = gzip.GzipFile(fileobj=BytesIO(resp.read()), mode="r")
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
            # deflate
        if resp.headers.get("content-encoding") == "deflate":
            gz = BytesIO(self.deflate(resp.read()))
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
        return resp


class MiniAxel(HttpUtil):
    def __init__(self, progress_bar=None, retransmission=True, timeout=DEFAULT_TIMEOUT,
                 debug_lvl=DEFAULT_DEBUG_LVL, proxy=None):
        HttpUtil.__init__(self, timeout=timeout, debug_lvl=debug_lvl, proxy=proxy)
        self.threads = []
        self.progress_bar = progress_bar
        self.retransmission = retransmission

    def dl(self, url, fp, headers=None, n=5):
        assert n > 0
        if n == 1:
            self.fetch(url, DownloadStreamHandler(fp), headers=headers)
            return
        self.response = self._request(url)
        # assert self.response.code == 200
        info = self.response.info()
        size = int(info.getheaders('Content-Length')[0])
        assert info.getheaders('Accept-Ranges')[0] == 'bytes'
        clips = []
        clip_size = size/n
        for i in xrange(0, n-1):
            clips.append((i*clip_size, i*clip_size+clip_size-1))
        clips.append(((n-1)*clip_size, size))
        mutex = _Lock()
        cur_size = 0
        if self.retransmission:
            self.history_file = HistoryFile(fp.name)
            clips, cur_size = self.history_file.reindex(clips, size)
        if self.progress_bar:
            self.progress_bar.reset(size, cur_size)
        self.mgr = threadutil.ThreadManager()
        for clip in clips:
            thread = MiniAxel.DownloadThread(url=url, fp=fp, start_at=clip[0],
                                             end_at=clip[1], mutex=mutex,
                                             callback=self.__callback)
            self.mgr.addThreads([thread])
        try:
            self.mgr.startAll()
            while self.mgr.isWorking():
                self.mgr.joinAll(timeout=1)
                if self.progress_bar:
                    self.progress_bar.display()
                if self.history_file:
                    self.history_file.update_file()
            if self.progress_bar:
                self.progress_bar.display(force=True)
            if self.history_file:
                self.history_file.clean()
        except :
            self.mgr.stopAll()
            self.mgr.joinAll()
            if self.history_file:
                self.history_file.update_file(force=True)

    def __callback(self, offset, data):
        if self.progress_bar:
            self.progress_bar.update(len(data))
        if self.history_file:
            self.history_file.update(offset, len(data))

    def stop(self):
        self.mgr.stopAll()
        self.mgr.joinAll()

    class DownloadThread(threadutil.ThreadBase):
        def __init__(self, url, fp, start_at, end_at, mutex,
                     callback=None, headers=None, log=None):
            threadutil.ThreadBase.__init__(self)
            self.url = url
            self.fp = fp
            self.start_at = start_at
            self.end_at = end_at
            self.mutex = mutex
            self.callback = callback
            self.headers = headers
            self.log = log
            self.download_handle = DownloadStreamHandler(fp=self.fp, duration=0,
                                                         start_at=self.start_at, end_at=self.end_at,
                                                         mutex=self.mutex, callback=self.callback)

        def stop(self):
            self.download_handle.stop()
            threadutil.ThreadBase.stop(self)

        def run(self):
            http = HttpUtil(timeout=6)
            http.add_header('Range', 'bytes=%s-%s' % (self.start_at, self.end_at))
            while True:
                try:
                    http.fetch(self.url, self.download_handle, headers=self.headers)
                    return
                except _socket_timeout as e:
                    if self.log:
                        self.log.exception(e)
                    pass


class ProgressBar:
    def __init__(self, size=None):
        self.reset(size, 0)
        self.mutex = _Lock()

    def reset(self, total_size, cur_size):
        self.size = total_size
        self.cur_size = cur_size
        self.last_size = 0
        self.last_updat = self.start = _time()

    def update(self, data_size):
        with self.mutex:
            self.cur_size += data_size

    def display(self, force=False):
        assert self.size
        now = _time()
        duration = now - self.last_updat
        if not force and duration < 1:
            # print '*******%d-%d=%d'%(now, self.last_updat, duration)
            return
        percentage = 10.0*self.cur_size/self.size
        speed = (self.cur_size - self.last_size)/duration
        output_format = '\r[%3.1d%% %5.1dk/s][ %5.1ds/%5.1ds] [%dk/%dk]'
        if speed > 0:
            output = output_format % (percentage*10, speed/1024, now - self.start,
                (self.size-self.cur_size)*1024/speed, self.cur_size/1024, self.size/1024)
        else:
            if self.cur_size == 0:
                expect = 0
            else:
                expect = (self.size - self.cur_size)*(now - self.start)/self.cur_size
            output = output_format % (percentage*10, 0, now - self.start, expect,
                                       self.cur_size/1024, self.size/1024)
        sys.stdout.write(output)
        sys.stdout.flush()
        self.last_updat = now
        self.last_size = self.cur_size


class HistoryFile:
    def __init__(self, target):
        txt = os.path.abspath(target)
        self.txt = txt + '.txt'
        self.mutex = _Lock()
        self.buffered = 0

    def reindex(self, indexes, size):
        if os.path.exists(self.txt):
            self.indexes = []
            with open(self.txt, 'r') as fp:
                for num in fp.read().split('|'):
                    if num.strip() != '':
                        (a, b) = num.split(',')
                        a, b = int(a), int(b)
                        if a <= b:
                            size -= b - a
                            self.indexes.append((a, b))
        else:
            self.indexes = indexes
            with open(self.txt, 'w') as fp:
                for num in indexes:
                    fp.write('%d,%d|'%num)
            size = 0
        return self.indexes, size

    def update(self, offset, size):
        assert size > 0
        assert offset >=0
        with self.mutex:
            self.buffered += size
            for i in xrange(len(self.indexes)):
                a, b = self.indexes[i]
                if a <= offset <= b:
                    assert a+size <= b+1
                    if a + size <= b + 1:
                        self.indexes[i] = (a + size, b)
                    break

    def clean(self):
        with self.mutex:
            if os.path.exists(self.txt):
                os.remove(self.txt)

    def update_file(self, force=False):
        str = ''
        with self.mutex:
            if not force and self.buffered < 1000*512:
                return
            self.buffered = 0
            for (a, b) in self.indexes:
                if a < b+1:
                    str += '%d,%d|' % (a, b)
                else:
                    assert a <= b+1
        with open(self.txt, 'w') as fp:
            fp.write(str)


if __name__ == "__main__":
    import util
    url = r'http://cdn.mysql.com/Downloads/Connector-J/mysql-connector-java-gpl-5.1.31.msi'
    orig_md5 = r'140c4a7c9735dd3006a877a9acca3c31'
    filename = '140c4a7c9735dd3006a877a9acca3c31'
    progress_bar = ProgressBar()
    axel = MiniAxel(progress_bar=progress_bar)
    if not os.path.exists(filename):
        with open(filename, 'w'):
            pass
    with open(filename, 'rb+') as fp:
        if not fp.closed:
            axel.dl(url, fp=fp)
    with open(filename, 'rb') as fp:
        assert orig_md5 == util.md5_for_file(fp)
    os.remove(filename)
