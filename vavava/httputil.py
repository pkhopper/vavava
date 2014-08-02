#!/usr/bin/env python
# coding=utf-8

import os
import sys
import io
import gzip
import zlib
import urllib
import urllib2
import cookielib
from time import time as _time, sleep as _sleep
from threading import Event as _Event
from threading import Lock as _Lock
from socket import timeout as _socket_timeout
import threadutil

from io import BytesIO

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


CHARSET = "utf8"
TIMEOUT = 30  # MS
DEBUG_LVL = 0
DEFAULT_HEADERS = {
    'Referer'   : "http://www.time.com/",
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
    'Connection': 'keep-alive',
    'Accept'    : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}


class HttpUtil:
    """ a simple client of http"""
    def __init__(self):
        self._headers = DEFAULT_HEADERS
        self.set_debug_level()
        self.handlers = [
            ContentEncodingProcessor,
            urllib2.ProxyHandler,
            urllib2.UnknownHandler,
            urllib2.HTTPHandler,
            urllib2.HTTPDefaultErrorHandler,
            urllib2.HTTPRedirectHandler,
            urllib2.FTPHandler,
            urllib2.FileHandler,
            urllib2.HTTPErrorProcessor
        ]
        self.build_opener()

    def get(self, url, timeout=TIMEOUT):
        self.response = self.get_response(url, timeout=timeout)
        return self.response.read()

    def post(self, url, post_dic, timeout=TIMEOUT):
        post_data = urllib.urlencode(post_dic).encode('utf8')
        self.response = self.get_response(url, post_data=post_data, timeout=timeout)
        return self.response.read()

    def head(self, url, timeout=TIMEOUT):
        import httplib
        from urlparse import urlparse
        parts = urlparse(url)
        con = httplib.HTTPConnection(parts.netloc, timeout=timeout)
        if parts.query == '':
            url = parts.path
        else:
            url = parts.path + '/?' + parts.query
        con.request('HEAD', url, headers=self._headers)
        res = con.getresponse()
        con.close()
        if res.status == 302:
            res = self.head(res.getheader('Location'))
        return res

    def get_response(self, url, post_data=None, timeout=TIMEOUT):
        req = urllib2.Request(url, data=post_data, headers=self._headers)
        return self._opener.open(req, timeout=timeout)

    def build_opener(self, *handlers):
        import types
        def isclass(obj):
            return isinstance(obj, (types.ClassType, type))
        #################################################################
        if hasattr(self, '_proxy'):
            self.handlers.append(urllib2.ProxyHandler(self._proxy))
        if hasattr(self, '_cookie'):
            self.handlers.append(urllib2.HTTPCookieProcessor(self._cookie))
        #################################################################
        opener = urllib2.OpenerDirector()
        if hasattr(urllib2.httplib, 'HTTPS'):
            self.handlers.append(urllib2.HTTPSHandler)
        skip = set()
        for klass in self.handlers:
            for check in handlers:
                if isclass(check):
                    if issubclass(check, klass):
                        skip.add(klass)
                elif isinstance(check, klass):
                    skip.add(klass)
        for klass in skip:
            self.handlers.remove(klass)

        for klass in self.handlers:
            opener.add_handler(klass())

        for h in handlers:
            if isclass(h):
                h = h()
            opener.add_handler(h)
        self._opener = opener
        return opener

    def add_handler(self, *handlers):
        self.build_opener(handlers)

    def parse_charset(self):
        if self.response:
            import re
            charset = re.search(r'charset=([\w-]+)', self.response.headers['content-type'])
            if not charset:
                raise ValueError('content-type filed not found.')
            return charset.group(1)

    def add_header(self, key, value):
        self._headers[key] = value

    def set_cookie(self, cookie=None):
        if cookie:
            self._cookie = cookie
        else:
            self._cookie = cookielib.CookieJar()
        self.build_opener()

    def set_proxy(self, proxy):
        self._proxy = proxy
        self.build_opener()

    def set_debug_level(self, level=0):
        from httplib import HTTPConnection
        HTTPConnection.debuglevel = level
        self._debug_lvl = level


class ContentEncodingProcessor(urllib2.BaseHandler):
    """A handler to add gzip capabilities to urllib2 requests """
    # add headers to requests
    handler_order = 2045

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
        if not req.has_header('Accept-Encoding'):
            return resp
        if req.has_header('Range'):
            return resp
        if resp.headers.get("content-encoding") == "gzip":
            gz = gzip.GzipFile(fileobj=StringIO(resp.read()), mode="r")
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
            # deflate
        if resp.headers.get("content-encoding") == "deflate":
            gz = StringIO(self.deflate(resp.read()))
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
        return resp


class HttpDownloadClipHandler(urllib2.BaseHandler):
    handler_order = 2046
    BUFFER_SIZE = 1024*20

    def __init__(self, fp, range=None, file_mutex=None,
                 bs=BUFFER_SIZE, callback=None):
        self.parent = None
        self.range = range
        # 'self.offset' always point to a new position, [0, --)
        if range:
            self.start_at, self.end_at = self.range
            self.offset = self.start_at
        else:
            self.start_at = 0
            self.offset = 0
            self.end_at = -1
        self.fp = fp
        self.stop_ev = _Event()
        self.file_mutex = file_mutex
        self.buffer_size = bs
        self.callback = callback

    def http_request(self, req):
        if self.range is not None:
            req.add_header('Range', 'bytes=%s-%s' % self.range)
        if req.headers.has_key('Accept-encoding'):
            # disable ContentEncodingProcessor
            del req.headers['Accept-encoding']
        return req

    def http_response(self, req, resp):
        self.stop_ev.clear()
        try:
            if 200 <= resp.code < 300:
                if self.range:
                    self.__handle_clip(req, resp)
                else:
                    self.__handle_all(req, resp)
        except:
            raise
        finally:
            # reset for retransmission
            self.range = (self.offset, self.end_at)
            self.start_at = self.offset
        return resp

    def __handle_all(self, req, resp):
        assert resp.headers['Accept-Ranges'] == 'bytes'
        if resp.headers.has_key('Content-Length'):
            size = int(resp.headers['Content-Length'])
        else:
            size = 0
        self.end_at = size-1
        while not self.stop_ev.is_set():
            data = resp.read(self.buffer_size)
            if not data:
                break
            data_len = len(data)
            if not self.fp.closed:
                self.fp.write(data)
            if self.callback and not self.fp.closed:
                self.callback(self.offset, data_len)
            self.offset += data_len
        assert self.offset == size

    def __handle_clip(self, req, resp):
        assert resp.headers['content-range'].startswith('bytes %d-%d' % self.range)
        assert resp.code in (200, 206)
        while not self.stop_ev.is_set():
            data = resp.read(self.buffer_size)
            data_len = len(data)
            if data_len == 0:
                break
            assert self.offset + data_len <= self.end_at + 1
            if self.file_mutex:
                with self.file_mutex:
                    if not self.fp.closed:
                        self.fp.seek(self.offset)
                        self.fp.write(data)
                    else:
                        assert False
            if self.callback and not self.fp.closed:
                self.callback(self.offset, data_len)
            self.offset += data_len
        assert self.offset == self.end_at + 1

    def setStop(self):
        self.stop_ev.set()


class HttpFetcher(HttpUtil):
    """ a simple client of http"""
    def __init__(self):
        HttpUtil.__init__(self)
        self.handler = None
        self.__fetching = False

    def fetch(self, url, fp, range=None, file_mutex=None,
              bs=HttpDownloadClipHandler.BUFFER_SIZE, callback=None, timeout=None):
        self.__fetching = True
        self.handler = HttpDownloadClipHandler(fp, range=range, file_mutex=file_mutex,
                                          bs=bs, callback=callback)
        self.build_opener(self.handler)
        self.get_response(url)
        self.__fetching = False

    def isFetching(self):
        return self.__fetching

    def setStop(self):
        if self.handler:
            self.handler.setStop()


class MiniAxel:
    def __init__(self, progress_bar=None, retrans=False, proxy=None, log=None):
        self.threads = []
        self.progress_bar = progress_bar
        self.retransmission = retrans
        self.history_file = None
        self.mgr = None
        self.proxy = None
        self.log = log

    def dl(self, url, out, headers=None, n=5):
        if isinstance(out, file) or isinstance(out, BytesIO):
            self.__dl(url, out, headers=headers, n=n)
        elif os.path.exists(out):
            with open(out, 'rb+') as fp:
                self.__dl(url, fp, headers=headers, n=n)
        else:
            with open(out, 'wb') as fp:
                self.__dl(url, fp, headers=headers, n=n)

    def __head(self, url):
        try:
            info = HttpUtil().head(url)
            size = int(info.getheader('Content-Length'))
            assert info.getheader('Accept-Ranges') == 'bytes'
        except Exception as e:
            if self.log:
                self.log.exception(e)
            return None
        return size

    def __dl(self, url, fp, headers=None, n=5):
        assert n > 0
        assert url
        self.mgr = threadutil.ThreadManager()
        mutex = _Lock()
        cur_size = 0
        size = self.__head(url)
        clips = self.__div_file(size, n)

        if size and self.retransmission and not isinstance(fp, BytesIO):
            self.history_file = HistoryFile(fp.name)
            clips, cur_size = self.history_file.mk_clips(clips, size)

        # can not retransmission
        if clips is None or size is None or size == 0:
            clips = [None]
            size = 0

        if self.progress_bar:
            self.progress_bar.reset(total_size=size, cur_size=cur_size)

        for clip in clips:
            thread = MiniAxel.DownloadThread(url=url, fp=fp, range=clip, mutex=mutex,
                                             msgq=self.mgr.msg_queue, proxy=self.proxy,
                                             callback=self.__callback, log=self.log)
            self.mgr.addThreads([thread])

        try:
            self.mgr.startAll()
            if self.progress_bar:
                self.progress_bar.display(force=True)

            while self.mgr.isWorking():
                self.mgr.joinAll(timeout=0.2)
                if not self.mgr.msg_queue.empty():
                    msg = self.mgr.msg_queue.get()
                    if msg:
                        if msg == 'error':
                            raise RuntimeError('thread crashed')
                if self.progress_bar:
                    self.progress_bar.display()
                if self.history_file:
                    self.history_file.update_file()

            if self.progress_bar:
                self.progress_bar.display(force=True)
            if self.history_file:
                self.history_file.clean()
        except:
            self.mgr.stopAll()
            self.mgr.joinAll()
            if self.history_file:
                self.history_file.update_file(force=True)
            raise

    def __div_file(self, size, n):
        minsize = 1024
        # if n == 1 or size <= minsize:
        if size <= minsize:
            return None
        clip_size = size/n
        clips = [(i*clip_size, i*clip_size+clip_size-1) for i in xrange(0, n-1)]
        clips.append(((n-1)*clip_size, size-1))
        return clips

    def __callback(self, offset, size):
        if self.progress_bar:
            self.progress_bar.update(size)
        if self.history_file:
            self.history_file.update(offset, size=size)

    def terminate_dl(self):
        if self.mgr:
            self.mgr.stopAll()
            self.mgr.joinAll()

    class DownloadThread(threadutil.ThreadBase):

        def __init__(self, url, fp, range=None, mutex=None,
                     msgq=None, proxy=None, callback=None, log=None):
            threadutil.ThreadBase.__init__(self, log=log)
            self.url = url
            self.fp = fp
            self.range = range
            self.mutex = mutex
            self.msgq = msgq
            self.proxy = proxy
            self.callback = callback
            self.http_fetcher = HttpFetcher()

        def setToStop(self):
            threadutil.ThreadBase.setToStop(self)
            self.http_fetcher.setStop()

        def run(self):
            while not self.isSetToStop():
                try:
                    self.http_fetcher.fetch(self.url, fp=self.fp, range=self.range,
                            file_mutex=self.mutex, callback=self.callback)
                    break
                except _socket_timeout:
                    if self.log:
                        self.log.debug('timeout  %s', self.url)
                except urllib2.URLError as e:
                    if self.log:
                        self.log.exception(e)
                except:
                    self.msgq.put("error")
                    raise

                _sleep(1)


class ProgressBar:

    def __init__(self, size=None):
        self.reset(size, 0)
        self.mutex = _Lock()

    def reset(self, total_size, cur_size):
        self.size = total_size
        if self.size == 0:
            self.size = 1
        self.cur_size = cur_size
        self.last_size = 0
        self.last_updat = self.start = _time()

    def update(self, data_size):
        with self.mutex:
            self.cur_size += data_size

    def display(self, force=False):
        assert self.size is not None
        now = _time()
        duration = now - self.last_updat
        if not force and duration < 1:
            # print '*******%d-%d=%d'%(now, self.last_updat, duration)
            return
        percentage = 10.0*self.cur_size/self.size
        if duration == 0:
            speed = 0
        else:
            speed = (self.cur_size - self.last_size)/duration
        output_format = '\r[%3.1d%% %5.1dk/s][ %5.1ds/%5.1ds] [%dk/%dk]            '
        if speed > 0:
            output = output_format % (percentage*10, speed/1024, now - self.start,
                (self.size-self.cur_size)/speed, self.cur_size/1024, self.size/1024)
        else:
            if self.cur_size == 0:
                expect = 0
            else:
                expect = (self.size-self.cur_size)*(now-self.start)/self.cur_size
            output = output_format % (percentage*10, 0, now - self.start, expect,
                                       self.cur_size/1024, self.size/1024)
        sys.stdout.write(output)
        if percentage == 100:
            sys.stdout.write('\n')
        sys.stdout.flush()
        self.last_updat = now
        self.last_size = self.cur_size
        if force and percentage == 10:
            print ''


class HistoryFile:

    def __init__(self, target_file):
        self.target_file = os.path.abspath(target_file)
        self.txt = self.target_file + '.txt'
        self.mutex = _Lock()
        self.buffered = 0

    def mk_clips(self, clips, size):
        """ return clips, current_size, is_retransmission
        """
        cur_size = size
        if os.path.exists(self.txt) and os.path.exists(self.target_file):
            self.clips = []
            with open(self.txt, 'r') as fp:
                for num in fp.read().split('|'):
                    if num.strip() != '':
                        (a, b) = num.split(',')
                        a, b = int(a), int(b)
                        if a <= b:
                            cur_size -= b - a + 1
                            self.clips.append((a, b))
            return self.clips, cur_size
        else:
            if clips is None:
                self.clips = [(0, size - 1)]
            else:
                self.clips = clips
            with open(self.txt, 'w') as fp:
                for clip in self.clips:
                    fp.write('%d,%d|' % clip)
            return clips, 0


    def update(self, offset, size):
        assert size > 0
        assert offset >=0
        with self.mutex:
            self.buffered += size
            for i in xrange(len(self.clips)):
                a, b = self.clips[i]
                if a <= offset <= b:
                    if size <= b - a + 1:
                        self.clips[i] = (a + size, b)
                    else:
                        assert size <= b - a + 1
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
            for (a, b) in self.clips:
                if a < b + 1:
                    str += '%d,%d|' % (a, b)
                else:
                    assert a <= (b + 1)
        with open(self.txt, 'w') as fp:
            fp.write(str)


def mem_file_test(axel, url, md5, n):
    import util
    fp = BytesIO()
    axel.dl(url, out=fp, n=n)
    # fp.read = fp.getvalue
    ss = util.md5_for_file(fp)
    del fp
    if md5 != ss:
        print '[memTest] md5 not match, n=%d, %s' % (n, ss)


def file_test(axel, url, name, n):
    import util
    axel.dl(url, out=name, n=n)
    with open(name, 'rb') as fp:
        ss = util.md5_for_file(fp)
    os.remove(name)
    if name != ss:
        print '[fTest] md5 not match, n=%d, %s' % (n, ss)

def random_test(axel, n):
    import util
    fp = BytesIO()
    axel.dl('http://localhost/w/dl/test.jpg', out=fp, n=n)
    with io.open('321.jpg', 'wb') as ffp:
        fp.read = fp.getvalue
        data = fp.read()
        ffp.write(data)
    fp.close()
    with open('321.jpg', 'rb') as ffp:
        print util.md5_for_file(ffp)



def main():
    import util
    urls = {
        '0d851220f47e7aed4615aebbd5cd2c7a': 'http://localhost/w/dl/test.jpg',
        # '140c4a7c9735dd3006a877a9acca3c31': 'http://cdn.mysql.com/Downloads/Connector-J/mysql-connector-java-gpl-5.1.31.msi',
        # 'asdf': 'http://vavava.baoyibj.com/chaguan/'
    }
    log = util.get_logger()
    progress_bar = ProgressBar()
    axel = MiniAxel(progress_bar=progress_bar, retrans=True, log=log)
    for n in range(1, 6):
        for md5, url in urls.items():
            try:
                # random_test(axel, n)
                # mem_file_test(axel, url, md5, n)
                file_test(axel, url, md5, n)
            except Exception as e:
                print e
                raise
            finally:
                print ''
                # if os.path.exists(name):
                #     os.remove(name)


def test():
    import util
    name = 'id_XNzQ3OTg2MjQ4'
    url = 'http://v.youku.com/v_show/id_XNzQ3OTg2MjQ4.html\?f\=22590461\&ev\=1 -f 1'
    log = util.get_logger()
    progress_bar = ProgressBar()
    axel = MiniAxel(progress_bar=progress_bar, retrans=True, log=log)
    try:
        axel.dl(url, out=name, n=2)
    except Exception as e:
        print e
        raise
    finally:
        print ''
        # if os.path.exists(name):
        #     os.remove(name)


if __name__ == "__main__":
    main()