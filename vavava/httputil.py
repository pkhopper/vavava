#!/usr/bin/env python
# coding=utf-8


import urllib
import urllib2
import cookielib
from gzip import GzipFile as _GzipFile
from zlib import compress as _decompress, error as _zlib_error, MAX_WBITS as _zlib_MAX_WBITS
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



def http_get(url, retry=None):
    if not retry:
        return HttpUtil().get(url)
    while retry > 0:
        retry -= 1
        try:
            return HttpUtil().get(url)
        except KeyboardInterrupt as e:
            raise e
        except Exception: # urllib2.HTTPError, urllib2.URLError:
            pass


class HttpUtil:
    """ a simple client of http"""
    def __init__(self):
        self._headers = DEFAULT_HEADERS
        self.set_debug_level()
        self.handlers = [
            ContentEncodingProcessor,  # diff with urllib2.build_opener(), disabled by HttpDownloadClipHandler
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

    def add_headers(self, headers):
        for k, v in headers.items():
            self.add_header(k, v)

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
            self.deflate = lambda data: _decompress(data, -1*_zlib_MAX_WBITS)
        except _zlib_error:
            self.deflate = lambda data: _decompress(data)

    def http_request(self, req):
        req.add_header("Accept-encoding", "gzip,deflate")
        return req

    def http_response(self, req, resp):
        old_resp = resp
        if not req.has_header('Accept-encoding'):
            return resp
        if req.has_header('Range'):
            return resp
        if resp.headers.get("content-encoding") == "gzip":
            gz = _GzipFile(fileobj=StringIO(resp.read()), mode="r")
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
            # deflate
        if resp.headers.get("content-encoding") == "deflate":
            gz = StringIO(self.deflate(resp.read()))
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
        return resp


from util import SynFileContainer
class HttpDownloadClipHandler(urllib2.BaseHandler):
    handler_order = 2046
    BUFFER_SIZE = 1024*20

    def __init__(self, fp, data_range=None, is_set_stop=None, callback=None, log=None):
        self.parent = None
        self.data_range = data_range
        # 'self.offset' always point to a new position, [0, --)
        if data_range:
            self.start_at, self.end_at = self.data_range
            self.offset = self.start_at
        else:
            self.start_at = 0
            self.offset = 0
            self.end_at = -1
        if isinstance(fp, SynFileContainer):
            self.fp = fp
        else:
            self.fp = SynFileContainer(fp)
        if is_set_stop:
            self.isSetStop = is_set_stop
        else:
            self.isSetStop = lambda : False
        self.__buffer_size = HttpDownloadClipHandler.BUFFER_SIZE
        self.callback = callback
        self.log = log

    def http_request(self, req):
        if self.data_range is not None:
            req.add_header('Range', 'bytes=%s-%s' % self.data_range)
        if 'Accept-encoding' in req.headers:
            # disable ContentEncodingProcessor
            del req.headers['Accept-encoding']
        return req

    def http_response(self, req, resp):
        try:
            if 200 <= resp.code < 300:
                if self.data_range:
                    self.__handle_clip(req, resp)
                else:
                    self.__handle_all(req, resp)
        except:
            raise
        finally:
            # reset for retransmission
            self.data_range = (self.offset, self.end_at)
            self.start_at = self.offset
        return resp

    def __handle_all(self, req, resp):
        assert resp.code in (200, 206)
        # assert resp.headers['Accept-Ranges'] == 'bytes'
        if resp.headers.has_key('Content-Length'):
            size = int(resp.headers['Content-Length'])
        else:
            size = 0
        self.end_at = size-1
        while not self.isSetStop():
            data = resp.read(self.__buffer_size)
            data_len = len(data)
            if data_len == 0:
                break
            self.fp.seek_write(b=data)
            if self.callback:
                self.callback(self.offset, data_len)
            self.offset += data_len
        if not self.isSetStop() and self.offset != size:
            self.log.error('[HttpDownloadClipHandler.__handle_all]  %d != %d',
                           self.offset, size)

    def __handle_clip(self, req, resp):
        # assert resp.headers['content-range'].startswith('bytes %d-%d' % self.range)
        assert resp.code in (200, 206)
        while not self.isSetStop():
            data = resp.read(self.__buffer_size)
            data_len = len(data)
            if data_len == 0:
                break
            assert self.offset + data_len <= self.end_at + 1
            self.fp.seek_write(b=data, pos=self.offset)
            if self.callback:
                self.callback(self.offset, data_len)
            self.offset += data_len
        if not self.isSetStop() and self.offset != self.end_at + 1:
            self.log.error('[HttpDownloadClipHandler.__handle_clip]  %d != %d',
                           self.offset, self.end_at + 1)


class HttpFetcher(HttpUtil):
    """ a simple client of http"""
    def __init__(self, log=None):
        HttpUtil.__init__(self)
        self.log = log
        self.handler = None

    @staticmethod
    def div_file(size, n):
        minsize = 1024
        # if n == 1 or size <= minsize:
        if size <= minsize:
            return None
        range_size = size/n
        ranges = [(i*range_size, i*range_size+range_size-1) for i in range(0, n-1)]
        ranges.append(((n-1)*range_size, size-1))
        return ranges

    @staticmethod
    def get_content_len(url):
        http = HttpUtil()
        info = http.head(url)
        if 200 <= info.status < 300:
            if info.msg.dict.has_key('Content-Length'):
                return int(info.getheader('Content-Length'))
        resp = http.get_response(url)
        if 200 <= resp.code < 300:
            # assert resp.has_header('Accept-Ranges')
            length = int(resp.headers.get('Content-Length'))
            resp.close()
            return length

    def fetch(self, url, fp, data_range=None, isSetStop=None, callback=None):
        self.handler = HttpDownloadClipHandler(
            fp, data_range=data_range,is_set_stop=isSetStop, callback=callback, log=self.log)
        self.build_opener(self.handler)
        self.get_response(url)




def ttttt(n, test_urls, log):
    import util, os
    fetcher = HttpFetcher(log)
    mm = ''
    for md5, url in test_urls.items():
        with open(md5, 'w') as fp:
            size = HttpFetcher.get_content_len(url)
            clips = HttpFetcher.div_file(size, 3)
            assert clips
            if n == 1:
                fetcher.fetch(url, fp)
            else:
                for r in clips:
                    fetcher.fetch(url, fp, data_range=r)
            log.info('========= checking n=%d ===================', n)
        with open(md5, 'r') as fp:
            mm = util.md5_for_file(fp)
        os.remove(md5)
        assert md5 == mm

def main_test():
    import util
    test_urls = {
            'dd3322be6b143c6a842acdb4bb5e9f60': 'http://localhost/w/dl/20140728233100.ts',
            # '0d851220f47e7aed4615aebbd5cd2c7a': 'http://localhost/w/dl/test.jpg'
    }
    log = util.get_logger()
    ttttt(1, test_urls, log)
    ttttt(3, test_urls, log)
    ttttt(4, test_urls, log)

if __name__ == "__main__":
    main_test()
