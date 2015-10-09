#!/usr/bin/env python
import socket
import asyncio
import logging

import aiohttp
import aiohttp.web
import boto3

s3 = boto3.client('s3')


log = logging.getLogger(__name__)


def make_object_url(bucket_name, object_name, request_method, expires_in,
                    api_kwargs={}):
    """
    Returns a signed URL to the given object

    Arguments:
        bucket_name (str): which bucket to access
        object_name (str): which object to access
        request_method (str): HTTP method to grant permission to. Defaults to
                              'GET'; other values could be 'HEAD'
        expires_in (int): seconds the URL should be valid for
        api_kwargs (dict): additional parameters to pass as arguments to the S3
                           API, e.g. ContentLength
    """
    if request_method in ('GET', 'HEAD'):
        api_method = 'get_object'
    elif request_method in ('PUT', 'POST'):
        api_method = 'put_object'
    else:
        raise ValueError("Unsupported request_method: %s" % request_method)

    api_args = {
        'Bucket': bucket_name,
        'Key': object_name,
    }
    api_args.update(api_kwargs)

    url = s3.generate_presigned_url(
        api_method,
        api_args,
        ExpiresIn=expires_in,
        HttpMethod=request_method,
    )
    return url


class Proxxy:
    """
    S3 backed proxy for HTTP(s) clients.

    Clients request resources to this proxy via various CNAMES. The proxy maps
    the CNAMES to remote backends. It will fetch the remote content and store
    on the specified S3 bucket, and redirect clients to S3.

    e.g.

        GET /foo/bar
        Host: remotename.localhost

        Will result in this proxy fetching /foo/bar from the backend host
        corresponding to remotename

    There are a few class variables that can be overridden:
        suffix (str): The suffix to strip off requests to determine which
                      backend to query. This defaults to  this host's hostname,
                      but should be whatever DNS name is used to reach this
                      service.

        reduced_redundancy (bool): If True (the default), then reduced
                                   redundancy storage is used on S3
    """

    suffix = ".{}".format(socket.getfqdn())
    reduced_redundancy = True
    expiry_time = 30

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.backends = {}
        self.request_session = aiohttp.ClientSession()

        # These functions are called in the specified order to handle the
        # request. If the function returns a truish value, that is returned to
        # the client.
        self.request_processors = [
            self.validate_request,
            self.handle_in_progress,
            self.handle_cached_object,
            self.download_object,
        ]

        # remote fetches in progress
        # mapping of object names to future objects that resolve when the
        # original request is done (not necessarily successfully!)
        self.in_progress = {}

    def close(self):
        """
        Close any resources in use by this instance
        """
        self.request_session.close()
        for f in self.in_progress:
            f.set_result(False)
        self.in_progress = {}

    def add_backend(self, backend_name, backend_remote):
        """
        Add a new backend to this proxy instance.

        Arguments:
            backend_name (str): Which CNAME we will map to this backend
            backend_remote (str): The canonical backend URL to fetch resources
                                  from
        """
        self.backends[backend_name] = backend_remote

    def make_object_url(self, object_name, request_method='GET'):
        """
        Wrapper around top-level make_object_url that uses self.bucket_name,
        and self.expiry_time.
        """
        return make_object_url(self.bucket_name, object_name, request_method,
                               expires_in=self.expiry_time)

    def make_put_url(self, object_name, backend_response):
        """
        Returns (url, headers) for PUTting an object to the bucket

        Arguments:
            object_name (str): The complete name of the object
            backend_response (response object): ...
        """
        # TODO: Copy cache control / expiry headers
        # TODO: Set lifecycle policy?
        storage_class = ('REDUCED_REDUNDANCY' if self.reduced_redundancy
                         else 'STANDARD')
        api_kwargs = {
            'ContentLength': int(backend_response.headers['Content-Length']),
            'ContentType': backend_response.headers['Content-Type'],
            'StorageClass': storage_class,
        }
        url = make_object_url(self.bucket_name, object_name, 'PUT',
                              expires_in=self.expiry_time,
                              api_kwargs=api_kwargs)
        headers = {
            'Content-Length': backend_response.headers['Content-Length'],
            'Content-Type': backend_response.headers['Content-Type'],
            'x-amz-storage-class': storage_class,

        }
        return url, headers

    def get_backend(self, request):
        hostname = request.host
        backend_name = hostname[:-len(self.suffix)]
        return self.backends[backend_name]

    def get_backend_url(self, request):
        requested_path = request.path_qs.lstrip('/')
        backend_remote = self.get_backend(request)
        backend_url = "{}/{}".format(backend_remote, requested_path)
        return backend_url

    def get_object_name(self, request):
        hostname = request.host
        backend_name = hostname[:-len(self.suffix)]
        requested_path = request.path_qs.lstrip('/')
        object_name = "{}/{}".format(backend_name, requested_path)
        return object_name

    @asyncio.coroutine
    def is_cached(self, object_name):
        """
        Returns True if the given object name is already present in our bucket
        Returns False otherwise

        NB It's accepted that there may be a race condition between this check,
        the client fetching the resource, and lifecycle policies deleting the
        object. Clients are expected to retry in this case.

        Arguments:
            object_name (str): The complete name of the object
        """
        url = self.make_object_url(object_name, 'HEAD')
        log.debug('Checking %s', url)
        req = yield from self.request_session.request('head', url)
        req.close()
        log.debug('HEAD %s: %s', url, req.status)
        return req.status == 200

    #
    # Request processing functions
    #
    @asyncio.coroutine
    def handle_request(self, request):
        """
        Request handling entry point. This will call each function in
        self.request_processors. If a function returns a truish value, then
        that will be return to the client and processing will stop.

        Arguments:
            request (aiohttp.web.Request object): the client request

        Returns:
            an aiohttp.web.Response object
        """
        log.info('%s %s', request.method, request.path_qs)

        for p in self.request_processors:
            response = yield from p(request)
            if response:
                return response

    @asyncio.coroutine
    def validate_request(self, request):
        """
        Validates the request; checks that the hostname matches our supported
        host suffix and backends.

        Raises an aiohttp.web.Response object if the request is invalid
        """
        hostname = request.host
        if not hostname.endswith(self.suffix):
            log.info('400 invalid suffix %s', request.path_qs)
            raise aiohttp.web.Response(status=400, text='invalid host suffix')

        backend_name = hostname[:-len(self.suffix)]
        if backend_name not in self.backends:
            log.info('400 invalid prefix %s', request.path_qs)
            raise aiohttp.web.Response(status=400, text='invalid host prefix')

    @asyncio.coroutine
    def handle_in_progress(self, request):
        # If there's another request for this object, wait for it to finish
        # before proceeding
        object_name = self.get_object_name(request)
        if object_name in self.in_progress:
            log.debug('waiting for other request to finish')
            url = (yield from
                   asyncio.wait_for(self.in_progress[object_name], None))
            if url:
                return aiohttp.web.Response(status=302,
                                            headers={'Location': url})

    @asyncio.coroutine
    def handle_cached_object(self, request):
        object_name = self.get_object_name(request)
        if (yield from self.is_cached(object_name)):
            log.info('302 CACHE_HIT %s', request.path_qs)
            return aiohttp.web.Response(
                status=302,
                headers={'Location': self.make_object_url(object_name,
                                                          request.method)})
        elif request.method == 'HEAD':
            log.info('404 CACHE_MISS %s', request.path_qs)
            return aiohttp.web.Response(status=404, text='object not found')

    @asyncio.coroutine
    def download_object(self, request):
        try:
            object_name = self.get_object_name(request)
            backend_url = self.get_backend_url(request)
            self.in_progress[object_name] = asyncio.Future()

            log.debug('fetching %s', backend_url)
            backend_response = (yield from self.request_session.request(
                'get', backend_url))

            # TODO: Check that we got a 200 or successful response!!
            # follow redirects?
            if backend_response.status != 200:
                backend_response.close()
                return aiohttp.web.Response(
                    status=backend_response.status,
                    text='backend response: {}'.format(
                        backend_response.status))

            put_url, put_headers = self.make_put_url(object_name,
                                                     backend_response)

            log.debug('putting to %s; headers: %s', put_url, put_headers)
            s3_response = (yield from self.request_session.request(
                'put', put_url, headers=put_headers,
                data=backend_response.content))

            backend_response.close()
            s3_response.close()

            log.info('302 CACHE_MISS %s', request.path_qs)
            url = self.make_object_url(object_name)
            self.in_progress[object_name].set_result(url)
            return aiohttp.web.Response(status=302, headers={'Location': url})
        except:
            # Unhandled exceptions should clear out the in-progress future
            log.exception('Unhandled exception')
            self.in_progress[object_name].set_result(False)
            raise
        finally:
            del self.in_progress[object_name]


def make_app(proxxy):
    app = aiohttp.web.Application()
    app.router.add_route('get', '/{path:.*}', proxxy.handle_request)
    app.router.add_route('head', '/{path:.*}', proxxy.handle_request)
    # TODO: Support purging the cache
    return app


if __name__ == '__main__':
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s')

    p = Proxxy('mozilla-releng-test')
    p.add_backend('ftp', 'https://ftp.mozilla.org')
    log.debug("responding to %s", p.suffix)

    app = make_app(p)
    loop = asyncio.get_event_loop()
    handler = app.make_handler()

    server = loop.run_until_complete(
        loop.create_server(handler, '0.0.0.0', 8080))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(handler.finish_connections(1.0))
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.run_until_complete(app.finish())
        p.close()
    loop.close()
