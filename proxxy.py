#!/usr/bin/env python
import socket
import asyncio
import logging

import aiohttp
import aiohttp.web
import boto3


log = logging.getLogger(__name__)


class Proxxy:
    """
    S3 backed proxy for HTTP(s) clients.

    Clients request resources to this proxy via various CNAMES. The proxy maps
    the CNAMES to remote backends. It will fetch the remote content and store
    on the specified S3 bucket, and redirect clients to S3.

    e.g.

        GET /foo/bar
        Host: remotename.localhost

        Will result in this proxy fetching /foo/bar from the backend host corresponding to remotename

    There are a few class variables that can be overridden:
        suffix (str): The suffix to strip off requests to determine which
                      backend to query. This defaults to  this host's hostname,
                      but should be whatever DNS name is used to reach this
                      service.

        reduced_redundancy (bool): If True (the default), then reduced redundancy storage is used on S3
    """

    suffix = ".{}".format(socket.getfqdn())
    reduced_redundancy = True

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.backends = {}
        self.s3 = boto3.client('s3')
        self.request_session = aiohttp.ClientSession()

        # remote fetches in progress
        # mapping of object names to future objects that resolve when the
        # original request is done (not necessarily successfully!)
        self.in_progress = {}


    def add_backend(self, backend_name, backend_remote):
        self.backends[backend_name] = backend_remote


    def is_cached(self, object_name):
        url = self.s3.generate_presigned_url('get_object',
                                             {'Bucket': self.bucket_name,
                                              'Key': object_name},
                                             ExpiresIn=30,
                                             HttpMethod='HEAD',
                                             )
        log.debug('Checking %s', url)
        req = yield from self.request_session.request('head', url)
        req.close()
        log.debug('HEAD %s: %s', url, req.status)
        return req.status == 200

    def make_object_url(self, object_name, method='GET'):
        url = self.s3.generate_presigned_url('get_object',
                                             {'Bucket': self.bucket_name,
                                              'Key': object_name},
                                             ExpiresIn=30,
                                             HttpMethod=method,
                                             )
        return url

    def make_put_url(self, object_name, backend_response):
        # TODO: Copy cache control / expiry headers
        # TODO: Set lifecycle policy?
        storage_class = 'REDUCED_REDUNDANCY' if self.reduced_redundancy else 'STANDARD'
        url = self.s3.generate_presigned_url('put_object',
                                             {'Bucket': self.bucket_name,
                                              'Key': object_name,
                                              'ContentLength': int(backend_response.headers['Content-Length']),
                                              'ContentType': backend_response.headers['Content-Type'],
                                              'StorageClass': storage_class,
                                              },
                                             ExpiresIn=30,
                                             HttpMethod='PUT',
                                             )
        headers = {
            'Content-Length': backend_response.headers['Content-Length'],
            'Content-Type': backend_response.headers['Content-Type'],
            'x-amz-storage-class': storage_class,
        }
        return url, headers


    def handle_request(self, request):
        hostname = request.host
        if not hostname.endswith(self.suffix):
            log.info('400 invalid suffix %s', request.path_qs)
            return aiohttp.web.Response(status=400, text='invalid host suffix')

        backend_name = hostname[:-len(self.suffix)]
        if backend_name not in self.backends:
            log.info('400 invalid prefix %s', request.path_qs)
            return aiohttp.web.Response(status=400, text='invalid host prefix')

        backend_remote = self.backends[backend_name]

        requested_path = request.path_qs.lstrip('/')

        backend_url = "{}/{}".format(backend_remote, requested_path)
        object_name = "{}/{}".format(backend_name, requested_path)

        # If there's another request for this object, wait for it to finish
        # before proceeding

        log.info('%s %s', request.method, request.path_qs)

        if object_name in self.in_progress:
            log.debug('waiting for other request to finish')
            url = yield from asyncio.wait_for(self.in_progress[object_name], None)
            if url:
                return aiohttp.web.Response(status=302, headers={'Location': url})

        self.in_progress[object_name] = asyncio.Future()

        try:
            if (yield from self.is_cached(object_name)):
                log.info('302 CACHE_HIT %s', request.path_qs)
                return aiohttp.web.Response(status=302, headers={'Location': self.make_object_url(object_name, request.method)})
            elif request.method == 'HEAD':
                log.info('404 CACHE_MISS %s', request.path_qs)
                return aiohttp.web.Response(status=404, text='object not found')

            log.debug('fetching %s', backend_url)
            backend_response = yield from self.request_session.request('get', backend_url)

            # TODO: Check that we got a 200 or successful response!!
            # follow redirects?
            if backend_response.status != 200:
                return aiohttp.web.Response(status=backend_response.status, text='backend response: {}'.format(backend_response.status))

            put_url, put_headers = self.make_put_url(object_name, backend_response)

            log.debug('putting to %s; headers: %s', put_url, put_headers)
            s3_response = yield from self.request_session.request('put', put_url, headers=put_headers, data=backend_response.content)

            backend_response.close()
            s3_response.close()

            log.info('302 CACHE_MISS %s', request.path_qs)
            url = self.make_object_url(object_name)
            self.in_progress[object_name].set_result(url)
            return aiohttp.web.Response(status=302, headers={'Location': url})
        except:
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
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    p = Proxxy('mozilla-releng-test')
    p.add_backend('ftp', 'https://ftp.mozilla.org')

    app = make_app(p)
    loop = asyncio.get_event_loop()
    handler = app.make_handler()

    server = loop.run_until_complete(loop.create_server(handler, '0.0.0.0', 8080))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(handler.finish_connections(1.0))
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.run_until_complete(app.finish())
    loop.close()
