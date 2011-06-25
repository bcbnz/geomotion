# This file is part of geomotion, a library and assorted utilities to work with
# strong motion data from the GeoNet project.
# Copyright (C) 2011 Blair Bonnett
#
# geomotion is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# geomotion is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# geomotion.  If not, see <http://www.gnu.org/licenses/>.

import os.path
import wsgiref.util

# The status code definitions specified in RFC2616.
# See http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
STATUS_CODE = {
    100: '100 CONTINUE',
    101: '101 SWITCHING PROTOCOLS',
    200: '200 OK',
    201: '201 CREATED',
    202: '202 ACCEPTED',
    203: '203 NON-AUTHORITATIVE INFORMATION',
    204: '204 NO CONTENT',
    205: '205 RESET CONTENT',
    206: '206 PARTIAL CONTENT',
    300: '300 MULTIPLE CHOICES',
    301: '301 MOVED PERMANENTLY',
    302: '302 FOUND',
    303: '303 SEE OTHER',
    304: '304 NOT MODIFIED',
    305: '305 USE PROXY',
    306: '306 RESERVED',
    307: '307 TEMPORARY REDIRECT',
    400: '400 BAD REQUEST',
    401: '401 UNAUTHORIZED',
    402: '402 PAYMENT REQUIRED',
    403: '403 FORBIDDEN',
    404: '404 NOT FOUND',
    405: '405 METHOD NOT ALLOWED',
    406: '406 NOT ACCEPTABLE',
    407: '407 PROXY AUTHENTICATION REQUIRED',
    408: '408 REQUEST TIMEOUT',
    409: '409 CONFLICT',
    410: '410 GONE',
    411: '411 LENGTH REQUIRED',
    412: '412 PRECONDITION FAILED',
    413: '413 REQUEST ENTITY TOO LARGE',
    414: '414 REQUEST-URI TOO LONG',
    415: '415 UNSUPPORTED MEDIA TYPE',
    416: '416 REQUESTED RANGE NOT SATISFIABLE',
    417: '417 EXPECTATION FAILED',
    500: '500 INTERNAL SERVER ERROR',
    501: '501 NOT IMPLEMENTED',
    502: '502 BAD GATEWAY',
    503: '503 SERVICE UNAVAILABLE',
    504: '504 GATEWAY TIMEOUT',
    505: '505 HTTP VERSION NOT SUPPORTED',
}

class Application(object):
    """A WSGI application to provide access to the strong motion data along with
    various ways to visualise it.

    """

    def __init__(self, media_path=None):
        """

        :param media_path: The path to serve static media from. If this is set
                           to ``None``, the media/ directory under the directory
                           containing this module will be used.
        :type media_path: string

        """
        # The directory containing this module.
        self.path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

        # Calculate or store the media path.
        if media_path is None:
            self.media_path = os.path.join(self.path, 'media')
        else:
            self.media_path = os.path.abspath(media_path)

    def __call__(self, environment, start_response):
        """Handle a WSGI request. This is the entry point for the WSGI
        application.

        Any request paths with the prefix /media/ will be treated as static
        files and served from the media directory specified when constructing
        the class. This is suitable for the simple Python server included with
        the application, but if you are setting up a production server you
        probably want to let the server handle the static files rather than
        making a WSGI request for them.

        :param environment: The WSGI environment containing the request.
        :type environment: dictionary
        :param start_response: The WSGI function to start a response.
        :type start_response: function

        """
        # Get the path and query string.
        path = environment['PATH_INFO']

        # Static media files.
        if path.startswith('/media/'):
            return self.serve_media(path[7:], start_response)

        # No idea what they were after.
        start_response(STATUS_CODE[404], [])
        return ('File not found',)

    def serve_media(self, path, start_response):
        """Serve a static media request.

        :param path: The path of the requested file relative to the applications
                     media path.
        :type path: string
        :param start_response: The WSGI function to start a response.
        :type start_response: function

        """
        # Work out the full path to the file.
        filename = os.path.abspath(os.path.join(self.media_path, path))

        # Make sure it is in the media directory (unlikely it won't be, but
        # better safe than sorry).
        common = os.path.commonprefix((self.media_path, filename))
        if not common == self.media_path:
            start_response(STATUS_CODE[403], [])
            return ('You do not have permission to access this file.',)

        # If it doesn't exist, raise a 404 error.
        if not os.path.isfile(filename):
            start_response(STATUS_CODE[404], [])
            return ('File not found',)

        # Return the contents of the file.
        start_response('200 OK', [])
        return wsgiref.util.FileWrapper(open(filename, 'r'))
