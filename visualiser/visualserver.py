#!/usr/bin/env python

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

from wsgiref.simple_server import make_server
from wsgi import Application

server = make_server('localhost', 8080, Application())
print 'Starting server, use <Ctrl-C> to stop it.'
print 'Open http://localhost:8080/ in your browser to access it.'
try:
    server.serve_forever()
except KeyboardInterrupt:
    print
    print 'Stopping server.'
