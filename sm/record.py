# This file is part of geomotion, a library to work with strong motion data from
# the GeoNet project.  Copyright (C) 2011 Blair Bonnett
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

from datetime import datetime
import numpy
import pytz


def parse_component(source, timezone):
    """Parse component information from a file object. This assumes the file
    pointer is at the start of the component - if not, unspecified bad things
    will happen. The one exception is if the file pointer is at the end of the
    file, in which case it will raise an EOFError.

    A two element tuple will be returned. The first element contains the header
    information, and the second element contains the data itself. No data
    processing is performed by this function i.e., the returned data is exactly
    as it appears in the file.

    In general, you don't want to call this directly. Use either the
    component_iterator() method, which iterates over all components in a file,
    or even better construct a SiteRecord instance from the file.

    :param source: The file to read the component from.
    :type source: file object
    :param timezone: The timezone to return all dates and times in.
    :type timezone: pytz.timezone

    """
    # Create the output dictionaries.
    header = {}
    data = {}

    # To start with, there is 16 lines of alphanumeric text forming a
    # heading. Note we also use this to check if there is any data left in
    # the source.
    heading = []
    for i in range(16):
        heading.append(source.readline())
    if heading[0] == '':
        raise EOFError()
    header['heading'] = ''.join(heading)

    # Initialise information containers.
    header['event'] = {}
    header['site'] = {}
    header['magnitudes'] = {}

    # Next up is 4 lines of integer information, ten integers per line.
    # Note that the buffer start time is split over the lines.
    # First, the UTC date and time of the start of the event.
    y, m, d, h, mn, s, x, x, by, bm = map(int, source.readline().split())
    start = datetime(y, m, d, h, mn, s/10, 0, pytz.utc)
    header['event']['time'] = start.astimezone(timezone)

    # Then the epicentre location and depths.
    d, m, s, dd, mm, ss, h, c, bd, bh = map(int, source.readline().split())
    lat = -1 * (d + (m * 60.0 + s)/3600)
    lng = dd + (mm * 60.0 + ss)/3600
    header['event']['latitude'] = lat
    header['event']['longitude'] = lng
    header['event']['hypocentral_depth'] = h
    header['event']['centroid_depth'] = c

    # Then, the site location and distance to epicentre.
    d, m, s, dd, mm, ss, la, cd, b, dist = map(int, source.readline().split())
    lat = -1 * (d + (m * 60.0 + s)/3600)
    lng = dd + (mm * 60.0 + ss)/3600
    header['site']['latitude'] = lat
    header['site']['longitude'] = lng
    header['site']['longitudinal_axis'] = la
    header['axis'] = cd
    header['event']['bearing'] = b
    header['event']['distance'] = dist

    # And finally the number of samples.
    t, pre, app, a, v, d, x, x, bmin, bs = map(int, source.readline().split())

    # Collate the buffer start time.
    start = datetime(by, bm, bd, bh, bmin, bs/1000, 0, pytz.utc)
    header['buffer_start'] = start.astimezone(timezone)

    # And then six lines of ten floating-point numbers, most of which we
    # don't care about.
    source.readline()
    x, x, x, x, ml, ms, mw, mb, x, x, = map(float, source.readline().split())
    d, x, x, x, x, dt, x, x, x, g = map(float, source.readline().split())
    source.readline()
    source.readline()
    source.readline()

    # Store data from the floating point numbers.
    header['magnitudes']['Ml'] = ml
    header['magnitudes']['Ms'] = ms
    header['magnitudes']['Mw'] = mw
    header['magnitudes']['Mb'] = mb
    header['duration'] = d
    header['timestep'] = dt
    header['site']['local_gravity'] = g

    # And finally we get to the data.
    all_data = []
    while len(all_data) < t:
        all_data.extend(source.readline().split())

    # Convert it to a numpy array.
    all_data = numpy.array(all_data, dtype=float)

    # Split it out into the different sorts of data.
    start = pre
    if a:
        end = start + a
        data['acceleration'] = all_data[start:end]
        start = end
    else:
        data['acceleration'] = None
    if v:
        end = start + v
        data['velocity'] = all_data[start:end]
        start = end
    else:
        data['velocity'] = None
    if d:
        end = start + d
        data['displacement'] = all_data[start:end]
    else:
        data['displacement'] = None

    # All done.
    return header, data


def component_iterator(source, timezone):
    """Iterates over all the components in the given file. See the documentation
    for the parse_component function for information on how the components are
    extracted.

    :param source: The file to read the components from.
    :type source: file object
    :param timezone: The timezone to return all dates and times in.
    :type timezone: pytz.timezone

    """
    while True:
        try:
            yield parse_component(source, timezone)
        except EOFError as e:
            break


class Record(object):
    """Stores the record of a strong motion event at a particular site. In
    general, you won't want to create an instance directly. Instead, use the
    get_record() method of the sm.Server class. This will take care of
    downloading the appropriate data file from the GeoNet server, and will
    maintain a local cache of these files.

    """

    def __init__(self, site_info, source, timezone):
        """

        :param site_info: The site information dictionary as returned by
                          sm.Server.get_site_info().
        :param source: The source file to read the data from. This can be either
                       a file object, or a filename.
        :param timezone: The timezone to convert all dates and times to.
        :type timezone: pytz.timezone

        """
        # Given a filename, open it.
        close = False
        if isinstance(source, basestring):
            source = open(source, 'r')
            close = True

        # Use the given site info as a base.
        self.site = site_info

        # Start the lists of data.
        self.components = []
        self.acceleration = []

        # Pull out the components.
        first_run = True
        for header, data in component_iterator(source, timezone):
            # Use the first header to populate record information.
            if first_run:
                self.site.update(header['site'])
                self.event = header['event']
                self.magnitudes = header['magnitudes']
                self.start = header['buffer_start']
                self.timestep = header['timestep']
                self.duration = header['duration']
                first_run = False

            # Store which component this is.
            if header['axis'] == 999:
                self.components.append('vertical')
            else:
                self.components.append(header['axis'])

            # Store the data.
            self.acceleration.append(data['acceleration'])

        # If we opened a file we ought to close it.
        if close:
            source.close()
