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
import math
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
    header['event']['distance'] = dist * 1000

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
    header['site']['local_gravity'] = g/1000

    # And finally we get to the data.
    all_data = []

    # The data is given in lines of 10 floating-point numbers, each represented
    # as 8 ASCII characters. In general, there is a space between them so we
    # could just use the .split() method to get the individual values. However,
    # 999999.9 seems to be the GeoNet way of representing NaN, leading to no
    # space. Some basic profiling indicated preparing some ranges ahead of time
    # and using them as indices to get the data was the most efficient way of
    # proceeding.
    blocks = [slice(i*8, i*8+8) for i in range(0, 10)]

    # Fun fact of the day: the number of digitised samples (the 't' variable
    # which is the first value of the fourth line of integer headers) is NOT
    # always the number of data points in the file. If it is larger than the
    # real number of points, we will start to chew up the next component in the
    # record (or hit EOF problems). If it is smaller, we will miss data and have
    # issues finding the start of the next component. Hence we use the number of
    # acceleration points in the file as our indicator.
    while len(all_data) < a:
        line = source.readline()
        values = [line[block] for block in blocks]
        all_data.extend([value for value in values if not value.isspace()])

    # Convert it to a numpy array.
    all_data = numpy.array(all_data, dtype=float)

    # Split it out into the different sorts of data.
    start = pre
    if a:
        end = start + a
        data['acceleration'] = all_data[start:end] / 1000
        start = end
    else:
        data['acceleration'] = None
    if v:
        end = start + v
        data['velocity'] = all_data[start:end] / 1000
        start = end
    else:
        data['velocity'] = None
    if d:
        end = start + d
        data['displacement'] = all_data[start:end] / 1000
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

class TooFewComponents(ValueError):
    """Exception raised by the :class:`Record` constructor when the data file
    given to it does not have enough components to describe a three-dimensional
    vector. It inherits from the standard Python :class:`ValueError`.

    """
    pass

class Record(object):
    """The record of a strong motion event at a particular site. This reads and
    processes the data from a GeoNet file.

    In general, a site will make measurements in three directions, two
    horizontal and one vertical. The two horizontal measurements are commonly
    made in orthogonal directions. As the headings of these measurements will
    differ from site to site, this class realigns them so that one is in a
    northerly direction (a heading of zero degrees) and the other is in an
    easterly direction (a heading of ninety degrees).

    The instance will have the following attributes:

        * ``acceleration`` - the acceleration data, given in m/s/s as a
                             numpy array with three rows. The first row is the
                             horizontal acceleration in a northerly direction
                             heading of zero degrees), the second row the
                             horizontal acceleration in an easterly direction
                             and the final row the vertical acceleration.
        * ``data_length`` - the length of each row of data.
        * ``duration`` - the duration of the record in seconds.
        * ``event`` - a dictionary containing some details of the event itself,
                      such as the bearing and distance from the site, the depth,
                      the location and when the event started.
        * ``magnitudes`` - a dictionary containing magnitude information about
                           the event. Note that one or more values may be
                           missing or set to zero for any given record.
        * ``site`` - a dictionary containing some information about the site,
                     such as its location, when it opened, and the local
                     gravity.
        * ``start`` - when the recording started. This is commonly a few seconds
                      prior to the start of the event.
        * ``time`` - a numpy array containing the times at which the data points
                     were recorded.
        * ``timestep`` - the time interval between one data point and the next.

    In general, you do not want to create an instance of this class yourself.
    Instead, you should use the :func:`get_record` method of the
    :class:sm.Server: class. This will download the data files from the GeoNet
    server when necessary, and will maintain a local cache of these files.

    """

    def __init__(self, site_info, source, timezone):
        """

        :param site_info: The site information dictionary as returned by
                          sm.Server.get_site_info().
        :type site_info: dictionary
        :param source: The source file to read the data from. This can be either
                       a file object, or a filename.
        :param timezone: The timezone to convert all dates and times to.
        :type timezone: pytz.timezone
        :raise TooFewComponents: If there are not enough components in the
                                 source to realign the measurements.

        """
        # Given a filename, open it.
        close = False
        if isinstance(source, basestring):
            source = open(source, 'r')
            close = True

        # Use the given site info as a base.
        self.site = site_info

        # Pull out the components.
        first_run = True
        seen_axes = set()
        horizontal_axes = 0
        vertical_axis = False
        for header, data in component_iterator(source, timezone):
            # Use the first header to populate record information.
            if first_run:
                self.site.update(header['site'])
                self.event = header['event']
                self.magnitudes = header['magnitudes']
                self.start = header['buffer_start']
                self.timestep = header['timestep']
                self.duration = header['duration']
                self.data_length = len(data['acceleration'])
                self.acceleration = numpy.zeros(shape=(3, self.data_length), dtype=float)
                self.time = numpy.array(range(0, self.data_length)) * self.timestep
                first_run = False

            # Sanity check: throw away components with repeated axes.
            if header['axis'] in seen_axes:
                continue
            seen_axes.add(header['axis'])

            # The vertical axis is represented by an angle of 999 degrees.
            if header['axis'] == 999:
                self.acceleration[2] = data['acceleration']
                vertical_axis = True

            # Only need two different horizontal axes to be able to realign to N
            # and E components.
            elif horizontal_axes < 2:
                angle = math.radians(header['axis'])
                self.acceleration[0] += data['acceleration'] * math.cos(angle)
                self.acceleration[1] += data['acceleration'] * math.sin(angle)
                horizontal_axes += 1

            # Shortcut: once we have realigned the horizontal components and
            # have a vertical axis, stop processing the file.
            if vertical_axis and horizontal_axes == 2:
                break

        # If we opened a file we ought to close it.
        if close:
            source.close()

        # Did we get enough components?
        if not vertical_axis or horizontal_axes < 2:
            raise TooFewComponents()
