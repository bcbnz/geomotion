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

import csv
from datetime import datetime
import ftplib
import os
import os.path
from operator import itemgetter
import pytz
import sqlite3
import urllib2

from sm.record import Record


class NoSuchSite(ValueError):
    """Exception raised by :func:`Server.get_site_info` when the requested site
    does not exist. It has one associated argument, the code of the site that
    could not be found. It inherits from the standard Python
    :class:`ValueError`.

    """
    pass


class NoSuchRecord(ValueError):
    """Exception raised by :func:`Server.get_record` when the requested record
    does not exist. It has two associated arguments, the event ID and the site
    that the record was requested for. It inherits from the standard Python
    :class:`ValueError`.

    """
    pass


class Server(object):
    """Interface with the Geonet servers and retrieve strong motion data.

    Caching
    -------

    There is a large amount of data available (as of 23 June 2011, 14,282 records
    covering 1,121 events since 6 July 2007). The records - and the information
    about what records are available - are stored on an FTP server. Although
    individual records are not large, it usually takes a few seconds to connect
    and log in to the server. Rather than having to do this repeatedly, we
    prefer to cache the data where possible. This is done in two ways.

    The lists of what sites, events, and records are available are stored in an
    SQLite database. This cache can be updated using the update_sites() and
    update_events() methods. You must run these methods the first time you
    create a new server instance. The data files themselves are retrieved when
    needed and are cached in the filesystem. When retrieving a record via the
    get_record() method you can force the cache to be ignored if you
    desire.

    The directory to store the cache information (both the SQLite database and
    the data files) can be specified when creating an instance of the class.
    Both caches are persistent across multiple instances of the class.

    Dates and times
    ---------------

    GeoNet uses UTC for all dates and time, and internally this class does the
    same. However, when creating an instance of the class you can specify the
    local timezone you want to work in (this defaults to NZ time). All dates
    returned by the class methods will be in this local timezone. When you pass
    dates into the methods, they can be in any timezone you like, and the class
    will convert them. If your dates don't contain timezone information (in
    Python terms, if they are naive dates), they are assumed to be in the local
    timezone given to the constructor.

    Units
    -----

    All returned values are in the units GeoNet uses. These are:
        * distances - kilometres
        * angles/bearings - degrees
        * latitude and longitude - decimal degrees
        * times - seconds
        * acceleration - millimetres per second per second
        * velocity - millimetres per second
        * displacement - millimetres

    Attribution
    -----------

    All data from the GeoNet site (and hence all data you can retrieve through
    this class) is made available free of charge. However, GeoNet do request
    that you acknowledge them, and their sponsors, as the source of the data in
    any output you create. See their data policy for further details:

        http://www.geonet.org.nz/resources/data-policy.html

    """

    def __init__(self, cache_dir='cache', local_timezone=pytz.timezone('NZ')):
        """

        :param cache_dir: The directory to use as a cache. This can be either an
                          absolute or relative path; if relative, the current
                          working directory is used as a base.
        :type cache_dir: string
        :param local_timezone: The timezone to return event dates in.
        :type local_timezone: pytz.timezone

        """
        # Store the timezone.
        self.local_timezone = local_timezone

        # Convert cache directory to an absolute path if necessary.
        if not os.path.isabs(cache_dir):
            cache_dir = os.path.abspath(cache_dir)

        # Clean the path up.
        self.cache_dir = os.path.normpath(cache_dir)

        # Create the cache directory if needed.
        if not os.path.isdir(self.cache_dir):
            os.mkdir(self.cache_dir)

        # Connect to the info cache.
        self.info_cache = sqlite3.connect(os.path.join(self.cache_dir,
                                                       'info_cache.sqlite'))

        # Row factory.
        self.info_cache.row_factory=sqlite3.Row

        # Enable foreign keys.
        self.info_cache.execute('pragma foreign_keys = ON;')

    def __del__(self):
        # Close the info cache, making sure we commit any pending changes.
        self.info_cache.commit()
        self.info_cache.close()

        # Ensure we close the FTP connection properly when the instance is deleted.
        self.disconnect_ftp()

    def connect_ftp(self):
        """Create or check the connection to the FTP server. If a connection
        previously existed, this will check it still works. If it has timed out,
        a replacement connection will be created.

        Note there is no need to call this manually; any functions which need to
        retrieve data from the server will call this automatically.

        """
        # We already have a connection, see if it is still alive.
        if hasattr(self, '_ftpconnection'):
            try:
                self._ftpconnection.sendcmd('NOOP')
            except ftplib.error_temp as e:
                # Connection has timed out, we'll replace it with a new one later
                # on.
                if e.args[0].startswith('421'):
                    delattr(self, '_ftpconnection')

                # Some other error we're not sure what to do with.
                else:
                    raise

        # New connection needed. Note we need to run the check again in case it
        # timed out in the previous block.
        if not hasattr(self, '_ftpconnection'):
            self._ftpconnection = ftplib.FTP('ftp.geonet.org.nz')
            self._ftpconnection.login()
            return

    def disconnect_ftp(self):
        """Close the connection to the FTP server. This is automatically called
        when Python destroys the instance, but you can call it earlier if you
        want.

        """
        # Make sure we actually have something to close.
        if hasattr(self, '_ftpconnection'):
            # The call to quit() may raise an exception if the server doesn't
            # like the QUIT command sent to it (e.g., if it has timed out
            # already). As we are disconnecting from it, we couldn't care less
            # about its crippling emotional issues, we just want to hide them
            # from public view.
            try:
                self._ftpconnection.quit()
            except:
                pass
            delattr(self, '_ftpconnection')

    def update_events(self, since=datetime(1950, 1, 1)):
        """Update the list of events. As it has to retrieve and parse directory
        listings from the GeoNet FTP server, it can take a few minutes to do a
        full update. However, you can limit it to only updating events since a
        certain time.

        :param since: Due to the way the data is organised on the server, events
                      are updated month by month. Only events in the same month
                      as this date or later months will be updated.
        :type since: date

        """

        # Get a cursor to the cache.
        cursor = self.info_cache.cursor()

        # Does the events table already exist in the cache?
        cursor.execute('''select count(*) from sqlite_master where type='table'
                       and name='events';''')
        if not bool(cursor.fetchone()[0]):
            # Create the table.
            cursor.execute('''create table events (
                id integer primary key autoincrement,
                year integer not null,
                month integer not null,
                day integer not null,
                hour integer not null,
                minute integer not null,
                second integer not null);''')

        # How about the records table?
        cursor.execute('''select count(*) from sqlite_master where type='table'
                       and name='records';''')
        if not bool(cursor.fetchone()[0]):
            # Create the table.
            cursor.execute('''create table records (
                event_id integer not null,
                site varchar not null,
                ftp_directory varchar not null,
                filename varchar not null,
                foreign key(event_id) references events(id) on delete cascade);''')

        # We'll need to be connected to the FTP server for this.
        self.connect_ftp()

        # First, lets get a list of all the years data possibly exists for.
        base_dir = '/strong/processed/Proc'
        self._ftpconnection.cwd(base_dir)
        years = sorted(map(int, self._ftpconnection.nlst()))

        # Filter out years earlier than the requested update time.
        years = [year for year in years if year >= since.year]

        # Next, lets process each year.
        for year in years:
            print 'Processing {0}'.format(year)

            # Move into the data directory for the year.
            year_dir = base_dir + '/' + str(year)
            self._ftpconnection.cwd(year_dir)

            # Get all the months data exists for..
            raw = self._ftpconnection.nlst()

            # Filter out the directories we know how to handle.
            months = sorted(map(int, (month[:2] for month in raw if month.endswith('_Prelim'))))

            # Filter out months that are earlier than the requested update time.
            if year == since.year:
                months = [month for month in months if month >= since.month]

            # Now, lets process each month.
            for month in months:
                print 'Processing {0}/{1}'.format(month, year)

                # Delete any existing events.
                cursor.execute('''delete from events where month=? and year=?;''',
                               (month, year))

                # Move into the data directory for the month.
                month_dir = year_dir + '/{0:02d}_Prelim'.format(month)
                self._ftpconnection.cwd(month_dir)

                # Get all the event directories.
                events = self._ftpconnection.nlst()

                # And finally, we need to get the sites for each event.
                for event in events:
                    # Split the folder name into its component parts to get the
                    # date of the event.
                    date, time = event.split('_')
                    y, m, d = date.split('-')
                    h, mn, s = time[0:2], time[2:4], time[4:6]

                    # Insert the event and get its ID.
                    cursor.execute('''insert into events (year, month, day,
                                   hour, minute, second) values (?, ?, ?, ?, ?,
                                   ?);''', (y, m, d, h, mn, s))
                    event_id = cursor.lastrowid

                    # Move into the data directory.
                    data_dir = month_dir + '/' + event + '/Vol1/data'
                    self._ftpconnection.cwd(data_dir)

                    # Get all filenames.
                    sites = self._ftpconnection.nlst()

                    # Get the site names.
                    sites = [(event_id, site[16:-4], data_dir, site) for site in sites]

                    # Insert it into the cache.
                    cursor.executemany('''insert into records (event_id, site,
                                       ftp_directory, filename) values(?, ?, ?,
                                       ?);''', sites)

                # Store the changes to this month.
                self.info_cache.commit()

        # Done. Shouldn't have to commit here but better safe than sorry. In the
        # scheme of this function, the extra call won't add any noticeable
        # delay.
        self.info_cache.commit()
        cursor.close()

    def update_sites(self):
        """Update the list of sites to match the list on the GeoNet website.

        """
        # Get a cursor for the cache.
        cursor = self.info_cache.cursor()

        # Does the sites table exist in the cache?
        cursor.execute('''select count(*) from sqlite_master where type='table'
                       and name='sites';''')
        if not bool(cursor.fetchone()[0]):
            # Create the table.
            cursor.execute('''create table sites (code varchar primary key not
                           null, name varchar not null, latitude float not null,
                           longitude float not null, opened timestamp not null,
                           status varchar not null, notes varchar);''')

        # It does exist; empty it so we can refresh it.
        else:
            cursor.execute('delete from sites;')

        # Get the raw CSV file.
        raw_csv = urllib2.urlopen("http://magma.geonet.org.nz/ws-delta/site?type=seismicSite&outputFormat=csv")

        # Skip the first line which is a note about what filtering was
        # performed to create the file.
        raw_csv.readline()

        # Let the CSV module parse the rest into a dictionary.
        sites = csv.DictReader(raw_csv)

        # Add the entries to the cache. Note we throw away sites with
        # duplicate codes as they refer to multiple sensors in the same site
        # (e.g., Wellington Hospital). Although they are separated spatially
        # its not by a huge amount, and we don't care *that* much that we
        # want to come up with some scheme of differentiating between them.
        # Unfortunately this means some currently operational sites are
        # marked as closed because there has been more than one site there.
        # Need to investigate if we can query the server with more than one
        # status filter, otherwise we'll need to do some filtering of our
        # own here.
        seen = set()
        for site in sites:
            # Filter duplicates.
            if site['Code'] in seen:
                continue

            # Format the date, converting it from the NZ time it is given in to
            # UTC.
            opened = datetime.strptime(site['Opened'], '%Y-%m-%d %H:%M:%S.%f')
            opened = pytz.timezone('NZ').localize(opened)
            site['Opened'] = opened.astimezone(pytz.utc).replace(tzinfo=None)

            # Insert the record.
            cursor.execute('''insert into sites (code, name, latitude,
                           longitude, opened, status, notes) values (:Code,
                           :Name, :Latitude, :Longitude, :Opened, :Status,
                           :Notes);''', site)
            seen.add(site['Code'])

        # Done.
        raw_csv.close()
        self.info_cache.commit()

    def get_years(self):
        """Get a list of years for which records exist.

        """
        cursor = self.info_cache.cursor()
        cursor.execute('select distinct year from events;')
        years = [row['year'] for row in cursor]
        cursor.close()
        return years

    def get_months(self, year):
        """Get a list of months in the given year for which records exist.

        :param year: The year in question.
        :type year: integer

        """
        cursor = self.info_cache.cursor()
        cursor.execute('select distinct month from events where year=?;', (year,))
        months = [row['month'] for row in cursor]
        cursor.close()
        return months

    def get_events(self, year, month):
        """Get a list of events in the given year and month. Each event is
        returned as a two-element tuple, the first element of which is the event
        ID and the second the date and time it occurred.

        :param year: The year in question.
        :type year: integer
        :param month: The month in question.
        :type month: integer'

        """
        cursor = self.info_cache.cursor()
        cursor.execute('''select id, day, hour, minute, second from events where
                       year=? and month=?;''', (year, month))
        events = [(row['id'], self._dbtolocal(year, month, row['day'], row['hour'],
                   row['minute'], row['second'])) for row in cursor]
        cursor.close()
        return sorted(events, key=itemgetter(1))

    def _dbtolocal(self, y, m, d, h, mn, s):
        """Helper function to format event dates into a local datetime object.

        """
        date = datetime(y, m, d, h, mn, s, tzinfo=pytz.utc)
        return date.astimezone(self.local_timezone)

    def get_sites(self, event):
        """Get a list of the sites which have records for the given event.

        :param event: The event ID.
        :type event: integer

        """
        cursor = self.info_cache.cursor()
        cursor.execute('select distinct site from records where event_id=?',
                       (event,))
        sites = [row['site'] for row in cursor]
        cursor.close()
        return sites

    def get_site_info(self, site):
        """Find further information about a particular GeoNet site. This
        information is returned as a dictionary with the following keys:

            * ``code`` - The GeoNet code for the site.
            * ``name`` - The full name of the site.
            * ``latitude`` - The latitude of the site.
            * ``longitude`` - The longitude of the site.
            * ``opened`` - When the site was opened.
            * ``status`` - The current status of the site.
            * ``notes`` - Any additional notes about the site.

        :param site: The GeoNet code for the site in question.
        :type site: string
        :raise NoSuchSite:

        """
        # Get a cursor for the cache.
        cursor = self.info_cache.cursor()

        # Try to get the site.
        cursor.execute('select * from sites where code=?;', (site,))
        info = cursor.fetchone()

        # Done with the cursor.
        cursor.close()

        # Invalid site code.
        if not info:
            raise NoSuchSite(site)

        # Convert row to a dictionary and convert timestamp to a datetime.
        d = dict(info)
        date = datetime.strptime(d['opened'], '%Y-%m-%d %H:%M:%S')
        date = pytz.utc.localize(date)
        d['opened'] = date.astimezone(self.local_timezone)
        return d

    def get_record(self, event, site, skip_cache=False):
        """Get the record of an event from a particular site. This is returned
        as a Record instance.

        :param event: The event ID to get the record for.
        :type event: integer
        :param site: The GeoNet code for the site in question.
        :type site: string
        :param skip_cache: Ignore cached data and force a download.
        :type skip_cache: Boolean

        """
        # Make sure the site name is uppercased.
        site = site.upper()

        # See if there is a corresponding record.
        cursor = self.info_cache.cursor()
        cursor.execute('''select ftp_directory, filename from records where
                       event_id=? and site=?;''', (event, site))
        row = cursor.fetchone()
        cursor.close()

        # No such record.
        if row is None:
            raise NoSuchRecord(event, site)

        # Retrieve the details.
        ftp_directory = row['ftp_directory']
        filename = row['filename']
        cache_filename = os.path.join(self.cache_dir, filename)

        # Do we need to download it?
        if skip_cache or not os.path.isfile(cache_filename):
            # Ensure we are connected.
            self.connect_ftp()

            # Move to the directory.
            self._ftpconnection.cwd(ftp_directory)

            # Retrieve the data.
            f = open(cache_filename, 'wb')
            try:
                self._ftpconnection.retrbinary('RETR {0}'.format(filename), f.write)
            except:
                # Close and remove the invalid file before propagating the
                # exception.
                f.close()
                os.remove(cache_filename)
                raise
            f.close()

        # Try to get the site info. In theory, the site must exist if we found
        # a record. But this depends on (a) the sites cache being populated, and
        # (b) the site list on the GeoNet website being processed correctly when
        # the cache is populated.
        try:
            site_info = self.get_site_info(site)
        except NoSuchSite:
            site_info = {}

        # Parse and return the data.
        return Record(site_info, cache_filename, self.local_timezone)
