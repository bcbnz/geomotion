Prerequisites:
==============

* Python.
* The pytz timezone library.
* SQLite 3.6.19 or later.

Usage:
======

First run
---------

1. Create an instance of the server:

    >>> import sm
    >>> server = sm.Server()

2. Populate the cache (the second call will take a few minutes as it has
   to scrape the GeoNet FTP server):

    >>> server.update_sites()
    >>> server.update_events()

Subsequent runs
---------------

1. Create an instance of the server:

    >>> import sm
    >>> server = sm.Server()

2. Find out what years data is available for:

    >>> years = server.get_years()

3. Pick a year and see what months we have data for:

    >>> months = server.get_months(2011)

4. Pick a month and see what events happened then:

    >>> events = server.get_events(2011, 6)
    >>> events[9]
    (1194, datetime.datetime(2011, 6, 13, 14, 20, 49, tzinfo=<DstTzInfo 'NZ' NZST+12:00:00 STD>))

    The first number is an event ID assigned by the cache, and may change
    between runs.

5. See what sites have a record of the event:

    >>> server.get_sites(1194)
    ['ADCS', 'AMBC', 'APPS', 'ARPS', 'ASHS', 'BFZ', 'BMTS', 'CACS', 'CBGS',
     'CECS', 'CHHC', 'CMHS', 'CSHS', 'CSTC', 'CTZ', 'D13C', 'D14C', 'D15C',
     'DCDS', 'DCZ', 'DFHS', 'DGNS', 'DKHS', 'DORC', 'DSLC', 'DSZ', 'DUNS',
     'DUWZ', 'EAZ', 'EYRS', 'FDCS', 'FGPS', 'FJDS', 'FOZ', 'GDLC', 'GLWS',
     'GMTS', 'GODS', 'GORS', 'GRZ', 'HAFS', 'HAZ', 'HDWS', 'HMCS', 'HORC',
     'HPSC', 'HSES', 'HVSC', 'IFPS', 'INGS', 'INZ', 'KARS', 'KHZ', 'KIKS',
     'KOKS', 'KOWC', 'KPOC', 'KUZ', 'LBZ', 'LINC', 'LPCC', 'LPLS', 'LSRC',
     'LTZ', 'MAYC', 'MCAS', 'MCNS', 'MECS', 'MISS', 'MQZ', 'MRZ', 'MSZ',
     'MWZ', 'MXZ', 'NBLC', 'NELS', 'NNBS', 'NNZ', 'OAMS', 'ODZ', 'OXZ',
     'PARS', 'PEEC', 'PGMS', 'PPHS', 'PRPC', 'PXZ', 'PYZ', 'QRZ', 'QTPS',
     'RDCS', 'REHS', 'RHSC', 'RKAC', 'ROLC', 'RTZ', 'SBRC', 'SCAC', 'SHFC',
     'SHLC', 'SJFS', 'SKFS', 'SLRC', 'SMTC', 'SPFS', 'SWNC', 'SYZ', 'TAFS',
     'TCW', 'TEPS', 'TFSS', 'TMBS', 'TOZ', 'TPLC', 'TRCS', 'TUZ', 'TWAS',
     'WAKC', 'WBCS', 'WEL', 'WEMS', 'WIGC', 'WKZ', 'WNHS', 'WNKS', 'WNPS',
     'WTMC', 'WVAS']

6. Decide on what site we are interested in:

    >>> server.get_site_info('CECS')
    {'code': u'CECS',
     'latitude': -42.815170000000002,
     'longitude': 173.27473000000001,
     'name': u'Cheviot Emergency Centre',
     'notes': u'Recorder is in store room in police station  west side of fire station.',
     'opened': datetime.datetime(2002, 2, 23, 0, 0, tzinfo=<DstTzInfo 'NZ' NZDT+13:00:00 DST>),
     'status': u'Operational'}

7. Get the record from that site. If this data file has not been cached, it
   may take a few seconds to retrieve it from the GeoNet FTP server:

    >>> record = server.get_record(1194, 'CECS')
    >>> record.event
    {'bearing': 207,
     'centroid_depth': 0,
     'distance': 94000,
     'hypocentral_depth': 6,
     'latitude': -43.56388888888889,
     'longitude': 172.74305555555554,
     'time': datetime.datetime(2011, 6, 13, 14, 20, 49, tzinfo=<DstTzInfo 'NZ' NZST+12:00:00 STD>)}
    >>> record.acceleration.max()
    0.21229035745505778
    >>> record.acceleration.min()
    -0.16467282107692785

Updating the cache
------------------

1. Create an instance of the server:

    >>> import sm
    >>> server = sm.Server()

2. Tell the server when you want to start the update from:

    >>> import datetime
    >>> server.update_events(since=datetime.date(2011, 6, 1))

Bugs
====

Not all data files seem to be parsed correctly due to inconsistencies in their
formatting. If you come across any such records, please file a bug at
https://github.com/blairbonnett/geomotion/issues stating the date and time of
the event and the site(s) in question. Other bugs and feature requests can also
be posted to the same address.
