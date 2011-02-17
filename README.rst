==============
Schema Compare
==============

A simple tool to compare two database schemas. Note: this has only been tested
with MySQL.

Create a settings file called ``conf.py`` (you can use a different name, but
you must specify the name using the ``-c`` command line option). The file
should contain two items: ``DATABASE_A`` and ``DATABASE_B``::

    DATABASE_A = {
        'host': 'localhost',
        'db': 'myapp',
        'user': 'appuser',
        'passwd': '1ns3cure3p355w0rd',
    }

    DATABASE_B = {
        'host': 'myapp.com',
        'db': 'prod-myapp',
        'user': 'appuser',
        'passwd': 's3cure3p355w0rd',
    }

Invoking the tool without any arguments will fetch the table listings of both
databases, and display all tables that are present in database B, but missing
from database A.

To get more detailed information about a table, provide the table name as a
command line argument. Information about missing / different fields and indexes
will be shown.

