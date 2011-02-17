#!/usr/bin/env python

import MySQLdb as mysqldb

import sys
import collections
import functools
import itertools
import optparse

IndexPart = collections.namedtuple('IndexPart', ' '.join([
    'table', 'non_unique', 'key_name', 'seq_in_index', 'column_name',
    'collation', 'cardinality', 'sub_part', 'packed', 'null', 'index_type',
    'comment'
]))

Field = collections.namedtuple('Field', 'name type null key default extra')


class Index(object):

    def __init__(self, parts):
        parts = sorted(parts, key=lambda p: p.seq_in_index)
        self.columns = [part.column_name for part in parts]
        self.unique = not parts[0].non_unique
        self.null = parts[0].null
        self.type = parts[0].index_type


class Database(object):

    def __init__(self, **kwargs):
        self.conn = mysqldb.connect(**kwargs)
        self.cursor = self.conn.cursor()
        self._tables = None
        self._indexes = None

    @property
    def _table_defs(self):
        if not self._tables:
            self._tables = {}
            self.cursor.execute("SHOW TABLES")
            for table in [row[0] for row in self.cursor.fetchall()]:
                self._tables[table] = self._load_info(table)
        return self._tables

    def _load_indexes(self, table):
        self.cursor.execute("SHOW INDEXES FROM %s" % table)
        parts = [IndexPart(*row) for row in self.cursor.fetchall()]
        key_fn = lambda i: i.key_name
        return [Index(group[1]) for group in itertools.groupby(parts, key_fn)]

    def _load_fields(self, table):
        self.cursor.execute("DESCRIBE %s" % table)
        return [Field(*row) for row in self.cursor.fetchall()]

    def _load_info(self, table):
        return {
            'fields': self._load_fields(table),
            'indexes': self._load_indexes(table),
        }

    def tables(self):
        """Return the names of the tables in the database.
        """
        return self._table_defs.keys()

    def fields(self, table):
        """Return a list of Field objects for the fields in the given table.
        """
        return self._table_defs[table]['fields']

    def indexes(self, table):
        """Return a list of Index objects for the indexes on the given table.
        """
        return self._table_defs[table]['indexes']


class SchemaComparer(object):

    def __init__(self, db1, db2):
        self.db1, self.db2 = db1, db2
        self.indent = 0

    def output(self, fmt, *args):
        print '  ' * self.indent + fmt.format(*args)

    def blank_line(self):
        self.output('')

    def check_equal(self, name, value1, value2, field):
        if value1 != value2:
            self.output('[{0}] "{1}" is {2}, should be {3}', name, field,
                        value1, value2)
        return value1 == value2

    def compare_tables(self):
        tables1 = self.db1.tables()
        tables2 = self.db2.tables()

        self.output('Checking table presence')
        self.indent += 1

        all_good = True
        for table in set(tables2) - set(tables1):
            self.output('[{0}] is missing', table)
            all_good = False

        if all_good:
            self.output('All tables are in present')

        self.indent -= 1

    def compare_indexes(self, table):
        try:
            db1_indexes = dict((','.join(i.columns), i)
                               for i in self.db1.indexes(table))
            db2_indexes = dict((','.join(i.columns), i)
                               for i in self.db2.indexes(table))
        except KeyError:
            raise ValueError('table "{0}" not found'.format(table))

        self.output('Comparing indexes for table "{0}"'.format(table))
        self.indent += 1

        all_good = True
        for name, index2 in db2_indexes.items():
            index1 = db1_indexes.get(name, None)
            display_name = name.replace(',', ', ')

            if not index1:
                self.output('[{0}] is missing', display_name)
                all_good = False
                continue

            equal = functools.partial(self.check_equal, display_name)

            is_null = lambda value: value == 'YES'
            if not equal(is_null(index1.null), is_null(index1.null), 'null'):
                all_good = False

            if not equal(index1.unique, index2.unique, 'unique'):
                all_good = False

        if all_good:
            self.output('All indexes in sync')

        self.indent -= 1

    def compare_fields(self, table):
        try:
            db1_fields = dict((f.name, f) for f in self.db1.fields(table))
            db2_fields = dict((f.name, f) for f in self.db2.fields(table))
        except KeyError:
            raise ValueError('table "{0}" not found'.format(table))

        self.output('Comparing fields for table "{0}"'.format(table))
        self.indent += 1

        all_good = True
        for name, field2 in db2_fields.items():
            field1 = db1_fields.get(name, None)

            if not field1:
                self.output('[{0}] is missing', name)
                all_good = False
                continue

            equal = functools.partial(self.check_equal, name)

            if not equal(field1.type, field2.type, 'type'):
                all_good = False

            is_null = lambda value: value == 'YES'
            if not equal(is_null(field1.null), is_null(field2.null), 'null'):
                all_good = False

            if not equal(field1.default, field2.default, 'default'):
                all_good = False

        if all_good:
            self.output('All fields in sync')

        self.indent -= 1

def main():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', dest='config', default='dbconf',
                      metavar='FILE', help='database connections '
                      'configuration file')
    options, args = parser.parse_args()

    try:
        config = __import__(options.config.rstrip('.py'))
    except ImportError:
        message = 'Error: could not find config module "{0}"'
        print >> sys.stderr, message.format(options.config)
        sys.exit(1)

    c1 = Database(**config.DATABASE_A)
    c2 = Database(**config.DATABASE_B)

    comparer = SchemaComparer(c1, c2)

    if len(sys.argv) > 1:
        table = sys.argv[1]

        comparer.blank_line()
        comparer.output('Inspecting table {0}...'.format(table))
        comparer.blank_line()

        try:
            comparer.compare_fields(table)
            comparer.blank_line()

            comparer.compare_indexes(table)
            comparer.blank_line()
        except ValueError, err:
            comparer.output('Error: {0}', str(err))
            comparer.blank_line()
    else:
        comparer.blank_line()
        comparer.compare_tables()
        comparer.blank_line()
        comparer.output('To compare individual table details, run:')
        comparer.indent += 1
        comparer.output('$ {0} <tablename>', sys.argv[0])
        comparer.indent -= 1
        comparer.blank_line()

if __name__ == '__main__':
    main()

