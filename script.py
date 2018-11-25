#!/usr/bin/env python
# coding=utf-8

""" Script to count how many rows from a collection of csv files match a predefined set of filters.
    To set filters, edit the FILTERS dict below.
"""

import csv
import os

FILTERS = [{
    'field': 'age',
    'condition': '<',
    'value': '30'
}, {
    'field': 'colour',
    'condition': '=',
    'value': 'red'
}]

SUPPORTED_CONDITIONS = ('=', '<', '<=', '>', '>=')


def main():
    path = os.path.dirname(os.path.realpath(__file__))
    filenames = sorted(os.listdir(path))

    file_count = 0
    total_count = 0

    for filename in filenames:
        if not filename.endswith('.csv'):
            continue

        count = 0
        file_count += 1

        fn = os.path.join(path, filename)
        with open(fn, 'r') as f:
            reader = csv.reader(f, delimiter=',', dialect=csv.excel)

            headers = None
            for x, row in enumerate(reader):
                if x == 0:
                    headers = row
                    continue

                try:
                    if check_filters(headers, row, filename):
                        count += 1
                except Exception, e:
                    print e.message
                    return

        total_count += count
        print '{0} - {1}'.format(filename, count)

    if file_count == 0:
        print 'No CSV files found in {0}'.format(path)
        return

    print '\nScanned {0} files\nTotal count = {1}'.format(file_count, total_count)


def check_filters(headers, row, filename):
    for f in FILTERS:
        field = f['field']
        condition = f['condition']
        value = f['value']

        if field not in headers:
            raise Exception('Field "{0}" not found in file "{1}"'.format(field, filename))

        if condition not in SUPPORTED_CONDITIONS:
            raise Exception('Unsupported condition: {0}'.format(condition))

        val = row[headers.index(field)]

        try:
            if condition == '=' and val != value:
                return False
            elif condition == '<' and not float(val) < float(value):
                return False
            elif condition == '<=' and not float(val) <= float(value):
                return False
            elif condition == '>=' and not float(val) >= float(value):
                return False
        except ValueError:
            msg = 'Invalid data - greater/less than queries can only be performed on numeric data ({0} {1} {2})'
            msg = msg.format(val, condition, value)
            raise ValueError(msg)

    return True

if __name__ == '__main__':
    main()
