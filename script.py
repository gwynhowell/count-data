#!/usr/bin/env python
# coding=utf-8

""" Script to count how many rows from a collection of csv files match a predefined set of filters.
    To set filters, edit the FILTERS dict below.
"""

import StringIO
import csv
import os
import re
import sys


DEFAULT_ROOT_DIR_PATH = os.path.expanduser('~/DNA_Filter_Script')
DEFAULT_IN_DIR_PATH = DEFAULT_ROOT_DIR_PATH + '/unfiltered_CLC_data'
DEFAULT_OUT_FILE_PATH = DEFAULT_ROOT_DIR_PATH + '/TMB_data/TMB_multiple_counts.csv'
DEFAULT_ERROR_LOG_FILE_PATH = DEFAULT_ROOT_DIR_PATH + '/errors.csv'
DEFAULT_FILTER_FILE_PATH = DEFAULT_ROOT_DIR_PATH + '/Filters.csv'

SUPPORTED_OPERATORS = ('=', '<', '<=', '>', '>=')

RE_FILE = '_([0-9]+)_.*\.csv'
RE_FILTER = '([=><]+)(.*)'

ACTION_PRINT_CSVS = 1
ACTION_PRINT_FILTERS = 2
ACTION_RESCAN_CSVS = 3
ACTION_RESCAN_FILTERS = 4
ACTION_ANALYSE = 5
ACTION_SETTINGS = 6
ACTION_EXIT = 9

ACTION_SETTING_EDIT_CASE_SENSITIVITY = 1


class AnalysisException(Exception):
    def __init__(self, message, col_num=None):
        self.message = message
        self.col_num = col_num


class App(object):
    def __init__(self):
        self.case_sensitive = True
        self.root_dir_path = DEFAULT_ROOT_DIR_PATH
        self.in_dir_path = DEFAULT_IN_DIR_PATH
        self.out_file_path = DEFAULT_OUT_FILE_PATH
        self.error_log_file_path = DEFAULT_ERROR_LOG_FILE_PATH
        self.filter_file_path = DEFAULT_FILTER_FILE_PATH

        self.in_files = []
        self.filters = []

        self.results = []
        self.error_log = []

        self.menu_count = 0

        self.in_files = self.get_in_files(fail_silent=True)
        self.filters = self.get_filters(fail_silent=True)

    def get_in_files(self, fail_silent=False):
        num_fn = {}
        for filename in os.listdir(self.in_dir_path):
            nums = re.findall(RE_FILE, filename)
            if len(nums) == 1:
                num = int(nums[0])
                if num in num_fn:
                    if fail_silent:
                        return []
                    msg = ('ERROR: Duplicate number "{0}" found in "{1}" and "{2}".\n'
                           'Please ensure all filenames have a unique "_xyz_" number.'.format(num, num_fn[num], filename))
                    sys.exit(msg)
                num_fn[num] = filename
            elif len(nums) > 1:
                if fail_silent:
                    return []
                msg = 'ERROR: Unable to determine number from file "%s".\n'
                msg += 'Number could be any of %s.\nPlease edit filename and try again.'
                msg = msg % (filename, ', '.join(nums))
                sys.exit(msg)

        filenames = [(k, v) for k, v in num_fn.items()]

        return filenames

    def get_filters(self, fail_silent=False):
        """ parses the filters.csv file and returns a data structure similar to the following:

            [{'field': '',  # name of field to filter on
              'op': '',  # operator, can be one of =, <, <=, >, >=
              'vals': []  # the value to filter on. = operations can have multile value to cater for OR's
            }]
        """

        filters = []
        with open(self.filter_file_path, 'r') as f:
            reader = csv.reader(f, delimiter=',', dialect=csv.excel)

            for x, row in enumerate(reader):
                field = None
                for y, col in enumerate(row):
                    cell = '%s%s' % (self._get_cell_ref(y+1), x+1)
                    if y == 0:
                        field = col
                        continue

                    if len(filters) < y:
                        filters.append([])

                    filter = filters[y-1]

                    condition = col

                    r = re.match(RE_FILTER, condition)

                    if r and len(r.groups()) == 2:
                        op = r.group(1)
                        val = r.group(2)
                        if op not in SUPPORTED_OPERATORS:
                            msg = ('Invalid Filter in Filters.csv cell %s: %s\n'
                                   'Filter must be one of <, <=, > or >=') % (cell, condition)
                            sys.exit(msg)

                        # comparison has to be numeric
                        try:
                            val = float(val)
                        except:
                            msg = ('Invalid filter condition in Filters.csv cell %s: "%s"\n'
                                   'Value "%s" must be numeric') % (cell, condition, val)
                            sys.exit(msg)

                        filter.append({'field': field,
                                       'op': op,
                                       'vals': [val]})
                    elif condition == 'BLANK':
                        # means match empty string ...
                        filter.append({'field': field,
                                       'op': '=',
                                       'vals': ['']})

                    elif condition == 'IGNORE':
                        pass
                    else:
                        # the only other option is a comma separated list of strings, which serve as an OR condition
                        vals = map(lambda c: c.strip(), condition.split(','))
                        filter.append({'field': field,
                                       'op': '=',
                                       'vals': vals})

        return filters

    def run(self):
        print '\n########################################################################'
        print ' Matthew\'s CSV Counting Tool'
        print '########################################################################\n'

        action = None
        while True:
            msg = ('Hello! I have found {0} csv files ripe for analysis, and {1} filters to run against them.\n'
                   'Choose an option from below:\n'
                   ' 1) Print Input CSV Files\n'
                   ' 2) Print Filters\n'
                   ' 3) Re-scan Input Files\n'
                   ' 4) Re-scan Filters\n'
                   ' 5) Perform Analysis\n'
                   ' 6) View / Edit Settings\n'
                   ' 9) Exit\n\n').format(len(self.in_files), len(self.filters))
            action = raw_input(msg)

            try:
                action = int(action)
            except:
                pass

            if action == ACTION_EXIT or action == 'q':
                break
            elif action == ACTION_PRINT_CSVS:
                self.print_in_files()
            elif action == ACTION_PRINT_FILTERS:
                self.print_filters()
            elif action == ACTION_RESCAN_CSVS:
                self.in_files = self.get_in_files()
            elif action == ACTION_RESCAN_FILTERS:
                self.filters = self.get_filters()
            elif action == ACTION_ANALYSE:
                self.analyse_in_files()
            elif action == ACTION_SETTINGS:
                self.view_settings()
            else:
                msg = '\nSorry, I do not understand "{0}". Hit any key to continue ...'.format(action)
                raw_input(msg)

            self.menu_count += 1

        print '\nThanks for using me! Happy analysing :)\nBye!\n'

    def print_in_files(self):
        print '\nThe files that have been identified for analysis are:'
        for filename in self.in_files:
            print ' - {0:03d}\t{1}'.format(*filename)
        raw_input('\nHit any key to continue ...')

    def print_filters(self):
        for i, filter in enumerate(self.filters):
            print '\nFilter {0}:'.format(i+1)
            for condition in filter:
                val = condition['vals'][0]
                if len(condition['vals']) > 1:
                    val = ' OR '.join(condition['vals'])
                print '  {0} {1} {2}'.format(condition['field'],
                                             condition['op'],
                                             val if val else "''")
        raw_input('\nHit any key to continue ...')

    def analyse_in_files(self):
        msg = ('\nThis will analyse {0} files against {1} filters.\n'
               'Results will be saved to {2}\n'
               'WARNING: ANY EXISTING RESULTS WILL BE OVERWRITTEN!!!\n\n'
               'Do you wish to continue? (YES|NO)\n')

        msg = msg.format(len(self.in_files), len(self.filters), self.out_file_path)
        action = raw_input(msg)
        if action.upper() == 'YES':
            print '\nAnalysing, please wait ...'
            self._do_analysis()
            self.write_results()
            self.write_errors()

            msg = ('Analysis complete with {0} errors.\n'
                   'Results have been saved to {0}\n')
            if self.error_log:
                msg += 'Errors have been logged in {0}'.format(self.error_log_file_path)
            msg += 'Do you wish to open the results now? (YES|NO)\n'

            msg = msg.format(len(self.error_log), self.out_file_path)

            a = raw_input(msg)
            if a.upper() == 'YES':
                os.system('open ' + self.out_file_path)
        else:
            raw_input('\nAnalysis cancelled. No changes have been made.\nHit any key to continue ...')

    def _do_analysis(self):
        self.results = []

        for file_num, filename in self.in_files:
            counts = [file_num, filename]

            fn = os.path.join(self.in_dir_path, filename)
            with open(fn, 'r') as f:
                reader = csv.reader(f, delimiter=',', dialect=csv.excel)

                headers = None
                rows = []
                for x, row in enumerate(reader):
                    if x == 0:
                        headers = row
                    else:
                        rows.append(row)

                for filter in self.filters:
                    count = 0
                    for x, row in enumerate(rows):
                        try:
                            if self._check_filter(headers, row, filter, filename):
                                count += 1
                        except AnalysisException, e:
                            cell = ''
                            if e.col_num is not None:
                                cell = '%s%s' % (self._get_cell_ref(e.col_num+1), x+2)
                            self.error_log.append((filename, cell, e.message))
                    counts.append(count)
            self.results.append(counts)

    def _check_filter(self, headers, row, filters, filename):
        for f in filters:
            field = f['field']
            op = f['op']
            vals = f['vals']

            if field not in headers:
                raise AnalysisException('Field "{0}" not found'.format(field))

            col_num = headers.index(field)
            val = row[col_num]

            try:
                if op == '=' and val not in vals:
                    return False
                elif op == '<' and not float(val) < float(vals[0]):
                    return False
                elif op == '<=' and not float(val) <= float(vals[0]):
                    return False
                elif op == '>=' and not float(val) >= float(vals[0]):
                    return False
            except ValueError:
                msg = 'Invalid data - greater/less than queries can only be performed on numeric data ({0} {1} {2})'
                msg = msg.format(val, op, val)
                raise AnalysisException(msg, col_num)

        return True

    def write_results(self):
        self._write_csv_data_to_file(self.out_file_path, self.results)

    def write_errors(self):
        self._write_csv_data_to_file(self.error_log_file_path, self.error_log)

    def _write_csv_data_to_file(self, filename, csv_data):
        """ writes csv data to a file
            Args:
                filename: the filename of the file to write to
                csv_data: a 2d list of data
        """

        s = StringIO.StringIO()
        w = csv.writer(s)

        for results in csv_data:
            w.writerow(results)

        data = s.getvalue()
        s.close()

        with open(filename, 'w') as f:
            f.write(data)

    def view_settings(self):
        settings = {
            'case_sensitive': 'YES' if self.case_sensitive else 'NO',
            'root_dir_path': self.root_dir_path,
            'in_dir_path': self.in_dir_path,
            'filter_file_path': self.filter_file_path,
            'out_file_path': self.out_file_path}

        msg = ('Settings:\n'
               ' 1) Case Sensitive String Filters             {case_sensitive}\n'
               ' 2) Root Directory                            {root_dir_path}\n'
               ' 3) CSV Directory                             {in_dir_path}\n'
               ' 4) Filters Input Filepath                    {filter_file_path}\n'
               ' 5) Results Output Filepath                   {out_file_path}\n\n'
               'Enter a number to edit, or hit RETURN to go back to main menu\n\n')
        msg = msg.format(**settings)
        action = raw_input(msg)

        try:
            action = int(action)
        except:
            pass

        if action == ACTION_SETTING_EDIT_CASE_SENSITIVITY:
            self.edit_case_sensitivity()

    def edit_case_sensitivity(self):
        msg = 'Case Sensitive String Filters is currently ENABLED.\nDo you wish to disable them? (YES|NO)\n'
        if not self.case_sensitive:
            msg = 'Case Sensitive String Filters is currently DISABLED.\nDo you wish to enable them? (YES|NO)\n'

        action = raw_input(msg)

        if action.upper() == 'YES':
            self.case_sensitive = not self.case_sensitive

        self.view_settings()

    def _get_cell_ref(self, n):
        string = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string


def main():
    app = App()
    app.run()


if __name__ == '__main__':
    main()
