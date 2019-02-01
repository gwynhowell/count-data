#!/usr/bin/env python
# coding=utf-8

""" Script to count how many rows from a collection of csv files match a predefined set of filters.
    To set filters, edit the FILTERS dict below.
"""

import StringIO
import csv
import datetime
import os
import re
import time

# the max field size limit defaults to 131072 bytes (2**17).
# increase to 32 bits to accommodate for large csv files ...
csv.field_size_limit(2**32)

APP_NAME = 'CSV Counting & Analysis Tool'

DEFAULT_IN_DIR = 'unfiltered_CLC_data'
DEFAULT_OUT_FILE = 'results.csv'
DEFAULT_ERROR_FILE = 'errors.csv'
DEFAULT_FILTER_FILE = 'filters.csv'
DEFAULT_SAMPLE_FILTER_FILE = 'filters_SAMPLE.csv'

SUPPORTED_OPERATORS = ('=', '<', '<=', '>', '>=')

RE_FILE = '^.*_([0-9]{3})_g1_.*\.csv$'
RE_FILTER = '([=><]+)(.*)'

CSV_PATTERN = '_123_g1_'

ACTION_PRINT_CSVS = 1
ACTION_PRINT_FILTERS = 2
ACTION_RESCAN_CSVS = 3
ACTION_RESCAN_FILTERS = 4
ACTION_GENERATE_SAMPLE_FILTERS = 5
ACTION_ANALYSE = 6
ACTION_SETTINGS = 7
ACTION_HELP = 8
ACTION_EXIT = 9

ACTION_SETTING_EDIT_CASE_SENSITIVITY = 1
ACTION_SETTING_EDIT_CSV_PATH = 2
ACTION_SETTING_EDIT_FILTER_PATH = 3
ACTION_SETTING_EDIT_OUT_PATH = 4
ACTION_SETTING_RESTORE_DEFAULTS = 5

SAMPLE_FILTERS = (
    'Count,>= 10,>=100',
    'Coverage,>=10,>=100',
    'Forward read count,>=5,>=50',
    'Reverse read count,>=5,>=50',
    'dbSNP,BLANK,BLANK',
    'Type,"Deletion,Insertion,MNV,Replacement","Deletion,MNV"',
    'Frequency,>=2.5,IGNORE',
    'Non-synonymous,"Yes,No,-","Yes,-"',
    'COSMIC,BLANK,IGNORE')


class AnalysisException(Exception):
    def __init__(self, message, col_num=None):
        self.message = message
        self.col_num = col_num


class DuplicateCsvNumException(Exception):
    def __init__(self, num, fn1, fn2):
        self.num = num
        self.fn1 = fn1
        self.fn2 = fn2


class InvalidFilterOperatorException(Exception):
    def __init__(self, cell, condition):
        self.cell = cell
        self.condition = condition


class InvalidFilterValueException(Exception):
    def __init__(self, cell, condition):
        self.cell = cell
        self.condition = condition


class CsvFile(object):
    def __init__(self, num, filename):
        self.num = num
        self.filename = filename

        self.num_str = '{0:03d}'.format(num)


class App(object):
    def __init__(self):
        self.case_sensitive = True

        self.root_path = os.path.dirname(os.path.realpath(__file__))

        self.csv_path = os.path.join(self.root_path, DEFAULT_IN_DIR)
        self.out_file_path = os.path.join(self.root_path, DEFAULT_OUT_FILE)
        self.error_log_file_path = os.path.join(self.root_path, DEFAULT_ERROR_FILE)
        self.filter_file_path = os.path.join(self.root_path, DEFAULT_FILTER_FILE)
        self.sample_filter_file_path = os.path.join(self.root_path, DEFAULT_SAMPLE_FILTER_FILE)

        self.settings_path = os.path.join(os.path.expanduser("~"), '.csv-filter-analysis')

        self.load_settings()

        self.csv_filenames = []
        self.filters = []

        self.results = []
        self.error_log = []

        self.first_run = True

    def run(self):
        # attempt to silently load the csvs and filters now ...
        try:
            self._scan_for_csvs()
        except:
            # swallow the exception during startup ...
            pass

        try:
            self._scan_for_filters()
        except:
            # swallow the exception during startup ...
            pass

        action = None
        while True:
            print '\n########################################################################'
            print ' ' + APP_NAME
            print '########################################################################\n'

            if self.first_run:
                greeting = self.get_greeting()
                print greeting + '\n'
                self.first_run = False

            msg = ('{0} CSV file{1} detected\n'
                   '{2} Filter{3} detected\n\n'
                   'Choose an option from below:\n'
                   ' 1) Print CSV Files\n'
                   ' 2) Print Filters\n'
                   ' 3) Re-scan CSV Files\n'
                   ' 4) Re-scan Filters\n'
                   ' 5) Generate sample filters.csv file\n'
                   ' 6) Perform Analysis\n'
                   ' 7) View / Edit Settings\n'
                   # ' H) Help\n'
                   ' Q) Exit\n\n')
            msg = msg.format(len(self.csv_filenames),
                             '' if len(self.csv_filenames) == 1 else 's',
                             len(self.filters),
                             '' if len(self.filters) == 1 else 's',)

            action = raw_input(msg)

            try:
                action = int(action)
            except:
                pass

            if action == ACTION_EXIT or action == 'q':
                break
            elif action == ACTION_PRINT_CSVS:
                self.print_csv_filenames()
            elif action == ACTION_PRINT_FILTERS:
                self.print_filters()
            elif action == ACTION_RESCAN_CSVS:
                self.scan_for_csvs()
            elif action == ACTION_RESCAN_FILTERS:
                self.scan_for_filters()
            elif action == ACTION_GENERATE_SAMPLE_FILTERS:
                self.generate_sample_filters()
            elif action == ACTION_ANALYSE:
                self.analyse()
            elif action == ACTION_SETTINGS:
                self.view_settings()
            else:
                msg = '\nSorry, I do not understand "{0}". Hit any key to continue ...'.format(action)
                raw_input(msg)

        print '\nThanks for using me! Happy analysing :)\nBye!\n'

    def get_greeting(self, hour=None):
        if hour is None:
            hour = datetime.datetime.now().hour

        if hour > 22 or hour < 4:
            return 'Evening Guv\', you\'re working late'
        elif hour <= 9:
            return 'Morning Guv, early start this morning I see'
        elif hour <= 12:
            return 'Good Morning!'
        elif hour < 18:
            return 'Good Afternoon!'
        else:
            return 'Good Evening!'

    def scan_for_csvs(self):
        try:
            self._scan_for_csvs()

            print 'Scanning for CSVs, please wait ...'
            time.sleep(1)
            print 'Found {0} CSV files'.format(len(self.csv_filenames))
            time.sleep(1)
        except OSError:
            msg = ('\n*** Error: Folder Not Found ***'
                   '\nUnable to load CSV files, as the folder "{0}" was not found.'
                   '\nPlease ensure this folder exists, and contains CSV files with the pattern "{1}" in the filename.'
                   '\nAlternatively, you can configure the location of the CSV folder from the Settings menu.'
                   '\nHit any key to continue ...')
            msg = msg.format(self.csv_path, CSV_PATTERN)
            raw_input(msg)
        except DuplicateCsvNumException, e2:
            msg = ('ERROR: Duplicate number "{0}" found in "{1}" and "{2}".\n'
                   'Please ensure all filenames have a unique "{3}" number.\n'
                   'Hit any key to continue ...')
            msg = msg.format(e2.num, e2.fn1, e2.fn2, CSV_PATTERN)
            raw_input(msg)

    def _scan_for_csvs(self):
        num_fn = {}
        for filename in os.listdir(self.csv_path):
            r = re.match(RE_FILE, filename)
            if not r:
                continue
            num = r.group(1)
            if num in num_fn:
                # oops! the same number was found in different filenames ...
                raise DuplicateCsvNumException(num, num_fn[num], filename)

            num_fn[num] = filename

        csv_filenames = [CsvFile(int(k), v) for k, v in num_fn.items()]
        self.csv_filenames = csv_filenames

    def scan_for_filters(self):
        try:
            self._scan_for_filters()

            print 'Scanning for Filters, please wait ...'
            time.sleep(1)
            print 'Found {0} Filters'.format(len(self.filters))
            time.sleep(1)
        except OSError:
            msg = ('\n*** Error: Filter File Not Found ***'
                   '\nUnable to load Filters, as the file "{0}" was not found.'
                   '\nPlease ensure this file exists, and contains valid filters'
                   '\nTo generate a sample Filter file, return to the main menu and use option (5) "Generate sample Filters file"'
                   '\nHit any key to continue ...')
            msg = msg.format(self.filter_file_path, CSV_PATTERN)
            raw_input(msg)
        except InvalidFilterOperatorException, e1:
            msg = ('\n*** Error: Invalid Filter ***'
                   '\nInvalid Filter in Filters.csv cell {0}: {1}'
                   '\nFilter operator must be one of <, <=, > or >='
                   '\nHit any key to continue ...')
            msg = msg.format(e1.cell, e1.condition)
            raw_input(msg)
        except InvalidFilterValueException, e1:
            msg = ('\n*** Error: Invalid Filter ***'
                   '\nInvalid Filter in Filters.csv cell {0}: {1}'
                   '\nValue must be numeric'
                   '\nHit any key to continue ...')
            msg = msg.format(e1.cell, e1.condition)
            raw_input(msg)

    def _scan_for_filters(self):
        """ parses the filters.csv file and returns a data structure similar to the following:

            [{'field': '',  # name of field to filter on
              'op': '',  # operator, can be one of =, <, <=, >, >=
              'vals': []  # the value to filter on. = operations can have multile value to cater for OR's
            }]
        """

        filters = []
        with open(self.filter_file_path, 'rU') as f:
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

                    f = filters[y-1]

                    condition = col

                    r = re.match(RE_FILTER, condition)

                    if r and len(r.groups()) == 2:
                        op = r.group(1)
                        val = r.group(2)
                        if op not in SUPPORTED_OPERATORS:
                            raise InvalidFilterOperatorException(cell, condition)

                        # comparison has to be numeric
                        try:
                            val = float(val)
                        except:
                            raise InvalidFilterValueException(cell, condition)

                        f.append({'field': field,
                                  'op': op,
                                  'vals': [val]})
                    elif condition == 'BLANK':
                        # means match empty string ...
                        f.append({'field': field,
                                  'op': '=',
                                  'vals': ['']})

                    elif condition == 'IGNORE':
                        pass
                    else:
                        # the only other option is a comma separated list of strings, which serve as an OR condition
                        vals = map(lambda c: c.strip(), condition.split(','))
                        f.append({'field': field,
                                  'op': '=',
                                  'vals': vals})

        self.filters = filters

    def generate_sample_filters(self):
        if os.path.exists(self.sample_filter_file_path):
            msg = ('This will overwrite the existing sample filters file located at {0}\n'
                   'Do you wish to continue? (YES|NO)\n')

            msg = msg.format(self.sample_filter_file_path)
            action = raw_input(msg)
            if action.upper() == 'YES':
                self._generate_sample_filters()
            else:
                raw_input('\nAction cancelled. No changes have been made.\nHit any key to continue ...')
        else:
            self._generate_sample_filters()

    def _generate_sample_filters(self):
        print '\nGenerating sample filters file, please wait ...'
        time.sleep(1)

        with open(self.sample_filter_file_path, 'w') as f:
            f.write('\n'.join(SAMPLE_FILTERS))

        msg = ('A sample filters file has been generated.\n')
        msg += 'Do you wish to open the file now? (YES|NO)\n'

        a = raw_input(msg)
        if a.upper() == 'YES':
            os.system('open ' + self.sample_filter_file_path)

    def print_csv_filenames(self):
        if not self.csv_filenames:
            msg = ('\nNo CSV files have been found.'
                   '\nPlease return to the main menu and use option (3) "Re-scan CSV Files" to scan for CSV files to load into the app, then try again.\n'
                   '\nHit any key to continue ...')
            msg = msg.format(self.csv_path)
            raw_input(msg)
            return

        print '\nThe following CSV files have been identified for analysis:'
        for csv_file in self.csv_filenames:
            print ' {0}\t{1}'.format(csv_file.num_str, csv_file.filename)
        raw_input('\nHit any key to continue ...')

    def print_filters(self):
        if not self.filters:
            msg = ('\nNo Filters have been found.'
                   '\nPlease return to the main menu and use option (4) "Re-scan Filters" to scan for Filters to load into the app, then try again.\n'
                   '\nHit any key to continue ...')
            msg = msg.format(self.csv_path)
            raw_input(msg)
            return

        print '\nThe following Filters have been loaded for analysis:'
        for i, f in enumerate(self.filters):
            print '\nFilter {0}:'.format(i+1)
            for condition in f:
                val = condition['vals'][0]
                if len(condition['vals']) > 1:
                    val = ' OR '.join(condition['vals'])
                print '  {0} {1} {2}'.format(condition['field'],
                                             condition['op'],
                                             val if val else "''")
        raw_input('\nHit any key to continue ...')

    def analyse(self):
        if not self.filters or not self.csv_filenames:
            msg = ('It is not possible to start the analysis without at least 1 input CSV file and 1 filter.'
                   '\nPlease use options (3) or (4) to re-scan.')
            raw_input(msg)
            return

        msg = ('\nThis will analyse {0} filters against {1} files.\n'
               'Results will be saved to {2}\n'
               'WARNING: ANY EXISTING RESULTS WILL BE OVERWRITTEN!!!\n\n'
               'Do you wish to continue? (YES|NO)\n')

        msg = msg.format(len(self.filters), len(self.csv_filenames), self.out_file_path)
        action = raw_input(msg)
        if action.upper() == 'YES':
            print '\nAnalysing, please wait ...'
            time.sleep(1)

            self._do_analysis()
            self.write_results()
            self.write_errors()

            msg = ('\nAnalysis complete with {0} errors\n'
                   'Results have been saved to {1}\n').format(len(self.error_log), self.out_file_path)
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
        header = ['Num', 'File']
        for i, f in enumerate(self.filters):
            header.append('Filter {0}'.format(i + 1))
        self.results = [header]

        for csv_file in self.csv_filenames:
            result = [csv_file.num_str, csv_file.filename]

            fn = os.path.join(self.csv_path, csv_file.filename)
            with open(fn, 'rU') as f:
                reader = csv.reader(f, delimiter=',', dialect=csv.excel)

                headers = None
                rows = []
                for x, row in enumerate(reader):
                    if x == 0:
                        headers = row
                    else:
                        rows.append(row)

                for f in self.filters:
                    count = 0
                    for x, row in enumerate(rows):
                        try:
                            if self._check_filter(headers, row, f, csv_file.filename):
                                count += 1
                        except AnalysisException, e:
                            cell = ''
                            if e.col_num is not None:
                                cell = '%s%s' % (self._get_cell_ref(e.col_num+1), x+2)
                            self.error_log.append((csv_file.filename, cell, e.message))
                    result.append(count)
            self.results.append(result)

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
        while True:
            settings = {
                'case_sensitive': 'YES' if self.case_sensitive else 'NO',
                'csv_path': self.csv_path,
                'filter_file_path': self.filter_file_path,
                'out_file_path': self.out_file_path}

            msg = ('Settings:\n'
                   ' 1) Case Sensitive String Filters             {case_sensitive}\n'
                   ' 2) CSV Input Directory                       {csv_path}\n'
                   ' 3) Filters Input File                        {filter_file_path}\n'
                   ' 4) Results Output File                       {out_file_path}\n'
                   ' 5) Restore defaults\n\n'
                   'Enter a number to edit, or hit RETURN to go back to main menu\n\n')
            msg = msg.format(**settings)
            action = raw_input(msg)

            try:
                action = int(action)
            except:
                pass

            if action == ACTION_SETTING_EDIT_CASE_SENSITIVITY:
                self.edit_case_sensitivity()
            elif action == ACTION_SETTING_EDIT_CSV_PATH:
                self.edit_path('csv_path', 'Enter the path to the CSV Input Directory:', False)
            elif action == ACTION_SETTING_EDIT_FILTER_PATH:
                self.edit_path('filter_file_path', 'Enter the path to the Filters Input File:', True)
            elif action == ACTION_SETTING_EDIT_OUT_PATH:
                self.edit_path('out_file_path', 'Enter the path to the Results Output File:', True)
            elif action == ACTION_SETTING_RESTORE_DEFAULTS:
                self.restore_defaults()
            else:
                self.save_settings()
                break

    def save_settings(self):
        try:
            with open(self.settings_path, 'w') as f:
                settings = [self.root_path,
                            self.csv_path.replace(self.root_path, ''),
                            self.filter_file_path.replace(self.root_path, ''),
                            self.out_file_path.replace(self.root_path, ''),
                            '1' if self.case_sensitive else '0']
                f.writelines('\n'.join(settings))
        except:
            pass

    def load_settings(self):
        try:
            with open(self.settings_path, 'r') as f:
                settings = f.readlines()
                root_path = settings[0].strip()
                if root_path == self.root_path:
                    # only use the settings of the script is in the same location,
                    # otherwise all the relative paths with break

                    csv_path = settings[1].strip()
                    filter_file_path = settings[2].strip()
                    out_file_path = settings[3].strip()

                    self.csv_path = os.path.join(self.root_path, csv_path)
                    self.filter_file_path = os.path.join(self.root_path, filter_file_path)
                    self.out_file_path = os.path.join(self.root_path, out_file_path)
                    self.case_sensitive = settings[4].strip() == '1'
        except:
            pass

    def edit_case_sensitivity(self):
        msg = 'Case Sensitive String Filters is currently ENABLED.\nDo you wish to disable them? (YES|NO)\n'
        if not self.case_sensitive:
            msg = 'Case Sensitive String Filters is currently DISABLED.\nDo you wish to enable them? (YES|NO)\n'

        action = raw_input(msg)

        if action.upper() == 'YES':
            self.case_sensitive = not self.case_sensitive

    def edit_path(self, prop, msg, is_file):
        path = raw_input(msg + '\n')
        abs_path = self._get_absolute_path_or_file(path, is_file)

        if abs_path:
            setattr(self, prop, abs_path)
        else:
            raw_input('Path not found: {0}\n'.format(path))

    def _get_absolute_path_or_file(self, path, is_file):
        """ returns true if the path or file exists
            path can be absolute, relative to current file, or relative to user dir
        """

        # expand user directory if neseary ...
        if path.startswith('~'):
            path = os.path.expanduser(path)

        # check absolute path ...
        if self._validate_path_or_file_exists(path, is_file):
            return path

        # check relative path ...
        path = os.path.join(self.root_path, path)
        if self._validate_path_or_file_exists(path, is_file):
            return path

    def _validate_path_or_file_exists(self, path, is_file):
        """ returns true if the path or file exists """

        if is_file:
            return os.path.isfile(path)
        return os.path.exists(path)

    def restore_defaults(self):
        msg = ('This will restore all settings to their default values.\n'
               'Do you wish to continue? (YES|NO)\n')

        msg = msg.format(self.sample_filter_file_path)
        action = raw_input(msg)
        if action.upper() == 'YES':
            self.csv_path = os.path.join(self.root_path, DEFAULT_IN_DIR)
            self.out_file_path = os.path.join(self.root_path, DEFAULT_OUT_FILE)
            self.filter_file_path = os.path.join(self.root_path, DEFAULT_FILTER_FILE)

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
