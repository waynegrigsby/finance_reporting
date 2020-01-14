import argparse
import datetime
from .services.fund_report import run as run_fund_report
from .services.data_compare import run as run_standard_compare


now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")


class ReportGen:
    """
    This class is used as a shell for future development, and will serve as the entry point for any and all report
    logging and validation checks prior to the prefect pipeline.
    """
    def __init__(self, args):
        self.args = args
        self.files = args['file1'], args['file2']
        if 'output' in args.keys():
            self.output_report_location = args['output']
        self.type = args['type']
        self.results = self.run_report

    @staticmethod
    def check_arguments():
        # """
        # Checks whether the file is an spss or csv file
        # :return: file type (spss or csv)
        # """
        # if str(self.file).endswith('.sav'):
        #     details = 'This is a .sav file'
        #     return {'value': 'sav', 'log_status': 1, 'log_details': details}
        #
        # elif str(self.file).endswith('.csv'):
        #     details = 'This is a csv file.'
        #     return {'value': 'csv', 'log_status': 1, 'log_details': details}
        # else:
        #     # return a failure or Raise Exception
        #     return {'value': '', 'log_status': -1,
        #             'log_details': 'The wrong file type was passed'}
        return 0

    @staticmethod
    def prep_for_report(self):

        return 0

    @staticmethod
    def results_report(self):
        """
        This function turns the dictionary of qc check results into a
        dataframe.

        :return: the results formatted into a dataframe and the generated
        filename
        """

        return 0

    @staticmethod
    def run_report(self):
        """
        This function checks if values exist within each existing required
         column. Results "pass"/"fail" are passed to a results dict.

        :return: dictionary of results (pass/fail)
        """


class Reporter(ReportGen):
    """
    This class serves as the runner for all reports.
    """
    def __init__(self, *args):
        super().__init__(*args)

    def run_checks(self):
        """
        Using functions from the master class, this function executes the
        checks in order and outputs the final results into a dict.

        :return: Final results dict
        """
        if self.type == 'fund_analysis':
            self.results = run_fund_report(self.args)
            return self.results_report(self)
        elif self.type == 'standard_comparison':
            self.results = run_standard_compare(self.args)
            return self.results_report(self)


def main():
    parser = argparse.ArgumentParser(description='Finance Report Generator')
    parser.add_argument('-t', '--type', required=True, choices=['standard_comparison', 'fund_analysis'],
                        help='pick which type of report you need {}'),

    parser.add_argument('-f1', '--file1', required=False,
                        help='file or directory of files to analyze'),

    parser.add_argument('-f2', '--file2', required=False,
                        help='file or directory of files to analyze'),

    parser.add_argument('-dir', '--dir', required=False,
                        help='file or directory of files to analyze'),

    parser.add_argument('-o', '--output_report', required=False,
                        help='submit a location where you would like a csv '
                             'report output to.'),

    args = vars(parser.parse_args())

    runner = {'fund_analysis': Reporter(args).run_checks,
              'standard_comparison': Reporter(args).run_checks}

    results = runner.get(args.get('type'))()


if __name__ == '__main__':
    main()
