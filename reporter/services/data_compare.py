"""
This script takes two csv files and compares them based on the field provided.
For example, if a field called "JOBID" is provided, this program takes File1 and
File2 and compares them based on the "JOBID" field. This program determines whether the field provided are
identical in both files or not. If they are not identical, a report is exported illustrating the differences.

To Run:
python data_compare.py -f1 {File1} -f2 {file2} -c {Field to compare} -o {Report Output Location}
"""
import argparse
import os
import pandas as pd
from pathlib import Path
import pendulum
from prefect import task, Flow, Parameter

now = pendulum.now()
current_dir = Path.cwd()


def sort_nums(num1, num2):
    """utility function to return numbers in order"""
    if num1 > num2:
        return num1, num2
    elif num2 > num1:
        return num2, num1


def timestamper(task, old_state, new_state):
    """
    Task state handler that timestamps new states
    and logs the duration between state changes using
    the task's logger.
    """
    new_state.timestamp = pendulum.now("utc")
    if hasattr(old_state, "timestamp"):
        duration = (new_state.timestamp - old_state.timestamp).in_seconds()
        task.logger.info(
            "{} seconds passed in between state transitions".format(duration)
        )
    return new_state


@task(name='Gather Data', state_handlers=[timestamper])
def gather_data(instance):
    """
    Gathers paths and converts to data frames per the arguments provided. Multiple
    checks are in place to ensure data exists prior to processing.
    :param args: Supplied arguments when the program is initiated
    :return: 2 dataframes and arguments object
    """
    paths = [instance.get('file1'), instance.get('file2')]
    if Path(instance.get('file1')).parents[0].is_dir() is True and Path(
            instance.get('file2')).parents[0].is_dir() is True:
        files = [f for f in paths if os.path.isfile(f)]
        if len(files) == 0:
            raise Exception('The files you passed do not exist!')
        dfs = []
        for file in files:
            try:
                if file.endswith('.csv'):
                    dfs.append(pd.read_csv(file))

                else:
                    raise Exception('Please pass a file ending in .csv')

            except Exception as exc:
                formatted = "Unable to locate files! Please ensure you have provided accurate file paths. {}".format(
                    repr(exc))
                raise Exception(formatted)

        return dfs, instance

    else:
        raise Exception('Please pass a valid file path.')


@task(name='Clean Data', state_handlers=[timestamper])
def clean_data(data):
    """
    cleans and standardizes data
    :param data: 2 dataframes and arguments object
    :return: cleaned data sets
    """
    # switch dict to flip the names of the RE data set to match SF
    col_switch = {'fund_id': 'project_id',
                  'gift_count': 'record_count.5',
                  'total': 'sum_of_amount.5'}

    # define package items
    dfs = data[0]
    args = data[1]

    # clean columns in dataframes
    for df in dfs:
        df.columns = [x.lower() for x in df.columns]
        if 'fund id' in df.columns:
            df.columns = [x.lower().replace(' ', '_') for x in df.columns]
            df['source'] = 'RE'
            df.rename(columns=col_switch, inplace=True)
        else:
            df.columns = [x.lower().replace(' ', '_') for x in df.columns]
            df['source'] = 'SF'
            df.drop(columns=['sum_of_amount', 'record_count', 'sum_of_amount.1', 'record_count.1', 'sum_of_amount.2',
                             'record_count.2', 'sum_of_amount.3',
                             'record_count.3', 'sum_of_amount.4', 'record_count.4'], inplace=True)

    # standardize project_IDs
    for df in dfs:
        df['project_id_new'] = df['project_id'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df['project_id_new'] = df['project_id_new'].apply(
            lambda x: x.replace(' - ', '-').lower() if isinstance(x, str) and '-' in x else x)
        df['project_id_new'] = df['project_id_new'].apply(
            lambda x: x.replace('- ', '-').lower() if isinstance(x, str) and '-' in x else x)
        df['project_id_new'] = df['project_id_new'].apply(
            lambda x: x.replace(' -', '-').lower() if isinstance(x, str) and '-' in x else x)
        df['project_id_new'] = df['project_id_new'].apply(
            lambda x: x.replace('-', '_').lower() if isinstance(x, str) and '-' in x else x)
        df['project_id_new'] = df['project_id_new'].apply(
            lambda x: x.replace(' ', '_').lower() if isinstance(x, str) and ' ' in x else x)
        df['project_id_new'] = df['project_id_new'].apply(lambda x: x.lower() if isinstance(x, str) else x)

    # standardize sum_of_amount
    for df in dfs:
        df['sum_of_amount_new'] = df['sum_of_amount.5'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df['sum_of_amount_new'] = df['sum_of_amount_new'].apply(
            lambda x: x.replace('USD', '').lower() if isinstance(x, str) and 'USD' in x else x)
        df['sum_of_amount_new'] = df['sum_of_amount_new'].apply(
            lambda x: x.replace('$', '').lower() if isinstance(x, str) and '$' in x else x)
        df['sum_of_amount_new'] = df['sum_of_amount_new'].apply(
            lambda x: x.replace(',', '').lower() if isinstance(x, str) and ',' in x else x)
        df['sum_of_amount_new'] = df['sum_of_amount_new'].apply(
            lambda x: float("{0:.2f}".format(float(x))) if isinstance(x, str) else x)

    return dfs, args


@task(name='Compare Data', state_handlers=[timestamper])
def compare_data(data):
    """
    Using pandas, this compares the data via the field provided.
    :param data: 2 dataframes and arguments object
    :return: either a string stating a match or the difference data.
    """
    dfs = data[0]
    args = data[1]

    # ID the project IDs that do not exist in both data sets
    missing = []
    for value in dfs[0]['project_id_new']:
        if value not in [x for x in dfs[1]['project_id_new']]:
            missing.append(value)

    for value in dfs[1]['project_id_new']:
        if value not in [x for x in dfs[0]['project_id_new']] and value not in missing:
            missing.append(value)

    # join data on values that exist in both data sets
    df = pd.merge(dfs[0], dfs[1], how='inner', on='project_id_new')

    differ = {}

    for index, row in df.iterrows():
        project = row['project_id_new']
        if row['sum_of_amount_new_x'] != row['sum_of_amount_new_y']:
            num1, num2 = sort_nums(row['sum_of_amount_new_x'], row['sum_of_amount_new_y'])
            variance = num1 - num2
            differ.update({project: variance})

    return missing, differ, args


@task(name='Report Findings', state_handlers=[timestamper])
def report_findings(findings):
    """
    Compiles differences into a report and structures it so it is legible.
    :param findings: Output of the comparison function.
    :return: Report saved to the location specified in the initial arguments.
    """
    missing = pd.DataFrame(findings[0], columns=['project_id'])
    differ = pd.DataFrame(list(findings[1].items()), columns=['project_id', 'variance'])
    args = findings[2]

    if args['output'] is not False:
        if Path(args['output']).is_dir():
            # build filenames
            missing_report_destination = Path(args['output'] + 'missing_report_{}'.format(str(now)) + '.csv')
            differ_report_destination = Path(args['output'] + 'differ_report_{}'.format(str(now)) + '.csv')

            # output df to csv
            missing.to_csv(missing_report_destination, index=False)
            differ.to_csv(differ_report_destination, index=False)

    elif args['output'] is False:
        # build filenames
        missing_report_destination = Path(str(current_dir) + 'missing_report_{}'.format(str(now)) + '.csv')
        differ_report_destination = Path(str(current_dir) + 'differ_report_{}'.format(str(now)) + '.csv')

        # output df to csv
        missing.to_csv(missing_report_destination, index=False)
        differ.to_csv(differ_report_destination, index=False)


def run(args):
    with Flow('Compare Data') as flow:
        gather = gather_data(args)
        clean = clean_data(gather)
        compare = compare_data(clean)
        report_findings(compare)

    return flow.run()


def main():
    parser = argparse.ArgumentParser(description='Data Comparison Tool')
    parser.add_argument('-f1', '--file1', default=False, required=True,
                        help='first file path to be compared'),
    parser.add_argument('-f2', '--file2', default=False, required=True,
                        help='second file path to be compared'),
    parser.add_argument('-c', '--compare_column', default=False,
                        required=False,
                        help='enter the column to be used during the comparison'),
    parser.add_argument('-o', '--output', default=False, required=True,
                        help='location where the report is saved.')

    results = run(vars(parser.parse_args()))


if __name__ == '__main__':
    main()
