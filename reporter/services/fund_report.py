"""
This script takes two csv files and compares them based on the field provided.
For example, if a field called "JOBID" is provided, this program takes File1 and
File2 and compares them based on the "JOBID" field. This program determines whether the field provided are
identical in both files or not. If they are not identical, a report is exported illustrating the differences.

To Run:
python -m reporter.report_manager -f1 {File1} -f2 {file2} -c {Field to compare} -o {Report Output Location} -t {report_type}
"""
import argparse
import os
import pandas as pd
from pathlib import Path
import pendulum
from prefect import task, Flow, Parameter

now = pendulum.now()
current_dir = Path.cwd()


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


@task(name='Validate Submission', state_handlers=[timestamper])
def validate_submission(data):
    files = [data['file1'], data['file2']]
    for file in files:
        if len(files) < 2:
            raise Exception("You must submit two files!")
        if Path(file[0]).name == Path(file[1]).name:
            raise Exception("You must submit two different files!")
        if '.csv' not in Path(file).name.lower():
            raise Exception("{} is not a .csv... This program currently only accepts .csv files".format(file))

    return files


@task(name='Clean Data', state_handlers=[timestamper])
def clean_data(data):
    files = data
    dfs = {}
    data = {}

    cnt = 0
    # df the csvs provided
    for file in files:
        df = pd.read_csv(Path(file))
        df[df.columns] = df[df.columns].applymap(lambda x: str(x))
        df.columns = [x.replace(' ', '_').lower() for x in df.columns]
        for col in df.columns:
            if 'fund' in col and df[col].dtype == 'object':
                df[col] = [x.lower() for x in df[col]]
        dfs.update({cnt: df})
        cnt += 1

    # determine which df is the payments or the pledge data. The payments dataset
    # will always have significantly more columns
    if len(dfs[0]) > len(dfs[1]):
        data.update({'payments': dfs[0]})
        data.update({'pledge_data': dfs[1]})
    else:
        data.update({'payments': dfs[1]})
        data.update({'pledge_data': dfs[0]})

    return data


@task(name='Validate Data', state_handlers=[timestamper])
def validate_data(data):
    # key_cols = ['fundid', 'opportunity_re_recordid', 'paymentid']
    #
    # for key in data.keys():
    #     for col in key_cols:
    #         if col not in data[key].columns:
    #             raise Exception("{} is not a column in the dataset your analyzing. "
    #                             "{} is a required column, please include or rename "
    #                             "column in your dataset. ".format(col, col))

    return data


@task(name='Build Dict', state_handlers=[timestamper])
def build_dict(data):
    """
    Creates two dictionaries sorting funds by usergiftids in both datasets.

    :param data: two dataframes
    :return: two dictionaries
    """

    # dataframes to compare
    df_payments = data['payments']
    df_pledge = data['pledge_data']

    # populate payment_data dict
    payment_data = {}
    for index, row in df_payments.iterrows():
        pledge = row['usergiftid_pledge']
        fund = row['fund']
        if pledge not in payment_data.keys():
            payment_data.update({pledge: []})
        if fund not in payment_data[pledge]:
            payment_data[pledge].append(fund)

    # populate pledge_data dict
    pledge_data = {}
    for index, row in df_pledge.iterrows():
        giftid = row['gift_id']
        fund = row['fund_id']
        if giftid not in pledge_data.keys():
            pledge_data.update({giftid: []})
        if fund not in pledge_data[giftid]:
            pledge_data[giftid].append(fund)

    return {'payment_dict': payment_data,
            'pledge_dict': pledge_data,
            'dfs': data}


@task(name='First Pass', state_handlers=[timestamper])
def first_pass(data):
    """
    Takes dictionaries of data and if all the pledge payments in dataset 1 should only be allocated to the funds in
    dataset 2 and vice versa. Where usergiftids with inconsistencies are logged and returned in two separate lists.

    :param data: two dictionaries
    :return: two lists of results
    """
    # Run first comparison
    not_exist = []
    pledge_results = []

    # define dictionaries
    pledge_data = data['pledge_dict']
    payment_data = data['payment_dict']

    for ugi, fund in pledge_data.items():
        if len(fund) > 1:
            try:
                payment_funds = payment_data[ugi]
                for i in fund:
                    if i not in payment_funds:
                        pledge_results.append(ugi)
            except KeyError:
                not_exist.append(ugi)

    # Run second comparison
    not_exist_ = []
    payment_results = []
    payment_results_u = []
    for ugi, fund in payment_data.items():
        if len(fund) > 1:
            try:
                pledge_funds = pledge_data[ugi]
                for i in fund:
                    if i not in pledge_funds:
                        payment_results.append(ugi)
            except KeyError:
                not_exist_.append(ugi)

    for x in payment_results:
        if x not in payment_results_u:
            payment_results_u.append(x)

    return {'payment_results': payment_results_u,
            'pledge_results': pledge_results,
            'pledge_dict': pledge_data,
            'dfs': data['dfs']}


@task(name='Second Pass', state_handlers=[timestamper])
def second_pass(data):

    payment_results = data['payment_results']
    pledge_results = data['pledge_results']
    pledge_dict = data['pledge_dict']
    payments_df = data['dfs']['payments']
    pledge_df = data['dfs']['pledge_data']

    # using the identified usergiftids in the payment_results list to create a new dictionary of all
    # payments associated to those usergiftids
    payments_dict = {}
    for ugid in payment_results:
        for index, row in payments_df.iterrows():
            if row['usergiftid_pledge'] == ugid:
                paymentid = row['paymentid']
                if ugid not in payments_dict.keys():
                    payments_dict.update({ugid: []})
                if paymentid not in payments_dict[ugid]:
                    payments_dict[ugid].append(paymentid)

    # analysis
    # all of the funds that are associated to all the payments are associated to the usergiftids from payment_results
    analysis = {}
    for key, value in payments_dict.items():
        if len(value) > 1:
            for index, row in payments_df.iterrows():
                for i in value:
                    if row['paymentid'] == i:
                        fund = row['fund']
                        if key not in analysis.keys():
                            analysis.update({key: []})
                        if fund not in analysis[key]:
                            analysis[key].append(fund)

    # Run second comparison
    not_exist_ = []
    payment_results = []
    payment_results_u = []
    for ugi, fund in analysis.items():
        if len(fund) > 1:
            try:
                pledge_funds = pledge_dict[ugi]
                for i in fund:
                    if i not in pledge_funds:
                        payment_results.append(ugi)
            except KeyError:
                not_exist_.append(ugi)

    for x in payment_results:
        if x not in payment_results_u:
            payment_results_u.append(x)

    print(payment_results_u)


def run(args):
    with Flow('Compare Data') as flow:
        submission = validate_submission(args)
        clean = clean_data(submission)
        validate = validate_data(clean)
        build = build_dict(validate)
        first = first_pass(build)
        second_pass(first)

    results = flow.run()

    return results


def main():
    parser = argparse.ArgumentParser(description='Data Comparison Tool')
    parser.add_argument('-f1', '--file1', default=False, required=True,
                        help='first file path to be compared'),
    parser.add_argument('-f2', '--file2', default=False, required=True,
                        help='second file path to be compared'),
    parser.add_argument('-c', '--compare_column', default=False,
                        required=False,
                        help='enter the column to be used during the comparison'),
    parser.add_argument('-o', '--output', default=False, required=False,
                        help='location where the report is saved.')

    results = run(vars(parser.parse_args()))

    return results


if __name__ == '__main__':
    main()
