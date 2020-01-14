# RE && FE Report Manager
As analysis and reporting has occurred during the transition between Raisers Edge and SalesForce,
all supporting reporting and analysis scripts have been compiled into a single python module. This module 
provides reports for the following:

- Standard Field Comparison
- Fund Analysis and Comparison

## Run
Individual requirements for each report are listed below. 

### Access program:
`python -m reporter.report_manager`

### Arguments: 
```
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
```

### Standard Field Comparison Report
This report provides a basic comparison between two fields. Those fields are currently predetermined, however,
efforts are underway to make this ad-hoc. 
#### Requirements: 
The program requires two files with the same column headers and an output location for the report.
#### Command
`python -m reporter.report_manager -f1 {file-one} -f2 {file-2} -o {output-location} -t standard_comparison`

### Fund Analysis Report
Analysis to determine if all payments in dataset associated to pledge are allocated to the same funds in both data sets.
(i.e. file1 && file2)
#### Requirements: 
The program requires two files with the same key column headers (fund, giftid, paymentid) and an output location for the report.
#### Command
`python -m reporter.report_manager -f1 {file-one} -f2 {file-2} -o {output-location} -t fund_analysis`