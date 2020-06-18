
import boto3
import pandas as pd
import time
import re

def upload_to_s3(local_file, bucket, s3_file, ACCESS_KEY = '<< Access_Key >>', 
                   SECRET_KEY = '<< Secret Key >>'):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    s3.upload_file(local_file, bucket, s3_file)

def make_dashboard(dataset, ACCESS_KEY = '<< Access_Key >>', 
                   SECRET_KEY = '<< Secret Key >>',
                   s3BucketName = "<< Bucket Name >>",
                   region_name = '<< Region >>',
                   accountid = '<< Account ID >>',
                   iamuser = '<< IAM user >>',
                   dstype = 'sales'):
    
    if dstype == 'sales':
        analysis = '<< Analysis ID >>'
        original_dataset = '<< Dataset ID >>'
    elif dstype == 'bank':
        analysis = '<< Analysis ID >>'
        original_dataset = '<< Dataset ID >>'
        
    quicksight = boto3.client('quicksight', region_name=region_name,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key= SECRET_KEY)
    
    filename = re.split(r'/|\\', dataset)[-1]
    upload_to_s3(dataset, s3BucketName, filename)
    print('File Uploaded')
    response = quicksight.list_data_sources(
        AwsAccountId=accountid
    )
    datasources = [x['DataSourceId'] for x in response['DataSources']]
    dsid = 'Data_Source_1'
    i = 1
    while dsid in datasources:
        i+=1
        dsid = dsid[:-1] + str(i)
    
    manifest = '''
    {
        "fileLocations": [
            {
                "URIs": [
                    "https://'''+ s3BucketName +'''.s3.amazonaws.com/''' + filename + '''"
                ]
            }
        ],
        "globalUploadSettings": {
            "format": "CSV",
            "delimiter": ",",
            "textqualifier": "'",
            "containsHeader": "true"
        }
    }
    '''
    
    with open('manifest.json', 'w') as f:
        f.write(manifest.strip())
    
    manifest_filename = filename.rsplit('.', 1)[0] + '.json'
    upload_to_s3('manifest.json', s3BucketName, manifest_filename)
    print('Manifest File Uploaded')
    response1 = quicksight.create_data_source(
        AwsAccountId=accountid,
        DataSourceId=dsid,
        Name='Data Source ' + str(i),
        Type='S3',
        DataSourceParameters={
            'S3Parameters': {
                'ManifestFileLocation': {
                    'Bucket': s3BucketName,
                    'Key': manifest_filename
                }
            }
        }
    )
    time.sleep(2)
    
    print('Data Source Created')
    
    #df = pd.read_csv(dataset)
    #columns = df.columns
    columns = "ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,ORDERDATE,STATUS,QTR_ID,MONTH_ID,YEAR_ID,PRODUCTLINE,MSRP,PRODUCTCODE,CUSTOMERNAME,PHONE,ADDRESSLINE1,ADDRESSLINE2,CITY,STATE,POSTALCODE,COUNTRY,TERRITORY,CONTACTLASTNAME,CONTACTFIRSTNAME,DEALSIZE".split(',')
    lst = []
    for i in columns:
        dic = {}
        dic['Name'] = i
        dic['Type'] = 'STRING'
        lst.append(dic)
    
    response2 = quicksight.list_data_sets(
        AwsAccountId=accountid
    )
    datasets = [x['DataSetId'] for x in response2['DataSetSummaries']]
    dsetid = 'Data_Set_1'
    i = 1
    while dsetid in datasets:
        i+=1
        dsetid = dsetid[:-1] + str(i)
    
    response3 = quicksight.create_data_set(
        AwsAccountId=accountid,
        DataSetId=dsetid,
        Name="Data Set " + str(i),
        PhysicalTableMap={
            'string': {
                'S3Source': {
                    'DataSourceArn': response1['Arn'],
                    'UploadSettings': {
                        'Format': 'CSV',
                        'StartFromRow': 1,
                        'ContainsHeader': True,
                        'Delimiter': ','
                    },
                    'InputColumns': lst
                }
            }
        },
        Permissions= [
            {
                "Principal": "arn:aws:quicksight:us-east-1:" + accountid + ":user/default/" + iamuser,
                "Actions": [
                    "quicksight:UpdateDataSetPermissions",
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions",
                    "quicksight:UpdateDataSet",
                    "quicksight:DeleteDataSet",
                    "quicksight:CreateIngestion",
                    "quicksight:CancelIngestion"
                ]
            }
        ],
        ImportMode='SPICE',
        LogicalTableMap={
        'string-Logical': {
            'Alias': 'string-1',
            'DataTransforms': [
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'MSRP',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'ORDERNUMBER',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'QTR_ID',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'MONTH_ID',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'SALES',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'YEAR_ID',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'QUANTITYORDERED',
                            'NewColumnType': 'INTEGER'
                    },
                },
                {
                    'CastColumnTypeOperation': {
                            'ColumnName': 'ORDERDATE',
                            'NewColumnType': 'DATETIME',
                            'Format': 'DD-MM-YYYY'
                    },
                },
                {
                    'TagColumnOperation': {
                        'ColumnName': 'COUNTRY',
                        'Tags': [
                            {
                                'ColumnGeographicRole': 'COUNTRY'
                            },
                        ]
                    }
                },
                {
                    'TagColumnOperation': {
                        'ColumnName': 'CITY',
                        'Tags': [
                            {
                                'ColumnGeographicRole': 'CITY'
                            },
                        ]
                    }
                },
                {
                    'TagColumnOperation': {
                        'ColumnName': 'STATE',
                        'Tags': [
                            {
                                'ColumnGeographicRole': 'STATE'
                            },
                        ]
                    }
                }
            ],
            'Source': {
                'PhysicalTableId': 'string'
            }
        }
    }
    )
    
    time.sleep(2)
    print('Data Set Created')
    response4 = quicksight.list_templates(
        AwsAccountId=accountid
    )
    templates = [x['TemplateId'] for x in response4['TemplateSummaryList']]
    tempid = 'Template_1'
    i = 1
    while tempid in templates:
        i+=1
        tempid = tempid[:-1] + str(i)
    
    response5 = quicksight.create_template(
        AwsAccountId=accountid,
        TemplateId=tempid,
        Name='Template ' + str(i),
        SourceEntity={
            'SourceAnalysis': {
                'Arn': "arn:aws:quicksight:us-east-1:"+ accountid +":analysis/" + analysis,
                'DataSetReferences': [
                    {
                        'DataSetPlaceholder': 'DS1',
                        'DataSetArn': 'arn:aws:quicksight:us-east-1:'+ accountid +':dataset/' + original_dataset
                    },
                ]
            }
        },
        VersionDescription='1'
    )
    
    time.sleep(2)
    print('Template Created')
    response6 = quicksight.list_dashboards(
        AwsAccountId=accountid
    )
    dashboards = [x['DashboardId'] for x in response6['DashboardSummaryList']]
    dashid = 'Dashboard_1'
    i = 1
    while dashid in dashboards:
        i+=1
        dashid = dashid[:-1] + str(i)
    
    response7 = quicksight.create_dashboard(
        AwsAccountId=accountid,
        DashboardId=dashid,
        Name='Automobile Sales Dashboard',
        Permissions=[
            {
                "Principal": "arn:aws:quicksight:us-east-1:"+ accountid +":user/default/" + iamuser,
                "Actions": [
                    "quicksight:DescribeDashboard",
                    "quicksight:ListDashboardVersions",
                    "quicksight:UpdateDashboardPermissions",
                    "quicksight:QueryDashboard",
                    "quicksight:UpdateDashboard",
                    "quicksight:DeleteDashboard",
                    "quicksight:DescribeDashboardPermissions",
                    "quicksight:UpdateDashboardPublishedVersion"
                ]
            },
        ],
        SourceEntity={
            'SourceTemplate': {
                'DataSetReferences': [
                    {
                        'DataSetPlaceholder': 'DS1',
                        'DataSetArn': response3['Arn']
                    },
                ],
                'Arn': response5['Arn']
            }
        },
        Tags=[
            {
                'Key': 'Name',
                'Value': 'Dashboard'
            },
        ],
        VersionDescription='1',
        DashboardPublishOptions={
            'AdHocFilteringOption': {
                'AvailabilityStatus': 'ENABLED'
            },
            'ExportToCSVOption': {
                'AvailabilityStatus': 'ENABLED'
            },
            'SheetControlsOption': {
                'VisibilityState': 'EXPANDED'
            }
        }
    )
    
    time.sleep(2)
    print('Dashboard Created')
    return r"https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/" + response7['DashboardId']
