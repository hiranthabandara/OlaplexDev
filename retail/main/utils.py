import hashlib
import pandas as pd
from datetime import datetime
import dateutil


def str_to_num(string_val, characters_to_remove=(' ', ',', '$')):
    """
    This method cleans a string value when there are characters
    :param string_val: '$123,32'
    :param characters_to_remove: (' ',',','$')
    :return: 12332
    """
    string_val = str(string_val)
    for ch in characters_to_remove:
        string_val = string_val.replace(ch, '')
    return float(string_val)


def generate_record_id(report_id, row_number):
    """
    Generate unique record_id for each row received in a email using (report_id + row_number)
    :param report_id: 'rg59de3add4cf86bdb267gaddga3d2as'
    :param row_number: '0'
    :return: record_id: '0259de37d44cf86bdb9d26e7619a3d2b'
    """
    hashing = f"{report_id}|{row_number}"
    record_id = hashlib.md5(hashing.encode()).hexdigest()
    return record_id


def generate_report_id(file_name, reporting_period, end_date, retailer_id, num_records, sheet_name=None):
    """
    Generate report_id value using (file_name + reporting_period + end_date + retailer_id + num_records + sheet_name)
    This is represents reports uniquely
    :param file_name: '1616086825_ADI Inventory Report 2021-01-31.xlsx'
    :param reporting_period: 'Monthly'
    :param end_date: '2021-01-31'
    :param retailer_id: 'C033038 ADI srl'
    :param num_records: '1233'
    :param sheet_name: 'Sales'
    :return: uuid (str) : '0259de37d44cf86bdb9d26e7619a3d2b'
    """
    file_name = file_name.split('_', maxsplit=1)[1]  # Trimming the Timestamp from filename
    hashing = f"{file_name}|{sheet_name}|{reporting_period}|{end_date}|{retailer_id}|{num_records}"
    report_id = hashlib.md5(hashing.encode()).hexdigest()
    return report_id


def generate_uuid(data_row):
    """
    Generate unique uuid value using (data_row) this is unique to each row. Duplicate records have same uuid.
    Using this you can identify duplicate records
    :param data_row:
    :return: uuid : '0259de37d44cf86bdb9d26e7619a3d2b'
    """
    uuid = hashlib.md5(data_row.encode()).hexdigest()
    return uuid


def get_retailer_info(class_name, retailer_info_file='retailer_info.csv'):
    """
    This method loads retailer details when requested.
    Following data are called, retailer_id, retailer_internal_id, email_label, file_extensions.
    :param class_name: 'ADI'
    :param retailer_info_file: 'retailer_info.csv'
    :return: retailer_info : {
                                retailer_id : 'C033038 ADI srl'
                                retailer_internal_id : '128883'
                                email_label : 'Retail_Reports-ADI'
                                extensions : 'xlsx'
                            }
    """
    df = pd.read_csv(retailer_info_file)
    retailer_info = {}
    if class_name in list(df.retailer_class_name):
        record = df[df.retailer_class_name == class_name].to_dict('records')
        if len(record) > 1:
            retailer_info['retailer_internal_id'] = ''
            retailer_info['retailer_id'] = ''
        else:
            retailer_info['retailer_internal_id'] = str(record[0]['retailer_internal_id'])
            retailer_info['retailer_id'] = record[0]['retailer_id']

        retailer_info["email_label"] = record[0]['email_label']
        retailer_info["file_extensions"] = record[0]['file_extensions'].split(';')

    return retailer_info


def get_standard_date(input_date):
    """
    This method is a global method for getting the date in the '%Y-%m-%d' format. Can read many date formats.
    :param input_date: '2021-02-01'
    :return: datetime: 2021-02-01
    """
    try:
        if isinstance(input_date, datetime):
            return input_date.strftime('%Y-%m-%d')
        else:
            return dateutil.parser.parse(input_date).strftime('%Y-%m-%d')
    except:
        return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(input_date) - 2).strftime('%Y-%m-%d')
