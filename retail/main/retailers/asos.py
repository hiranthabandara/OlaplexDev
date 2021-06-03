import os
import datetime
import pandas as pd
from zipfile import ZipFile
from retail.main.utils import str_to_num
from retail.main.config import app_config
from retail.main.retailer import Retailer


def get_start_date(file_name):
    """
    This function returns reporting_start date for sales report
    :param file_name: 'ASOS Weekly Sales Report Excel Details-Olaplex- Face and Body WB for 2021-01-04.xlsx'
    :return: 2020-01-01
    """
    dt = file_name.split()[-1].strip()
    dt = dt.replace('.xlsx', '')
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    weekday = dt.weekday()
    end_date = dt - datetime.timedelta(days=weekday + 1)
    start_date = end_date - datetime.timedelta(days=6)
    return start_date.strftime('%Y-%m-%d')


def get_end_date(file_name):
    """
    This function returns reporting_end date for sales report
    :param file_name: 'ASOS Weekly Sales Report Excel Details-Olaplex- Face and Body WB for 2021-01-04.xlsx'
    :return: 2021-01-31
    """
    dt = file_name.split()[-1].strip()
    dt = dt.replace('.xlsx', '')
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    weekday = dt.weekday()
    end_date = dt - datetime.timedelta(days=weekday + 1)
    return end_date.strftime('%Y-%m-%d')


def get_effective_date(file_name):
    """
    This function returns reporting_start date for sales report
    :param file_name: 'ASOS Weekly Sales Report Excel Details-Olaplex- Face and Body WB for 2021-01-04.xlsx'
    :return: 2021-01-01
    """
    dt = file_name.split()[-1].strip()
    dt = dt.replace('.xlsx', '')
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    weekday = dt.weekday()
    end_date = dt - datetime.timedelta(days=weekday)
    start_date = end_date - datetime.timedelta(days=1)
    return start_date.strftime('%Y-%m-%d')


class ASOS(Retailer):
    def __init__(self):
        super().__init__()

        # redshift col names are in right side
        self.sales_mapping = {
            'Category': 'product_line',
            'Style': 'product_retailer_sku',
            'Unnamed: 3': 'product_name',
            'Supplier Ref': 'product_sku',
            'Net Sales Unit': 'total_quantity',
            'Net Sales Value': 'total_value',
            'Returns Units': 'return_quantity',
        }
        self.sales_hardcoded = {
            "reporting_period": "Weekly",
            "currency": "GBP",
            "type": "by_sku"
        }

        self.inventory_mapping = {
            'Category': 'product_line',
            'Style': 'product_retailer_sku',
            'Unnamed: 3': 'product_name',
            'Supplier Ref': 'product_sku',
            'Stock Units': 'quantity_warehouse',
            'Stock Value (£)': 'value_warehouse'
        }
        self.inventory_hardcoded = {
            "reporting_period": "Weekly",
            "type": "by_sku",
            "currency": "GBP"
        }

    def _map_file(self, report_dict):
        """
        This method overide the method in super class as ASOS files are sent in password protected .zip format
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'Weekly ASOS Sales Report-Brand Excel Details',
                                'From': 'ASOS BI <biteam@asos.com>',
                                'To': 'Address for DRL Recipient <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 7:06 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\ASOS\\1616101111_ASOS Weekly Sales Report Excel.zip'
                            }
        """
        file_name = os.path.basename(report_dict["local_path"])
        _, file_extension = os.path.splitext(os.path.basename(report_dict["local_path"]))
        if file_extension in [".zip"] and 'ASOS Weekly Sales Report Excel' in file_name:
            with ZipFile(report_dict['local_path']) as myzip:
                myzip.extractall(path=self.download_path, pwd=app_config.asos_zip_password)
                extracted_file = os.path.join(self.download_path, myzip.namelist()[0])
                new_file = os.path.join(self.download_path, f"{report_dict['Timestamp']}_{myzip.namelist()[0]}")
                os.rename(extracted_file, new_file)
                report_dict['local_path'] = new_file

            super()._map_file(report_dict)

    def map_destination(self, report_dict, file_name, sheet_name=None, encoding='cp1252'):
        """
        The purpose of this map function is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'Weekly ASOS Sales Report-Brand Excel Details',
                                'From': 'ASOS BI <biteam@asos.com',
                                'To': 'Address for DRL Recipient <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 7:06 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\ASOS\\1616101111_ASOS Weekly Sales Report Excel.zip'
                            }
        :param file_name: '_data\\ASOS\\ASOS Weekly Sales Report Excel
                                Details-Olaplex- Face and Body WB for 2021-01-04.xlsx'
        :param sheet_name: 'sales'
        :param encoding: 'cp1252'
        """
        subject = report_dict['Subject']
        if " Weekly ASOS Sales Report-Brand Excel Details".lower() in subject.lower():
            if sheet_name == 'Brand Overview - Excel Detail (':
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    report_type='sales',
                    sheet=sheet_name,
                    mapping_dict=self.sales_mapping,
                    hardcoded_dict=self.sales_hardcoded
                )
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    report_type='inventory',
                    sheet=sheet_name,
                    mapping_dict=self.inventory_mapping,
                    hardcoded_dict=self.inventory_hardcoded
                )
            else:
                pass
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict):
        """
        This function is to parse relevant data according to the report type. Called in map destination function
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'Weekly ASOS Sales Report-Brand Excel Details',
                                'From': 'ASOS BI <biteam@asos.com>',
                                'To': 'Address for DRL Recipient <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 7:06 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\ASOS\\1616101111_ASOS Weekly Sales Report Excel.zip'
                            }
        :param sheet: 'sales'
        :param report_type: sales
        :param mapping_dict: sales_mapping
        :param hardcoded_dict: sales_hardcoded
        """
        try:
            df_original = pd.read_excel(io=report_dict['local_path'],
                                        skipfooter=1)
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]
            df.rename(columns=mapping_dict, inplace=True)

            if report_type == 'sales':
                df["total_quantity"] = df["total_quantity"].apply(str_to_num)
                df["total_value"] = df["total_value"].apply(str_to_num)
                df["reporting_period_start"] = get_start_date(report_dict['local_path'])
                df["reporting_period_end"] = get_end_date(report_dict['local_path'])

            else:
                df['effective_date'] = get_effective_date(report_dict['local_path'])

            if hardcoded_dict:
                for key in hardcoded_dict:
                    df[key] = hardcoded_dict[key]

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type=report_type,
                sheet=sheet
            )

        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
            )
