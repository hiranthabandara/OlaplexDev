import logging
import os
from datetime import datetime, date, timedelta
import pandas as pd
from retail.main.retailer import Retailer
from airflow import AirflowException


def get_reporting_period_start(file_path, sheetname):
    """
    This method returns the reporting_period_start of a weekly report
    :param file_path: 'OLAPLEX Total Weekly Sales - March 2021.xlsx'
    :param sheetname: 'wc 21st March'
    :return: '2021-03-21'
    """
    file_name = os.path.basename(file_path)
    file_date = datetime.strptime(file_name.split('-')[1].strip().replace('.xlsx', ''), "%B %Y")

    df = pd.read_excel(file_path, nrows=1, usecols=[0], sheet_name=sheetname)
    value = str(df.columns[0])  # Eg: Total Weekly Sales - wc 21st March 2021
    r_date = value.split('wc ')[1]  # 21st March 2021

    characters_to_remove = ['rd', 'th', 'nd', 'st']
    for ch in characters_to_remove:
        r_date = r_date.replace(ch, '')
    r_date = r_date.replace(' ', '-')  # 21-March-2021

    try:
        r_date = datetime.strptime(r_date, "%d-%b-%Y")
        if file_date.year != r_date.year:
            logging.info(f'Value of year is not consistent. Taking year from file i.e., {file_name}')
            r_date = date(file_date.year, r_date.month, r_date.day)
        return r_date.strftime("%Y-%m-%d")

    except ValueError as e:
        r_date = datetime.strptime(r_date, "%d-%B-%Y")
        if file_date.year != r_date.year:
            logging.info(f'Value of year is not consistent. Taking year from file i.e., {file_name}')
            r_date = date(file_date.year, r_date.month, r_date.day)
        return r_date.strftime("%Y-%m-%d")

    except Exception as e:
        logging.error("Can't Convert date")
        raise AirflowException(e)


def get_reporting_period_end(reporting_period_start):
    """
    This method returns the reporting_period_end of a weekly report
    :param reporting_period_start: '2021-03-21'
    :return: '2021-03-27'
    """
    reporting_period_start = datetime.strptime(reporting_period_start, "%Y-%m-%d")
    e_date = (reporting_period_start + timedelta(days=6)).strftime("%Y-%m-%d")
    return e_date


class CultBeauty(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_mapping = {
            "Name": "product_name",
            "Ean": "product_retailer_sku",
            "Mpn": "product_sku",
            "Unit Sales": "total_quantity",
            "£ Sales": "total_value",
        }
        self.sales_hardcoded = {
            "reporting_period": "Weekly",
            "type": "by_sku",
            "currency": "GBP"
        }

    def _map_file(self, report_dict):
        """
        This method override super class method. This is because we only read the last non-empty sheet
        One file contains data for all the passing weeks
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Olaplex Weekly Sales - wc 14th March 2021',
                                'From': 'Elle McGivern <Elle.McGivern@cultbeauty.co.uk>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Mar 22, 2021 at 7:12 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\CultBeauty\\1616101111_OLAPLEX Total Weekly Sales - March 2021.xlsx'
                            }
        """
        file_name = os.path.basename(report_dict["local_path"])
        _, file_extension = os.path.splitext(os.path.basename(report_dict["local_path"]))

        # calling the map_destination method from sub class which will be implemented in the sub class
        try:
            if file_extension.lower() in [".xlsx", ".xls"]:
                xl = pd.read_excel(report_dict["local_path"], sheet_name=None)
                sheet_list = xl.keys()
                sheet_processed = list(sheet_list)[0]
                for sheet_name in list(sheet_list):
                    temp_df = pd.read_excel(io=report_dict["local_path"], sheet_name=sheet_name, skiprows=1,
                                            skipfooter=1)
                    sheet_processed = sheet_name if not temp_df.empty else sheet_processed

                self.map_destination(report_dict, file_name, sheet_processed)

            else:
                pass
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict
            )

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map_destination method is to figure out which method to call based on
        file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Olaplex Weekly Sales - wc 14th March 2021',
                                'From': 'Elle McGivern <Elle.McGivern@cultbeauty.co.uk>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Mar 22, 2021 at 7:12 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\CultBeauty\\1616101111_OLAPLEX Total Weekly Sales - March 2021.xlsx'
                            }
        :param file_name: 'OLAPLEX Total Weekly Sales - March 2021.xlsx'
        :param sheet_name: 'wc 21st March'
        """
        if "OLAPLEX Total Weekly Sales".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='sales',
                mapping_dict=self.sales_mapping,
                hardcoded_dict=self.sales_hardcoded
            )
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Olaplex Weekly Sales - wc 14th March 2021',
                                'From': 'Elle McGivern <Elle.McGivern@cultbeauty.co.uk>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Mar 22, 2021 at 7:12 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\CultBeauty\\1616101111_OLAPLEX Total Weekly Sales - March 2021.xlsx'
                            }
        :param sheet: 'wc 21st March'
        :param report_type: 'sales'
        :param mapping_dict: sales_mapping
        :param hardcoded_dict: sales_hardcoded
        """
        try:
            file_name = os.path.basename(report_dict["local_path"])
            df_original = pd.read_excel(report_dict['local_path'], sheet, skiprows=[0], skipfooter=1)
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]

            if df.empty:
                logging.info(f"""Empty Sheet encountered
                Email Subject : {report_dict['Subject']}
                File Name : {file_name}
                Sheet Name : {sheet}
                """)
                return None

            df.rename(columns=mapping_dict, inplace=True)
            df["reporting_period_start"] = get_reporting_period_start(report_dict['local_path'], sheet)
            df["reporting_period_end"] = df["reporting_period_start"].apply(get_reporting_period_end)

            # hardcoded fields
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
                sheet=sheet
            )
