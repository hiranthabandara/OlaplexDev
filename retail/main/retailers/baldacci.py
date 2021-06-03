import pandas as pd
from datetime import datetime
from retail.main.retailer import Retailer
from retail.main.utils import str_to_num


def get_start_date(filename):
    df_temp = pd.read_csv(filepath_or_buffer=filename,
                          delimiter=';',
                          nrows=4,
                          encoding='cp1252',
                          usecols=[0])
    start_date = df_temp.iloc[0].str.split(':')[0][1].split(' - ')[0].strip()
    return start_date


def get_end_date(filename):
    df_temp = pd.read_csv(filepath_or_buffer=filename,
                          delimiter=';',
                          nrows=4,
                          encoding='cp1252',
                          usecols=[0])
    end_date = df_temp.iloc[0].str.split(':')[0][1].split(' - ')[1].strip()
    return end_date


def get_effective_date(filename):
    df_temp = pd.read_csv(filepath_or_buffer=filename,
                          usecols=[0],
                          encoding='cp1252',
                          nrows=1)
    string_val = df_temp.iloc[0].str.split(':')[0][1].strip()
    characters_to_remove = [' ', ',', '$', ';']
    for ch in characters_to_remove:
        string_val = string_val.replace(ch, '')
    effective_date = str(datetime.strptime(string_val, "%Y-%m-%d").date())
    return effective_date


def parse_european_currency(value):
    """
    This method removes characters in the currency value
    :param value: ' 111,84'
    :return value: '111.84'
    """
    value = str(value).replace(',', '.').replace(' ', '')
    return float(value)


class Baldacci(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_mapping = {
            "Art.nr": "product_retailer_sku",
            "Artikelnamn": "product_name",
            "Antal": "total_quantity",
            "Försäljningspris (exkl moms)": "total_value"
        }
        self.sales_hardcoded = {
            "reporting_period": "Monthly",
            "currency": "SEK",
            "type": "by_sku"
        }

        self.inventory_mapping = {
            "Artikelnummer": "product_retailer_sku",
            "Benämning": "product_name",
            "Antal i lager": "quantity_physical",
            "Summa (exkl moms)": "value_physical"
        }
        self.inventory_hardcoded = {
            "type": "by_sku",
            "reporting_period": "Monthly"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None, encoding='cp1252'):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Baldacci Feb 2021',
                                'From': 'Nabin Sharma <nabin@beacondata.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\Baldacci\\1616101111_tb-rapport 2021 02.csv'
                            }
        :param file_name: 'tb-rapport 2021 02.csv'
        :param encoding: 'cp1252'
        """
        if "tb-rapport".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                report_type='sales',
                mapping_dict=self.sales_mapping,
                hardcoded_dict=self.sales_hardcoded
            )
        elif "stockvalue".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                report_type='inventory',
                mapping_dict=self.inventory_mapping,
                hardcoded_dict=self.inventory_hardcoded
            )
        else:
            pass

    def parse_sales_inventory(self, report_dict, report_type, mapping_dict, hardcoded_dict):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Baldacci Feb 2021',
                                'From': 'Nabin Sharma <nabin@beacondata.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\Baldacci\\1616101111_tb-rapport 2021 02.csv'
                            }
        :param report_type: 'sales'
        :param mapping_dict: sales_mapping
        :param hardcoded_dict: sales_hardcoded
        """
        try:
            if report_type == 'sales':
                df_original = pd.read_csv(filepath_or_buffer=report_dict['local_path'],
                                          delimiter=';',
                                          skiprows=4,
                                          skipfooter=3,
                                          encoding='cp1252',
                                          usecols=mapping_dict.keys())
                df = pd.DataFrame()
                for col in df_original.columns:
                    for k in mapping_dict.keys():
                        if col.lower() == k.lower():
                            df[k] = df_original[col]

                df.rename(columns=mapping_dict, inplace=True)
                df["total_quantity"] = df["total_quantity"].apply(str_to_num)
                df["total_value"] = df["total_value"].apply(parse_european_currency)
                df["reporting_period_start"] = get_start_date(report_dict['local_path'])
                df["reporting_period_end"] = get_end_date(report_dict['local_path'])
                df["product_name"] = [i.replace(',', '.') for i in df["product_name"]]

            else:
                df_original = pd.read_csv(filepath_or_buffer=report_dict['local_path'],
                                          delimiter=';',
                                          skiprows=6,
                                          skipfooter=3,
                                          encoding='cp1252',
                                          usecols=mapping_dict.keys())
                df = pd.DataFrame()
                for col in df_original.columns:
                    for k in mapping_dict.keys():
                        if col.lower() == k.lower():
                            df[k] = df_original[col]

                df.rename(columns=mapping_dict, inplace=True)
                df["value_physical"] = df["value_physical"].apply(parse_european_currency)
                df["effective_date"] = get_effective_date(report_dict['local_path'])

            # hardcoded fields
            if hardcoded_dict:
                for key in hardcoded_dict:
                    df[key] = hardcoded_dict[key]

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type=report_type,
            )

        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
            )
