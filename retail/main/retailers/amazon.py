import pandas as pd
from datetime import datetime
from retail.main.utils import str_to_num
from retail.main.retailer import Retailer
from retail.main.config import *


class Amazon(Retailer):
    def __init__(self):
        """
        This is the super class of Amazon retailers. Initialize Amazon class which is a sub class of Retailer Class
       """
        self.sales_mapping = None
        self.inventory_mapping = None

        super().__init__()

    def get_reporting_periods(self, input_file, identifier='Viewing=', date_format="%m/%d/%y"):
        """
        This function is to parse reporting periods from specific place of a file
        having following format : Viewing=[2/1/21 - 2/28/21]
        :param input_file: '\\_data\\AmazonCA\\1616086825_Amazon CA Monthly Sales Diagnostics_2021-04.csv'
        :param identifier: 'Viewing=',
        :param date_format: "%m/%d/%y"
        :return: 2012-12-01, 2012-12-31
        """
        df = pd.read_csv(input_file, nrows=0)
        reporting_period_str = None
        for item in df.columns:
            if item.startswith(identifier):
                reporting_period_str = item

        start = datetime.strptime(reporting_period_str.split('=')[1].split('-')[0].replace('[', '').strip(),
                                  date_format)
        end = datetime.strptime(reporting_period_str.split('=')[1].split('-')[1].replace(']', '').strip(), date_format)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def parse_sales_inventory(self, report_dict, report_type, date_format="%d/%d/%y"):
        """
        Process the report_dict dictionary according to the report type
        This method is common to all the sub classes of Amazon
        :param report_dict: {
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'Jamie Kaltenbach <jamie@olaplex.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': 'Amazon US Vendor Central Monthly Reports - April 2021',
                                'Timestamp': 1616086825,
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'local_path': '\\_data\\AmazonUS\\1616086825_Amazon US Monthly Inventory Health_2021-04.csv'
                            }
        :param report_type: "sales"
        :param date_format: "%d/%d/%y"
        """
        df = None
        try:
            if report_type == 'sales':
                df = pd.read_csv(report_dict['local_path'], skiprows=1, usecols=self.sales_mapping.keys())
                df.rename(columns=self.sales_mapping, inplace=True)
                df["total_quantity"] = df["total_quantity"].apply(str_to_num)
                df["total_value"] = df["total_value"].apply(str_to_num)
                df['reporting_period_start'], df['reporting_period_end'] = self.get_reporting_periods(
                    input_file=report_dict['local_path'],
                    identifier='Viewing=',
                    date_format=date_format)

            else:
                df = pd.read_csv(report_dict['local_path'], skiprows=1, usecols=self.inventory_mapping.keys())
                df.rename(columns=self.inventory_mapping, inplace=True)
                df["quantity_warehouse"] = df["quantity_warehouse"].apply(str_to_num)
                df["quantity_intransit"] = df["quantity_intransit"].apply(str_to_num)
                df["value_warehouse"] = df["value_warehouse"].apply(str_to_num)
                _, df['effective_date'] = self.get_reporting_periods(
                    input_file=report_dict['local_path'],
                    identifier='Viewing=',
                    date_format=date_format)
            # hardcoded fields
            df['reporting_period'] = 'Monthly'
            df["type"] = "by_sku"
            df["currency"] = "USD"

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type=report_type
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict
            )


class AmazonUS(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue": "total_value",
            "Subcategory (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon US Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonUS\\1616101111_Amazon US Monthly Sales_2021-04.csv'
                            }
        :param file_name: Amazon US Monthly Sales_2021-04.csv
        :param sheet_name: None
        """
        if ("Amazon US Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%m/%d/%y")
        elif ("Amazon US Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%m/%d/%y")
        else:
            pass


class AmazonGB(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model/style number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue": "total_value",
            "Sub-category (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model/style number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon GB Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonGB\\1616101111_Amazon GB Monthly Sales_2021-04.csv'
                            }
        :param file_name: Amazon GB Monthly Sales_2021-04.csv
        :param sheet_name: None
        """
        if ("Amazon GB Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%d/%m/%y")
        elif ("Amazon GB Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%d/%m/%y")
        else:
            pass


class AmazonDE(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue": "total_value",
            "Subcategory (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon DE Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonDE\\1616101111_Amazon DE Monthly Sales_2021-04.csv'
                            }
        :param file_name: Amazon DE Monthly Sales_2021-04.csv
        :param sheet_name: None
        """
        if ("Amazon DE Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%m/%d/%y")
        elif ("Amazon DE Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%m/%d/%y")
        else:
            pass


class AmazonFR(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue": "total_value",
            "Subcategory (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon FR Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonFR\\1616101111_Amazon FR Monthly Sales_2021-04.csv'
                            }
        :param file_name: 'Amazon GB Monthly Sales_2021-04.csv'
        :param sheet_name: None
        """
        if ("Amazon FR Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%m/%d/%y")
        elif ("Amazon FR Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%m/%d/%y")
        else:
            pass


class AmazonES(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue": "total_value",
            "Subcategory (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon ES Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonES\\1616101111_Amazon ES Monthly Sales_2021-04.csv'
                            }
        :param file_name: Amazon FR Monthly Sales_2021-04.csv
        :param sheet_name: None
        """
        if ("Amazon ES Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%m/%d/%y")
        elif ("Amazon ES Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%m/%d/%y")
        else:
            pass


class AmazonCA(Amazon):

    def __init__(self):
        super().__init__()

        # Variables to map data between the file data and redshift tables
        # redshift col names are in right side
        self.sales_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Ordered Units": "total_quantity",
            "Ordered Revenue\r\n": "total_value",
            "Subcategory (Sales Rank)": "tags"
        }

        self.inventory_mapping = {
            "ASIN": "product_retailer_sku",
            "Model / Style Number": "product_sku",
            "Product Title": "product_name",
            "Subcategory": "product_line",
            "Sellable On Hand Units": "quantity_warehouse",
            "Open Purchase Order Quantity": "quantity_intransit",
            "Sellable On Hand Inventory": "value_warehouse"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name
        :param report_dict: {
                            'ID': '1784721fdb3cad7d',
                            'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                            'Subject': 'Amazon CA Monthly Sales Reports',
                            'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                            'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                            'Timestamp': 1616101111,
                            'local_path': '_data\\AmazonCA\\1616101111_Amazon CA Monthly Sales_2021-04.csv'
                            }
        :param file_name: 'Amazon CA Monthly Sales_2021-04.csv'
        :param sheet_name: None
        """
        if ("Amazon CA Monthly Sales".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='sales',
                                       date_format="%d/%m/%y")
        elif ("Amazon CA Monthly Inventory".lower() in file_name.lower()):
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       report_type='inventory',
                                       date_format="%d/%m/%y")
        else:
            pass
