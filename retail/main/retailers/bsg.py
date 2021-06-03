import os
import logging
import calendar
import pandas as pd
from datetime import date
from retail.main.retailer import Retailer
from retail.main.utils import str_to_num, get_retailer_info


def get_start_date(year, month):
    """
    This method returns the start date of the report
    :param year: '2021'
    :param month: '04'
    :return start_date: '2021-04-01'
    """
    start_date = date(year, month, 1).strftime("%Y-%m-%d")
    return start_date


def get_end_date(year, month):
    """
    This method returns the end date of the report
    :param year: '2021'
    :param month: '04'
    :return end_date: '2021-04-30'
    """
    num_days = int(calendar.monthrange(year, month)[1])
    end_date = date(year, month, num_days).strftime("%Y-%m-%d")
    return end_date


def get_country(currency):
    """
    This method returns the country of according to the value in the currency column
    :param currency: 'USD'
    :return country: 'US'
    """
    us_currency = [x.lower() for x in ('US Dollar', 'USD', 'US Dollars')]
    ca_currency = [x.lower() for x in ('Canadian Dollar', 'CAD', 'Canadian Dollars')]
    currency = currency.lower()
    if currency in us_currency:
        return 'US'
    elif currency in ca_currency:
        return 'CA'
    else:
        return None


class BSG(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.bsg_mapping = {
            "Channel": "sell_through_channel",
            "Region": "region",
            "SKU": "product_retailer_sku",
            "Manufacturer#": "product_sku",
            "Product": "product_name",
            "Size": "product_size",
            "Currency": "currency",
            "Qty": "total_quantity",
            "Sales": "total_value",
        }

        self.bsg_hardcoded = {
            "type": "by_region_channel_sku",
            'reporting_period': 'Monthly'
        }

        self.dtypes_bsg = {
            "Channel": str,
            "Region": str,
            "SKU": str,
            "Manufacturer#": str,
            "Product": str,
            "Size": str,
            "Currency": str
        }

        self.amlp_sales_to_franchisee_mapping = {
            "Channel": "sell_through_channel",
            "SKU": "product_retailer_sku",
            "Manufacturer#": "product_sku",
            "ProdName": "product_name",
            "ProdSize": "product_size",
            "Quantity": "total_quantity",
            "sales": "total_value",
        }

        self.amlp_sales_to_franchisee_hardcoded = {
            "type": "by_sku",
            'reporting_period': 'Monthly',
            "currency": "USD"
        }

    def _map_file(self, report_dict):
        """
        This method override the method in super class. This is because BSG have files with images which causes errors
        when reading the file using ReadExcel in retailer class
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] OLAPLEX- 2021 03 March',
                                'From': 'Murphy, Silvana <SMurphy@cosmoprofbeauty.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Apr 12, 2021 at 4:35 PM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\BSG\\1616101111_2021 03 March-OLAPLEX-SKU SALES.xlsx'
                            }
        """
        file_name = os.path.basename(report_dict["local_path"])
        if (report_dict["local_path"].lower().endswith('OLAPLEX Canada.xlsx'.lower()) or
                report_dict["local_path"].lower().endswith('OLAPLEX.xlsx'.lower())):
            logging.info(f"Skipping unnecessary file {file_name}")
        else:
            super()._map_file(report_dict)

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map function is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] OLAPLEX- 2021 03 March',
                                'From': 'Murphy, Silvana <SMurphy@cosmoprofbeauty.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Apr 12, 2021 at 4:35 PM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\BSG\\1616101111_2021 03 March-OLAPLEX-SKU SALES.xlsx'
                            }
        :param file_name: '1616101111_2021 03 March-OLAPLEX-SKU SALES.xlsx'
        :param sheet_name: 'BSG'
        """
        if "OLAPLEX-SKU SALES".lower() in file_name:
            if sheet_name.lower() == 'BSG'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.bsg_mapping,
                    hardcoded_dict=self.bsg_hardcoded,
                    dtypes=self.dtypes_bsg
                )
            # elif sheet_name.lower() == 'AMLP-Sales to Franchisee'.lower():
            #     self.parse_sales_inventory(
            #         report_dict=report_dict.copy(),
            #         sheet=sheet_name,
            #         report_type='sales',
            #         mapping_dict=self.amlp_sales_to_franchisee_mapping,
            #         hardcoded_dict=self.amlp_sales_to_franchisee_hardcoded
            #     )
            else:
                pass
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict, dtypes=None):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] OLAPLEX- 2021 03 March',
                                'From': 'Murphy, Silvana <SMurphy@cosmoprofbeauty.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Mon, Apr 12, 2021 at 4:35 PM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\BSG\\1616101111_2021 03 March-OLAPLEX-SKU SALES.xlsx'
                            }
        :param report_type: 'sales'
        :param mapping_dict: bsg_mapping
        :param hardcoded_dict: bsg_hardcoded
        :param dtypes: dtypes_bsg
        """
        try:
            df = pd.read_excel(io=report_dict['local_path'], sheet_name=sheet, dtype=dtypes)

            if report_type == 'sales':
                df['reporting_period_start'] = df.apply(lambda x: get_start_date(x.Year, x.Month), axis=1)
                df['reporting_period_end'] = df.apply(lambda x: get_end_date(x.Year, x.Month), axis=1)
                if sheet == 'BSG':
                    df['Country'] = df.apply(lambda x: get_country(x.Currency), axis=1)
                df.rename(columns=mapping_dict, inplace=True)
                df["total_quantity"] = df["total_quantity"].apply(str_to_num)
                df["total_value"] = df["total_value"].apply(str_to_num)

            else:
                pass

            if hardcoded_dict:
                for key in hardcoded_dict:
                    df[key] = hardcoded_dict[key]

            self.append_metadata(
                df=df,
                sheet=sheet,
                report_dict=report_dict,
                report_type=report_type
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict
            )
