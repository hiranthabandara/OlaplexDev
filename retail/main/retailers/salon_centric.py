import calendar
import logging
import pandas as pd
from datetime import date
from retail.main.retailer import Retailer
from retail.main.utils import str_to_num


def get_end_date(fiscal_month):
    """
    This function returns end date in %Y-%m-%d format
    This is used for client Salon Centric
    :param fiscal_month: '2020-M02 (Feb)'
    :return end_date: '2020-02-29'
    """
    fiscal_month = fiscal_month.split('(')[0].strip().replace('M', '')
    year, month = (int(x) for x in fiscal_month.split('-'))
    num_days = int(calendar.monthrange(year, month)[1])
    end_date = date(year, month, num_days).strftime("%Y-%m-%d")
    return end_date


def get_start_date(fiscal_month):
    """
    This function returns a tuple of dates in %Y-%m-%d format
    This is used for client Salon Centric
    :param fiscal_month : '2021-M04 (Feb)'
    :return start_date, end_date (tuple) : ('2020-02-01', '2020-02-29')
    """
    fiscal_month = fiscal_month.split('(')[0].strip().replace('M', '')
    start_date = f"{fiscal_month}-01"
    return start_date


class SalonCentric(Retailer):
    def __init__(self):

        super().__init__()

        self.sku_by_channel_mapping = {
            "Distribution Channel": "sell_through_channel",
            "Material Code": "product_retailer_sku",
            "Vendor Material Code": "product_sku",
            "Material": "product_name",
            "Net Sales Qty": "total_quantity",
            "Net Sls Sd": "total_value",
        }
        self.sku_by_channel_hardcoded = {
            'type': 'by_channel_sku',
            'reporting_period': 'Monthly'
        }

        self.dtypes_sku_by_channel = {
            "Distribution Channel": str,
            "Material Code": str,
            "Vendor Material Code": str,
            "Material": str
        }

        self.channel_by_state_mapping = {
            "Distribution Channel": "sell_through_channel",
            "Ship to State": "state",
            "Net Sales Qty": "total_quantity",
            "Net Sls Sd": "total_value",
        }

        self.channel_by_state_hardcoded = {
            'type': 'by_channel_state',
            'reporting_period': 'Monthly'
        }

        self.dtypes_channel_by_state = {
            "Distribution Channel": str,
            "Ship to State": str
        }

        self.store_mapping = {
            "Distribution Channel": "sell_through_channel",
            "Profit Center Code": "store_id",
            "Profit Center": "store_name",
            "Net Sales Qty": "total_quantity",
            "Net Sls Sd": "total_value",
            "AD Partner Store Rank": "tags"
        }

        self.store_hardcoded = {
            "type": "by_channel_(store)_sku",
            'reporting_period': 'Monthly'
        }
        self.dtypes_store = {
            "Distribution Channel": str,
            "Profit Center Code": str,
            "Profit Center": str,
            "AD Partner Store Rank": str
        }

        self.sub_distributor_mapping = {
            "Distribution Channel": "sell_through_channel",
            "Cust Lvl 4": "store_name",
            "Net Sales Qty": "total_quantity",
            "Net Sls Sd": "total_value"
        }

        self.sub_distributor_hardcoded = {
            "type": "by_channel_(sub_distributor)_sku",
            'reporting_period': 'Monthly'
        }

        self.dtypes_sub_distributor = {
            "Distribution Channel": str,
            "Cust Lvl 4": str
        }

        self.inventory_mapping = {
            "Plant": "plant_name",
            "Material Code": "product_retailer_sku",
            "Vendor Material Code": "product_sku",
            "MATERIAL DESC": "product_name",
            "Inv Total Qty": "quantity_warehouse"
        }

        self.inventory_hardcoded = {
            "type": "by_warehouse_sku",
            'reporting_period': 'Monthly'
        }

        self.dtypes_inventory = {
            "Plant": str,
            "Material Code": str,
            "Vendor Material Code": str,
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SC Monthly Brand Report - Olaplex - 02.2021',
                                'From': '[NA] SC Monthly Brand Reports <SCMonthlyBrandReports@saloncentric.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\SalonCentric\\1616101111_SC Monthly Brand Report - Olaplex
                                                - 03.2021.xlsx'
                            }
        :param file_name: 'SC Monthly Brand Report - Olaplex - 03.2021.xlsx'
        :param sheet_name: 'SKU by Channel'
        """
        if "SC Monthly Brand Report - Olaplex".lower():
            if sheet_name.lower() == 'SKU by Channel'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sku_by_channel_mapping,
                    hardcoded_dict=self.sku_by_channel_hardcoded,
                    dtypes=self.dtypes_sku_by_channel
                )

            elif sheet_name.lower() == 'Channel By State'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.channel_by_state_mapping,
                    hardcoded_dict=self.channel_by_state_hardcoded,
                    dtypes=self.dtypes_channel_by_state
                )
            elif sheet_name.lower() == 'Store'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.store_mapping,
                    hardcoded_dict=self.store_hardcoded,
                    dtypes=self.dtypes_store
                )
            elif sheet_name.lower() == 'Sub Distributor'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sub_distributor_mapping,
                    hardcoded_dict=self.sub_distributor_hardcoded,
                    dtypes=self.dtypes_sub_distributor
                )

            elif sheet_name.lower() == 'Inventory'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='inventory',
                    mapping_dict=self.inventory_mapping,
                    hardcoded_dict=self.inventory_hardcoded,
                    dtypes=self.dtypes_inventory
                )

            else:
                logging.info(f"Skipping {sheet_name} in {file_name}")
        else:
            logging.info(f"{file_name} is not a valid file")

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict, dtypes=None):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SC Monthly Brand Report - Olaplex - 02.2021',
                                'From': '[NA] SC Monthly Brand Reports <SCMonthlyBrandReports@saloncentric.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\SalonCentric\\1616101111_SC Monthly Brand Report - Olaplex
                                                - 03.2021.xlsx'
                            }
        :param sheet: 'SKU by Channel'
        :param report_type: 'sales'
        :param mapping_dict: sku_by_channel_mapping
        :param hardcoded_dict: sku_by_channel_hardcoded
        """
        try:
            df = pd.read_excel(report_dict['local_path'], sheet_name=sheet, dtype=dtypes)

            if report_type == 'sales':
                df['reporting_period_start'] = df['Fiscal Month'].apply(get_start_date)
                df['reporting_period_end'] = df['Fiscal Month'].apply(get_end_date)
                df.rename(columns=mapping_dict, inplace=True)

                df["total_quantity"] = df["total_quantity"].apply(str_to_num)
                df["total_value"] = df["total_value"].apply(str_to_num)
            else:
                df['effective_date'] = df['MONTH'].apply(get_end_date)
                df.rename(columns=mapping_dict, inplace=True)
                df["quantity_warehouse"] = df["quantity_warehouse"].apply(str_to_num)

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
