import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from retail.main.retailer import Retailer


def get_iso_first_date(iso_year, iso_week):
    """
    Returns ISO date range from year and week number
    :param iso_year: '2021'
    :param iso_week: '14'
    :return last_day: '2021-03-27'
    """
    try:
        iso_year = int(float(iso_year))
        iso_week = int(float(iso_week))
        # first_day = datetime.strptime(f'{iso_year}-W{int(iso_week )- 1}-1', "%Y-W%W-%w").date()
        first_day = datetime.strptime(f'{iso_year}-W{iso_week}-1', "%G-W%V-%u").date()
        first_day = first_day.strftime('%Y-%m-%d')
        return first_day
    except Exception as e:
        logging.error(e)
        raise


def get_iso_last_date(iso_year, iso_week):
    """
    Returns ISO date range from year and week number
    :param iso_year: '2021'
    :param iso_week: '14'
    :return last_day: '2021-04-02'
    """
    try:
        iso_year = int(float(iso_year))
        iso_week = int(float(iso_week))
        # first_day = datetime.strptime(f'{iso_year}-W{int(iso_week )- 1}-1', "%Y-W%W-%w").date()
        first_day = datetime.strptime(f'{iso_year}-W{iso_week}-1', "%G-W%V-%u").date()
        last_day = first_day + timedelta(days=6.9)
        last_day = last_day.strftime('%Y-%m-%d')
        return last_day
    except Exception as e:
        logging.error(e)
        raise


def get_effective_date_thg(file_name):
    """
    Get effective date from THG Stock View file
    :param file_name: 'stock view olaplex 24.08.2020 - 175b21a27ee195f5.xlsx'
    :return date: '2020-08-24'
    """
    try:
        file_name = os.path.basename(file_name)
        file_name = file_name.lower()
        dt_string = file_name.strip().split("-")[0].strip().split("stock view olaplex")[1].strip()
        year = dt_string.split(".")[2].strip()
        month = dt_string.split(".")[1].strip()
        day = dt_string.split(".")[0].strip()
        return f"{year}-{month}-{day}"
    except Exception as e:
        logging.error(e)
        raise


def get_inventory_dataframe(file_name):
    """
    Return a combined dataframe for stock view file used in inventory
    :param file_name: 'THG Weekly UK Report 2021-04-12.xlsx'
    :return result: DataFrame
    """
    df_omega = pd.read_excel(io=file_name, usecols=[1, 2, 5, 6], skiprows=1)
    df_poland = pd.read_excel(io=file_name, usecols=[1, 2, 13, 14], skiprows=1)
    df_omega['plant_name'] = "Omega"
    df_poland['plant_name'] = "Poland"
    poland_cols = {
        "SKU": "product_retailer_sku",
        "TITLE": "product_name",
        "Current stock.1": "quantity_physical",
        "On order.1": "quantity_intransit",
        'plant_name': 'plant_name',
    }
    omega_cols = {
        "SKU": "product_retailer_sku",
        "TITLE": "product_name",
        "Current stock": "quantity_physical",
        "On order": "quantity_intransit",
        'plant_name': 'plant_name',
    }
    df_poland.rename(columns=poland_cols, inplace=True)
    df_omega.rename(columns=omega_cols, inplace=True)
    result = pd.concat([df_poland, df_omega])
    return result


class THG(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_mapping = {
            'site_name': 'store_name',
            'title': 'product_name',
            'Product_ID': 'product_retailer_sku',
            'units': 'total_quantity',
            'revenue_USD': 'total_value'
        }
        self.sales_hardcoded = {
            "reporting_period": "Weekly",
            "sell_through_channel": "Online",
            "currency": "USD",
            "type": "by_sku"
        }

        self.inventory_mapping = {
            "SKU": "product_retailer_sku",
            "TITLE": "product_name",
            "Current stock": "quantity_physical",
            "On order": "quantity_intransit",
        }
        self.inventory_hardcoded = {
            "reporting_period": "Weekly",
            "type": "by_warehouse_sku",
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] THG Weekly UK Report 2021-04-12',
                                'From': 'Tim Olson <Tim.Olson@thehutgroup.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Apr 12, 2021 at 4:32 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_THG Weekly UK Report 2021-04-12.xlsx'
                            }
        :param file_name: 'THG Weekly UK Report 2021-04-12.xlsx'
        :param sheet_name: 'Sheet1'
        """
        if "THG Weekly UK Report".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='sales',
                mapping_dict=self.sales_mapping,
                hardcoded_dict=self.sales_hardcoded
            )
        elif "Stock View Olaplex".lower() in file_name.lower() and sheet_name.lower() == 'Stock View'.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='inventory',
                mapping_dict=self.inventory_mapping,
                hardcoded_dict=self.inventory_hardcoded
            )
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] THG Weekly UK Report 2021-04-12',
                                'From': 'Tim Olson <Tim.Olson@thehutgroup.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Apr 12, 2021 at 4:32 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_THG Weekly UK Report 2021-04-12.xlsx'
                            }
        :param sheet: 'Sheet1'
        :param report_type: 'sales'
        :param mapping_dict: sales_mapping
        :param hardcoded_dict: sales_hardcoded
        """
        try:
            if report_type == 'sales':
                df = pd.read_excel(report_dict['local_path'], sheet)
                if 'Year' not in df.columns:
                    df = pd.read_excel(report_dict['local_path'],sheet, skiprows=1)
                df.rename(columns=mapping_dict, inplace=True)
                df["reporting_period_start"] = df.apply(lambda x: get_iso_first_date(x.Year, x.iso_week), axis=1)
                df["reporting_period_end"] = df.apply(lambda x: get_iso_last_date(x.Year, x.iso_week), axis=1)
            elif report_type == 'inventory':
                df = get_inventory_dataframe(report_dict['local_path'])
                df["effective_date"] = get_effective_date_thg(report_dict['local_path'])

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
