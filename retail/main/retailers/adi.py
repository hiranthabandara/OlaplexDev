import os
import pandas as pd
from datetime import datetime
from retail.main.retailer import Retailer


def get_effective_date(raw_date, file_name):
    """
    This function is specific to ADI inventory. Which is in case of date format difference
    Return date in the file column or the file_name. file_name date prioritized
    :param raw_date: '04/05/2021'
    :param file_name: 'ADI Inventory Report 2021-04-30.xlsx'
    :return: '2021-04-30'
    """
    file_date = str(file_name.split('.')[0].strip()).split(' ')[-1].strip()
    e_date = None
    try:
        e_date = datetime.strptime(raw_date, "%Y-%m-%d")
    except Exception as e:
        e_date = datetime.strptime(raw_date, "%d/%m/%Y")
    return_date = datetime.strftime(e_date, "%Y-%m-%d")
    if return_date != file_date:
        return_date = file_date
    return return_date


class ADI(Retailer):
    def __init__(self):
        super().__init__()

        self.sales_mapping = {
            "reporting_period_start": "reporting_period_start",
            "reporting_period_end": "reporting_period_end",
            "retailer_name": "retailer_name",
            "sell_through_channel": "sell_through_channel",
            "store_id": "store_id",
            "store_name": "store_name",
            "region": "region",
            "country": "country",
            "state": "state",
            "product_sku": "product_retailer_sku",
            "olaplex_product_id": "product_sku",
            "product_name": "product_name",
            "product_size": "product_size",
            "currency": "currency",
            "total_quantity": "total_quantity",
            "total_value": "total_value",
            "return_quantity": "return_quantity",
            "return_value": "return_value",
            "free_replacements_quantity": "free_replacements_quantity",
            "free_replacements_value": "free_replacements_value",
            "Tags": "tags",
        }

        self.inventory_mapping = {
            "effective_date": "effective_date",
            "retailer_name": "retailer_name",
            "olaplex_product_id": "product_sku",
            "product_name": "product_name",
            "currency": "currency",
            "quantity_warehouse": "quantity_warehouse",
            "quantity_physical": "quantity_physical",
            "value_warehouse": "value_warehouse",
            "value_physical": "value_physical",
        }

        # Hardcoded values
        self.hardcoded_values = {
            "reporting_period": "Monthly",
            'type': 'by_sku'
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        Passes the type of the file according to the file name and sheet name
        :param report_dict: {
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'Kasun Sampath <kasun.sampath@beacondata.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': 'ADI Reports',
                                'Timestamp': 1616086825,
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'local_path': '\\_data\\ADI\\1616086825_ADI Inventory Report 2021-01-31.xlsx'
                            }
        :param file_name: 1616086825_ADI Inventory Report 2021-01-31.xlsx
        :param sheet_name: None
        """
        if "ADI Sales Report".lower() in file_name.lower():
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       sheet=sheet_name,
                                       report_type='sales',
                                       mapping_dict=self.sales_mapping,
                                       hardcoded_values=self.hardcoded_values)

        elif "ADI Inventory Report".lower() in file_name.lower():
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       sheet=sheet_name,
                                       report_type='inventory',
                                       mapping_dict=self.inventory_mapping,
                                       hardcoded_values=self.hardcoded_values)
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_values=None):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'Kasun Sampath <kasun.sampath@beacondata.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': 'ADI Reports',
                                'Timestamp': 1616086825,
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'local_path': '\\_data\\ADI\\1616086825_ADI Inventory Report 2021-01-31.xlsx'
                            }
        :param sheet: 'Inventory'
        :param report_type: 'inventory'
        :param mapping_dict: {inventory_mapping}
        :param hardcoded_values: {hardcoded_values}
        """
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet)
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]
            if report_type == 'sales':
                df.rename(columns=mapping_dict, inplace=True)
                df["reporting_period_start"] = df["reporting_period_start"].dt.strftime("%Y-%m-%d")
                df["reporting_period_end"] = df["reporting_period_end"].dt.strftime("%Y-%m-%d")

            else:
                file_name = os.path.basename(report_dict['local_path'])
                df.rename(columns=mapping_dict, inplace=True)
                try:
                    df["effective_date"] = df["effective_date"].dt.strftime("%Y-%m-%d")
                except:
                    df["effective_date"] = [get_effective_date(raw_date, file_name) for raw_date in
                                            df['effective_date']]

            # hardcoded fields
            if hardcoded_values:
                for key in hardcoded_values:
                    df[key] = hardcoded_values[key]

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
