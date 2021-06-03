import pandas as pd
from datetime import datetime
from retail.main.retailer import Retailer


class AstonAndFincher(Retailer):
    def __init__(self):
        """
        This AstonAndFincher retailer is a sub class of Standard Retailer
        """
        super().__init__()

        # redshift col names are in right side
        self.sales_mapping = {
            "reporting_period_start": "reporting_period_start",
            "reporting_period_end": "reporting_period_end",
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
            "total_quantity": "total_quantity",
            "total_value": "total_value",
            "return_quantity": "return_quantity",
            "return_value": "return_value",
            "free_replacements_quantity": "free_replacements_quantity",
            "free_replacements_value": "free_replacements_value",
            "Tags": "tags",
        }
        # Hardcoded values
        self.sales_hardcoded_values = {
            "reporting_period": "Monthly",
            "Type": "by_location_sku",
            "currency": "GBP"
        }

        self.inventory_mapping = {
            "effective_date": "effective_date",
            "warehouse_name": "plant_name",
            "olaplex_product_id": "product_sku",
            "product_sku": "product_retailer_sku",
            "product_name": "product_name",
            "product_size": "product_size",
            "quantity_warehouse": "quantity_warehouse",
            "quantity_physical": "quantity_physical",
            "quantity_intransit": "quantity_intransit",
            "value_warehouse": "value_warehouse",
            "value_physical": "value_physical",
            "value_intransit": "value_intransit",
            "Tags": "tags"
        }

        self.inventory_hardcoded_values = {
            "reporting_period": "Monthly",
            "Type": "by_sku",
            "Currency": "GBP"
        }

        self.dtypes_sales = {
            "store_id": str,
            "product_sku": str,
            "olaplex_product_id": str,
            "product_size": str,
            "region": str,
            "country": str,
            "state": str,
            "Tags": str
        }

        self.dtypes_inventory = {
            'warehouse_name': str,
            'product_sku': str
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map function is to figure out which method to call based on file_name and sheet_name
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'Aston & Fincher - Feb 2021',
                                'From': 'Anita Ranu <anita.ranu@afteam.co.uk>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Tue, May 4, 2021 at 7:33 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\AstonAndFincher\\1616101111_Aston  Fincher Monthly
                                                Sales Report - 2021-02-28.xlsx'
                            }
        :param file_name: '_data\\AstonAndFincher\\1616101111_Aston  Fincher Monthly Sales Report - 2021-02-28.xlsx'
        :param sheet_name: 'Sales'
        """
        # The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        if "Aston Fincher Sales".lower() in file_name.lower() or sheet_name.lower() == "Sales".lower():
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       sheet=sheet_name,
                                       report_type='sales',
                                       mapping_dict=self.sales_mapping,
                                       hardcoded_values=self.sales_hardcoded_values,
                                       data_types=self.dtypes_sales)

        elif "Aston Fincher Inventory".lower() in file_name.lower() or sheet_name.lower() == "Inventory".lower():
            self.parse_sales_inventory(report_dict=report_dict.copy(),
                                       sheet=sheet_name,
                                       report_type='inventory',
                                       mapping_dict=self.inventory_mapping,
                                       hardcoded_values=self.inventory_hardcoded_values,
                                       data_types=self.dtypes_inventory)
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, data_types, hardcoded_values=None):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'Aston & Fincher - Feb 2021',
                                'From': 'Anita Ranu <anita.ranu@afteam.co.uk>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Tue, May 4, 2021 at 7:33 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\AstonAndFincher\\1616101111_Aston  Fincher Monthly
                                                Sales Report - 2021-02-28.xlsx'
                            }
        :param sheet: 'Sales'
        :param report_type: 'sales'
        :param mapping_dict: sales_mapping
        :param hardcoded_values: sales_hardcoded_values
        :param data_types: dtypes_sales
        """
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet, dtype=data_types)
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
                df.rename(columns=mapping_dict, inplace=True)
                try:
                    df["effective_date"] = df["effective_date"].dt.strftime("%Y-%m-%d")
                except Exception as e:
                    df["effective_date"] = [datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m-%d')
                                            for i in df['effective_date']]

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
