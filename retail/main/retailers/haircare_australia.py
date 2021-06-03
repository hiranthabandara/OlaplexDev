import pandas as pd
from datetime import datetime
from retail.main.retailer import Retailer
from retail.main.utils import get_standard_date


class HaircareAustralia(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_mapping = {
            "Period Start": "reporting_period_start",
            "Period End": "reporting_period_end",
            "Region": "sell_through_channel",
            "Product Code": "product_retailer_sku",
            "Product Name": "product_name",
            "Quantity #": "total_quantity",
            "USD Value After Discount": "total_value"
        }
        self.aus_hardcoded = {
            "type": "by_channel_sku",
            'reporting_period': 'Monthly',
            "currency": "USD",
            "country": "AU"
        }

        self.nz_hardcoded = {
            "type": "by_channel_sku",
            'reporting_period': 'Monthly',
            "currency": "USD",
            "country": "NZ"
        }
        self.inventory_mapping = {
            "Effective Date": "effective_date",
            "Warehouse Code": "plant_Id",
            "Warehouse Name": "plant_name",
            "Product Code": "product_retailer_sku",
            "Product Name": "product_name",
            " Total (Stock on Hand Qty #)": "quantity_warehouse",
            " Total (Stock in Transit Qty #)": "quantity_intransit",
            " Total (Reporting SOH Value $)": "value_warehouse",
        }
        self.inventory_hardcoded = {
            "type": "by_warehouse_sku",
            'reporting_period': 'Monthly'
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] RE: Olaplex Data Warehouse: Haircare Australia Results --
                                            Input for Nov 2020',
                                'From': 'Steven Ihms <steven.lhms@haircareaust.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_HaircareAust_Sales.xlsx'
                            }
        :param file_name: 'HaircareAust_Sales.xlsx'
        :param sheet_name: 'HCA'
        """
        if "HaircareAust_Sales".lower() in file_name.lower():
            if sheet_name.lower() == 'HCA'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sales_mapping,
                    hardcoded_values=self.aus_hardcoded
                )
            elif sheet_name.lower() == 'HCNZ'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sales_mapping,
                    hardcoded_values=self.nz_hardcoded
                )
            else:
                pass
        elif "HaircareAust_Inventory".lower() in file_name.lower():
            if sheet_name.lower() == 'Inventory'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='inventory',
                    mapping_dict=self.inventory_mapping,
                    hardcoded_values=self.inventory_hardcoded
                )
            else:
                pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_values=None):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] RE: Olaplex Data Warehouse: Haircare Australia Results --
                                            Input for Nov 2020',
                                'From': 'Steven Ihms <steven.lhms@haircareaust.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_HaircareAust_Sales.xlsx'
                            }
        :param sheet: 'HCA'
        :param report_type: 'sales'
        :param mapping_dict: sales_mapping
        :param hardcoded_values: aus_hardcoded
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
                df["reporting_period_start"] = df["reporting_period_start"].apply(get_standard_date)
                df["reporting_period_end"] = df["reporting_period_end"].apply(get_standard_date)

                df['note'] = [f"Sub Region Code = {x}; Sub Region Name = {y}"
                              for x, y in zip(df_original['Sub Region Code'], df_original['Sub Region Name'])]

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
