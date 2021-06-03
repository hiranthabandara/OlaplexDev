import pandas as pd
from retail.main.retailer import Retailer


def get_reporting_period(file_name):
    """
    This method returns the reporting week according to the first row value in file
    :param file_name: 'Merchandise.xlsx'
    :return report_week: 'Week 14, 2021'
    """
    df_temp = pd.read_excel(file_name, nrows=2, usecols=[1])
    report_week = str(df_temp.iloc[1]).split('|')[1].strip()
    return report_week


class JCPenney(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_location_mapping = {
            "Location ": "store_id",
            "Description ": "store_name",
            "Region": "region",
            "Wk(s) Net Sls U": "total_quantity",
            "Wk(s) Net Sls R": "total_value"
        }
        self.sales_location_hardcoded = {
            "type": "by_location",
            'reporting_period': 'Weekly',
            "sell_through_channel": "Store",
            "currency": "USD",
        }
        self.inventory_location_mapping = {
            "Location ": "plant_Id",
            "Description ": "plant_name",
            "Whse EOP U": "quantity_warehouse",
            "Phys EOP U": "quantity_physical",
            "InTran U": "quantity_intransit",
            "Whse EOP C": "value_warehouse",
            "Phys EOP C": "value_physical",
            "InTran C": "value_intransit"
        }
        self.inventory_location_hardcoded = {
            "reporting_period": "Weekly",
            "type": "by_location",
            "currency": "USD"
        }
        self.sales_merchandise_mapping = {
            "Product ": "product_retailer_sku",
            "Supp Style #": "product_sku",
            "Description ": "product_name",
            "Wk(s) Net Sls U": "total_quantity",
            "Wk(s) Net Sls R": "total_value"
        }
        self.sales_merchandise_hardcoded = {
            "reporting_period": "Weekly",
            "currency": "USD",
            "type": "by_sku"
        }
        self.inventory_merchandise_mapping = {
            "Description ": "product_name",
            "Product ": "product_retailer_sku",
            "Whse EOP U": "quantity_warehouse",
            "Phys EOP U": "quantity_physical",
            "InTran U": "quantity_intransit",
            "Whse EOP C": "value_warehouse",
            "Phys EOP C": "value_physical",
            "InTran C": "value_intransit"
        }
        self.inventory_merchandise_hardcoded = {
            "reporting_period": "Weekly",
            "type": "by_sku",
            "currency": "USD"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Baldacci Feb 2021',
                                'From': '<data@olaplex.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Tue, May 11, 2021 at 3:35 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\JCPenney\\1616101111_Merchandise.xlsx'
                            }
        :param file_name: 'Merchandise.xlsx'
        :param sheet_name: 'Sheet1'
        """
        if "Merchandise".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='sales',
                mapping_dict=self.sales_merchandise_mapping,
                hardcoded_dict=self.sales_merchandise_hardcoded
            )
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='inventory',
                mapping_dict=self.inventory_merchandise_mapping,
                hardcoded_dict=self.inventory_merchandise_hardcoded
            )
        elif "Location".lower() in file_name.lower():
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='inventory',
                mapping_dict=self.inventory_location_mapping,
                hardcoded_dict=self.inventory_location_hardcoded
            )
            self.parse_sales_inventory(
                report_dict=report_dict.copy(),
                sheet=sheet_name,
                report_type='sales',
                mapping_dict=self.sales_location_mapping,
                hardcoded_dict=self.sales_location_hardcoded
            )

        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Baldacci Feb 2021',
                                'From': '<data@olaplex.com>',
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'Date': 'Tue, May 11, 2021 at 3:35 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\JCPenney\\1616101111_Merchandise.xlsx'
                            }
        :param sheet: 'Sheet1'
        :param report_type: 'sales'
        :param mapping_dict: sales_merchandise_mapping
        :param hardcoded_dict: sales_merchandise_hardcoded
        """
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet, skiprows=[0, 1, 2, 3, 4, 6])
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]

            report_week = get_reporting_period(report_dict['local_path'])

            if report_type == 'sales':
                df.rename(columns=mapping_dict, inplace=True)
                df["reporting_period_start"] = report_week
                df["reporting_period_end"] = report_week
                df_obj = df.select_dtypes(['object'])
                df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

            else:
                df.rename(columns=mapping_dict, inplace=True)
                df["effective_date"] = report_week
                if 'plant_name' in df.columns:
                    df["plant_name"] = [x.strip() for x in df.plant_name]

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
