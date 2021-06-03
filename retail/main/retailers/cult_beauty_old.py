import pandas as pd
import sys
sys.path.append('..')
from retail.main.retailer import Retailer


class CultBeautyOLD(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_mapping = {
            "reporting_period_start": "reporting_period_start",
            "reporting_period_end": "reporting_period_end",
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

    def map_destination(self, report_dict, file_name, sheet_name=None):
        # The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        if "Cult Beauty Retail Sales".lower() in file_name.lower():
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
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet, skiprows=[0], skipfooter=1)
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]

            df.rename(columns=mapping_dict, inplace=True)
            df["reporting_period_start"] = df["reporting_period_start"].dt.strftime("%Y-%m-%d")
            df["reporting_period_end"] = df["reporting_period_end"].dt.strftime("%Y-%m-%d")

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
