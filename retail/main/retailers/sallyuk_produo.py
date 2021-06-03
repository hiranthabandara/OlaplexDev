import os
import pandas as pd
from datetime import datetime
from retail.main.retailer import Retailer
from retail.main.utils import generate_record_id, generate_report_id, generate_uuid
from retail.main.config import app_config


def retailer_name_to_id(retailer_name):
    """
    This function returns the retailer_id specific to the retailer name
    :param retailer_name: 'Sally Salon Services'
    :return: 'C128878 Sally Salon Services'
    """
    if retailer_name.lower() == 'Sally Salon Services'.lower():
        return 'C128878 Sally Salon Services'
    elif retailer_name.lower() == 'Pro-Duo NV/SA'.lower():
        return 'C155330 Pro-Duo NV/SA'


def retailer_name_to_internal_id(retailer_name):
    """
    This function returns the retailer_internal_id specific to the retailer name
    :param retailer_name: 'Sally Salon Services'
    :return: '6598548'
    """
    if retailer_name.lower() == 'Sally Salon Services'.lower():
        return '6598548'
    elif retailer_name.lower() == 'Pro-Duo NV/SA'.lower():
        return '7101588'


class SallyUKProDuo(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
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
        self.sales_hardcoded = {
            "type": "by_country_channel_sku",
            "reporting_period": "Monthly",
        }

        self.dtypes_sales = {
            "retailer_name": str,
            "sell_through_channel": str,
            "store_id": str,
            "store_name": str,
            "region": str,
            "country": str,
            "state": str,
            "product_sku": str,
            "olaplex_product_id": str,
            "product_name": str,
            "product_size": str,
            "currency": str,
            "Tags": str
        }

        self.inventory_mapping = {
            "effective_date": "effective_date",
            "retailer_name": "retailer_name",
            "warehouse_name": "plant_name",
            "olaplex_product_id": "product_sku",
            "product_sku": "product_retailer_sku",
            "product_name": "product_name",
            "product_size": "product_size",
            "currency": "currency",
            "quantity_warehouse": "quantity_warehouse",
            "quantity_physical": "quantity_physical",
            "quantity_intransit": "quantity_intransit",
            "value_warehouse": "value_warehouse",
            "value_physical": "value_physical",
            "value_intransit": "value_intransit",
            "Tags": "tags",
        }
        self.inventory_hardcoded = {
            "reporting_period": "Monthly",
            "type": "by_warehouse_sku"
        }

        self.dtypes_inventory = {
            "retailer_name": str,
            "warehouse_name": str,
            "olaplex_product_id": str,
            "product_sku": str,
            "product_name": str,
            "product_size": str,
            "currency": str,
            "Tags": str
        }

    def append_metadata(self, df, report_dict, report_type, sheet=None):
        """
        This method override the parent class method because there are data for two retailers in this file.
        This method append metadata to the file.
        :param df: DataFrame
        :param report_dict:{
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'De Vilder Tom <Tom.De.Vilder@pro-duo.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Sally UK - ProDuo Monthly Report - jan 2021',
                                'Timestamp': 1616086825,
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'local_path': '\\_data\\SallyUKProDuo\\1616086825_Sally UK ProDuo Monthly
                                                Report 2021-03-04.xlsx'
                            }
        :param report_type: 'sales'
        :param sheet: 'Sales'
        """
        file_name = os.path.basename(report_dict['local_path'])

        uuid_list = []
        report_id_list = []

        for index in range(len(df)):
            md5_text = ''.join([val for val in df.iloc[index].astype(str).values])
            uuid_list.append(generate_uuid(md5_text))

            report_id_list.append(
                generate_report_id(
                    file_name=file_name,  # filename without Timestamp
                    reporting_period=df.reporting_period.iloc[index],
                    end_date=df.reporting_period_end.iloc[index] if report_type == 'sales' else df.effective_date.iloc[
                        index],
                    retailer_id=df.retailer_id.iloc[index],
                    num_records=len(df),
                    sheet_name=sheet
                )
            )

        df["uuid"] = uuid_list
        df['report_id'] = report_id_list

        df['record_id'] = [generate_record_id(report_id=report_id, row_number=index)
                           for index, report_id in enumerate(df.report_id)]

        df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["file_name"] = file_name
        df["sheet_name"] = sheet
        df["number_of_records_in_sheet"] = len(df)
        df["sender_email_address"] = report_dict["From"]
        df["email_received_date"] = report_dict["Date"]
        df["email_subject"] = report_dict["Subject"]

        # Updating the temp dictionary with metadata
        report_dict["output_df"] = df
        report_dict["report_type"] = report_type

        if sheet:
            report_dict["sheet_name"] = sheet

        s3_path = f"{app_config.s3_unprocessed_dir}/{report_type}/{self.name}/{report_dict['ID']}"
        f_name = f"{file_name}.json"
        if sheet:
            f_name = f"{sheet}_{f_name}"
        report_dict["s3_location"] = f"s3://{app_config.s3_bucket_name}/{s3_path}/{f_name}"

        self.status.append(report_dict)

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'De Vilder Tom <Tom.De.Vilder@pro-duo.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Sally UK - ProDuo Monthly Report - jan 2021',
                                'Timestamp': 1616086825,
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'local_path': '\\_data\\SallyUKProDuo\\1616086825_Sally UK ProDuo Monthly
                                                Report 2021-03-04.xlsx'
                            }
        :param file_name: 'Sally UK ProDuo Monthly Report 2021-03-04.xlsx'
        :param sheet_name: 'Sales'
        """
        if "Sally UK ProDuo Monthly Report".lower() in file_name.lower():
            if sheet_name.lower() == 'Sales'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sales_mapping,
                    hardcoded_dict=self.sales_hardcoded,
                    dtypes=self.dtypes_sales
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
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict, dtypes):
        """
        Process the report_dict dictionary according to the report type
        :param report_dict: {
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'De Vilder Tom <Tom.De.Vilder@pro-duo.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Sally UK - ProDuo Monthly Report - jan 2021',
                                'Timestamp': 1616086825,
                                'To': 'reports@olaplex.com <reports@olaplex.com>',
                                'local_path': '\\_data\\SallyUKProDuo\\1616086825_Sally UK ProDuo Monthly
                                                Report 2021-03-04.xlsx'
                            }
        :param sheet: 'Sales'
        :param report_type: 'sales'
        :param mapping_dict:
        :param hardcoded_dict:
        """
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet, dtype=dtypes)
            df = pd.DataFrame()
            for col in df_original.columns:
                for k in mapping_dict.keys():
                    if col.lower() == k.lower():
                        df[k] = df_original[col]

            df['retailer_id'] = df['retailer_name'].apply(retailer_name_to_id)
            df['retailer_internal_id'] = df['retailer_name'].apply(retailer_name_to_internal_id)

            if report_type == 'sales':
                df.rename(columns=mapping_dict, inplace=True)
                df["reporting_period_start"] = df["reporting_period_start"].dt.strftime("%Y-%m-%d")
                df["reporting_period_end"] = df["reporting_period_end"].dt.strftime("%Y-%m-%d")

            else:
                df.rename(columns=mapping_dict, inplace=True)
                df["effective_date"] = df["effective_date"].dt.strftime("%Y-%m-%d")

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
