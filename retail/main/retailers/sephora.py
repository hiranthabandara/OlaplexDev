import os
import logging
import datetime
import pandas as pd
from aws import Redshift
from datetime import datetime
from datetime import timedelta
from retail.main.config import app_config
from retail.main.retailer import Retailer
from retail.main.utils import generate_uuid, generate_report_id, generate_record_id


def get_ca_stores():
    """
    This method is specific to Sephora. This method returns all the Canada Store ID's in Sephora as a list.
    Data are retrieved from sephora_ca_stores_table in the schema
    :return ca_stores_list: []
    """
    rdsft = Redshift(**app_config.redshift_creds)
    ca_stores_sql = f"SELECT * FROM {app_config.redshift_sephora_ca_stores_table};"
    ca_stores_df = rdsft.read_sql(ca_stores_sql)
    ca_stores_list = [x.lower() for x in ca_stores_df['store_id']]
    return ca_stores_list


def get_country(location, ca_stores_list):
    """
    This method is specific to Sephora. Returns the country as CA if the store_id is present in the ca_stores_list.
    :param location: '1000'
    :param ca_stores_list: ['100','101']
    :return country: 'US'
    """
    if str(location).split(' - ')[0].lower() in ca_stores_list:
        country = 'CA'
    else:
        country = 'US'
    return country


def get_retailer_id(country):
    """
    This method is specific to Sephora. Returns the retailer_id according to the country.
    :param country: 'US'
    :return country: 'C050439 Sephora'
    """
    if country == 'CA':
        return 'C095719 Sephora Beauty Canada, Inc.'
    elif country == 'US':
        return 'C050439 Sephora'
    else:
        return None


def get_retailer_internal_id(country):
    """
    This method is specific to Sephora. Returns the retailer_internal_id according to the country.
    :param country: 'US'
    :return country: '1210192'
    """
    if country == 'CA':
        return '5077296'
    elif country == 'US':
        return '1210192'
    else:
        return None


def get_effective_date(file_name, sheet_name):
    """
    This method is specific to Sephora. Called for Sheet Inventory in file name Best Seller - Olaplex - Skincare.xlsx
    :param file_name: 'Best Seller - Olaplex - Skincare.xlsx'
    :param sheet_name: 'Inventory'
    :return : '2021-04=03'
    """
    cell_index = (3, 'A')
    week_end_str = get_excel_cell(file_name, sheet_name, row=cell_index[0], col=cell_index[1])

    # format: Week end date: Jun 15, 2019
    week_end_str = week_end_str.split(":")[1].strip()

    end_date = datetime.strptime(week_end_str, '%b %d, %Y')
    return end_date.strftime("%Y-%m-%d")


def get_excel_cell(file_path, sheet_name, row=1, col='A'):
    """
    This method is specific to Sephora. This method returns the value in a specific cell in the excel sheet.
    :param file_path: 'Best Seller - Olaplex - Skincare.xlsx'
    :param sheet_name: 'Inventory'
    :param row: '3'
    :param col: 'A'
    :return : 'Week end date: Jan 30, 2021'
    """
    if row < 1:
        row = 1
    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=col, header=None)
    return df.iloc[row - 1].values[0]


def get_weekly_range(file_name, sheet_name):
    """
    This method is specific to Sephora. Returns weekly range for file 'Best Seller - Olaplex - Skincare.xlsx'
    :param file_name: 'Best Seller - Olaplex - Skincare.xlsx'
    :param sheet_name: 'US'
    :return : '2021-04=03', '2021=04=10'
    """
    cell_index = (3, 'A')
    week_end_str = get_excel_cell(file_name, sheet_name, row=cell_index[0], col=cell_index[1])

    # format: Week end date: Jun 15, 2019
    week_end_str = week_end_str.split(":")[1].strip()

    end_date = datetime.strptime(week_end_str, '%b %d, %Y')
    start_date = end_date - timedelta(days=6)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_weekly_range2(file_name, sheet_name):
    """
    This method is specific to Sephora. Returns weekly range for file 'Olaplex Weekly Brand Sales by Store and SKU.xlsx'
    :param file_name: 'Olaplex Weekly Brand Sales by Store and SKU.xlsx'
    :param sheet_name: 'WEEKLY brand sales by store'
    :return : '2021-04=03', '2021=04=10'
    """
    cell_index = (2, 'B')
    week_end_str = get_excel_cell(file_name, sheet_name, row=cell_index[0], col=cell_index[1])
    # Format: Jun-27-2020
    end_date = datetime.strptime(str(week_end_str), '%b-%d-%Y')
    start_date = end_date - timedelta(days=6)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_channel(district_name):
    """
    This method is specific to Sephora. Returns sell_through_channel for
    file 'Olaplex Weekly Brand Sales by Store and SKU.xlsx'
    :param district_name: 'Olaplex Weekly Brand Sales by Store and SKU.xlsx'
    :return : 'online'
    """
    if district_name == 'Dotcom .CA':
        return 'online'
    else:
        return 'store'


class Sephora(Retailer):
    def __init__(self):
        super().__init__()

    def append_metadata(self, df, report_dict, report_type, sheet=None):
        """
        This method overrides the Retailer class append_metadata method because Sephora file have both Sephora CA and US
        data.
        :param df:
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                                'Timestamp': 1616101111,
                                'local_path': '\\_data\\Sephora\\1616101111_Best Seller - Olaplex.xlsx'
                            }
        :param  file_name: '1616101111_Best Seller - Olaplex.xlsx'
        :param report_type: 'Sales'
        :param sheet: 'US_1'
        """
        file_name = os.path.basename(report_dict['local_path'])

        df['retailer_id'] = df['country'].apply(get_retailer_id)
        df['retailer_name'] = [id.split(' ', maxsplit=1)[1] for id in df['retailer_id']]
        df['retailer_internal_id'] = df['country'].apply(get_retailer_internal_id)
        df['reporting_period'] = 'Weekly'

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
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                                'Timestamp': 1616101111,
                                'local_path': '\\_data\\Sephora\\1616101111_Best Seller - Olaplex.xlsx'
                            }
        :param  file_name: '1616101111_Best Seller - Olaplex.xlsx'
        :param sheet_name: 'US_1'
        """
        if "Best Seller - Olaplex".lower() in file_name.lower():
            if sheet_name in ('US_1', 'Canada_2', 'US', 'Canada'):
                self.parse_inventory_best_seller(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name
                )
                self.build_sales_by_sku(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name
                )
            else:
                logging.info(f"Skipping the sheet {sheet_name} from {file_name}")

        elif "weekly brand sales by store and sku" in file_name.lower():
            if sheet_name == 'WEEKLY brand sales by store' or sheet_name == 'WEEKLY brand sales by store_2':
                self.build_sales_by_loc_ca(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name)
            else:
                logging.info(f"Skipping the sheet {sheet_name} from {file_name}")
        elif "weekly sales by locations" in file_name.lower():
            if sheet_name == 'Page1' or sheet_name == 'Page1_1':
                self.build_sales_by_loc(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name)
            else:
                logging.info(f"Skipping the sheet {sheet_name} from {file_name}")
        else:
            logging.info(f"{file_name} doesn't match with search criteria. Skipping further processing")

    def parse_inventory_best_seller(self, report_dict, sheet):
        """
        This method is for file named 'Best Seller - Olaplex.xlsx' and sheet 'Inventory'
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                                'Timestamp': 1616101111,
                                'local_path': 'C:\\Users\\Dell\\Beacon Data\\OLAPLEX\\Olaplex-Retail-Dev\\_data\\
                                                Sephora\\1616101111_Best Seller - Olaplex.xlsx'
                            }
        :param sheet: 'Inventory'
        """
        try:
            header_index = 10
            country, cols = None, None

            if sheet == 'US' or sheet == 'US_1':
                logging.info("Building US inventory report")
                country = 'US'
                cols = {
                    'E': ('SKU', 'product_retailer_sku'),
                    'F': ('SKU Description', 'product_name'),
                    'L': ('Vendor MFG', 'product_sku'),
                    'BP': ('Total.6', 'quantity_warehouse'),
                    'BS': ('.COM OO', None),
                    'BY': ('.DC OO', None)
                }
                report_type = 'US_Inventory'

            elif sheet == 'Canada_2' or sheet == 'Canada':
                logging.info("Building CA inventory report")
                country = 'CA'
                cols = {
                    'E': ('SKU', 'product_retailer_sku'),
                    'F': ('SKU Description', 'product_name'),
                    'K': ('Vendor MFG', 'product_sku'),
                    'BO': ('Total.6', 'quantity_warehouse'),
                    'BR': ('.COM OO', None),
                    'BX': ('.DC OO', None)
                }
                report_type = 'CA_Inventory'

            else:
                return

            df = pd.read_excel(
                io=report_dict['local_path'],
                sheet_name=sheet,
                header=header_index,
                usecols=','.join(cols.keys()),
                index_col=None)

            df['quantity_intransit'] = df['.COM OO'] + df['DC OO']
            col_mapping = {v[0]: v[1] for k, v in cols.items() if v[1]}
            df = df.rename(columns=col_mapping)
            df.drop(columns=['.COM OO', 'DC OO'], inplace=True)

            df['country'] = country

            df['type'] = 'by_country_sku'
            df['effective_date'] = get_effective_date(
                file_name=report_dict['local_path'],
                sheet_name=sheet
            )

            df["report_type"] = report_type

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type='inventory',
                sheet=sheet
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
                sheet=sheet
            )

    def build_sales_by_sku(self, report_dict, sheet):
        """
        This method is for file named 'Best Seller - Olaplex.xlsx' and sheet 'Inventory'
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': '<SMART.Reports@sephora.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Tue, Feb 2, 2021 at 10:14 PM',
                                'Timestamp': 1616101111,
                                'local_path': '\\_data\\Sephora\\1616101111_Best Seller - Olaplex.xlsx'
                            }
        :param sheet: 'Inventory'
        """
        try:
            header_index = 10

            country, cols, report_type = None, None, None
            if sheet == 'US_1' or sheet == 'US':
                logging.info("Building US sales by SKU report")
                country = 'US'
                report_type = 'US_Sales_by_SKU'
                cols = {
                    'E': ('SKU', 'product_retailer_sku'),
                    'F': ('SKU Description', 'product_name'),
                    'L': ('Vendor MFG', 'product_sku'),
                    'Z': ('Total', 'total_value'),
                    'AE': ('Total.1', 'total_quantity'),
                }

            elif sheet == 'Canada_2' or sheet == 'Canada':
                logging.info("Building CA sales by SKU report")
                country = 'CA'
                report_type = 'CA_Sales_by_SKU'
                cols = {
                    'E': ('SKU', 'product_retailer_sku'),
                    'F': ('SKU Description', 'product_name'),
                    'K': ('Vendor MFG', 'product_sku'),
                    'Y': ('Total', 'total_value'),
                    'AD': ('Total.1', 'total_quantity'),
                }

            else:
                return

            df = pd.read_excel(io=report_dict['local_path'],
                               sheet_name=sheet,
                               header=header_index,
                               usecols=','.join(cols.keys()),
                               index_col=None)

            col_mapping = {v[0]: v[1] for k, v in cols.items() if v[1]}
            df = df.rename(columns=col_mapping)

            start_date, end_date = get_weekly_range(file_name=report_dict['local_path'], sheet_name=sheet)

            df['reporting_period_start'] = start_date
            df['reporting_period_end'] = end_date
            df['country'] = country
            df['type'] = 'by_country_sku'
            df["report_type"] = report_type

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type='sales',
                sheet=sheet
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
                sheet=sheet
            )

    def build_sales_by_loc_ca(self, report_dict, sheet):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': ' [EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': '<SMART.Reports@sephora.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Thu, Apr 1, 2021 at 4:28 PM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\Sephora\\1616101111_Olaplex Weekly Brand Sales by Store and SKU.xlsx'
                            }
        :param sheet: 'Weekly brand sales by store'
        """
        try:
            logging.info("Building CA sales by location report")
            header_index = 7
            country = 'CA'
            report_type = 'CA_Sales_by_SKU'
            cols = {
                'A': ('District Name', 'region'),
                'B': ('Location', 'store_id'),
                'C': ('Location Name', 'store_name'),
                'D': ('$Sales TY', 'total_value'),
            }
            col_mapping = {v[0]: v[1] for k, v in cols.items() if v[1]}

            df = pd.read_excel(
                io=report_dict['local_path'],
                sheet_name=sheet,
                header=header_index,
                usecols=','.join(cols.keys()),
                index_col=None)

            df = df.rename(columns=col_mapping)

            # Get rid of the last row
            df = df[:-1]

            start_date, end_date = get_weekly_range2(file_name=report_dict['local_path'], sheet_name=sheet)

            df['reporting_period_start'] = start_date
            df['reporting_period_end'] = end_date
            df['country'] = country
            df['sell_through_channel'] = df['region'].apply(get_channel)
            df['type'] = 'by_location_channel'
            df["report_type"] = report_type

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type='sales',
                sheet=sheet
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
                sheet=sheet
            )

    def build_sales_by_loc(self, report_dict, sheet):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': ' [EXTERNAL SENDER] SEPHORA Reports: 533067 - Olaplex - Hair',
                                'From': '<SMART.Reports@sephora.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Thu, Apr 1, 2021 at 4:28 PM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\Sephora\\1616101111_Olaplex - Sales by Location.xlsx'
                            }
        :param sheet: 'Sheet1'
        """
        try:
            logging.info("Building US/CA sales by location report")
            header_index = 6
            country = 'US'
            report_type = 'US_CA_Sales_by_SKU'

            cols = {
                'B': ('Week End Date (Saturday)', 'reporting_period_end'),
                'C': ('Location', None),
                'D': ('Week End Sales Net $', 'total_value')
            }

            col_mapping = {v[0]: v[1] for k, v in cols.items() if v[1]}

            df = pd.read_excel(
                io=report_dict['local_path'],
                sheet_name=sheet,
                skiprows=6,
                usecols=','.join(cols.keys()),
                index_col=None)

            df = df[1:-2]
            df = df.reset_index(drop=True)

            # skip first row and last two rows
            df = df.rename(columns=col_mapping)

            # Generate extra fields
            df['reporting_period_end'] = df['reporting_period_end']
            df['reporting_period_start'] = df['reporting_period_end'] - timedelta(days=6)

            df['reporting_period_start'] = df['reporting_period_start'].dt.strftime('%Y-%m-%d')
            df['reporting_period_end'] = df['reporting_period_end'].dt.strftime('%Y-%m-%d')

            ca_stores_list = get_ca_stores()
            df['country'] = [get_country(location, ca_stores_list) for location in df['Location']]
            df = df[df.country != 'CA']
            df['type'] = 'by_location'
            df['store_id'] = [x.split('-')[0] for x in df['Location']]
            df['store_name'] = [x.split('-')[1] for x in df['Location']]
            df.drop(columns=['Location'], inplace=True)
            df["report_type"] = report_type

            self.append_metadata(
                df=df,
                report_dict=report_dict,
                report_type='sales',
                sheet=sheet
            )
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict,
                sheet=sheet
            )
