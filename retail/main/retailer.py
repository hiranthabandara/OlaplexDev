import os
import glob
import sys
import logging
import traceback
import pandas as pd
from datetime import datetime
from config import app_config
from abc import ABC, abstractmethod
from airflow import AirflowException
from sql import sales_table_ddl
from sql import inventory_table_ddl
from sql import load_retail_sales_sql
from sql import load_retail_inventory_sql
from awscli.clidriver import create_clidriver
from utils import get_retailer_info, generate_report_id, generate_record_id, generate_uuid

sys.path.append(app_config.project_home)
from emails import Gmail
from aws import Redshift, S3


class Retailer(ABC):

    def __init__(self):
        """
        This is the Super Class of Every Retailer when creating a object it clears the files in the folder
        retailer information loaded from a CSV file
        """
        self.name = self.__class__.__name__  # name of the class
        retailer_info = get_retailer_info(self.name)
        if retailer_info:
            self.retailer_internal_id = retailer_info['retailer_internal_id']
            self.retailer_id = retailer_info["retailer_id"]
            self.email_label = retailer_info["email_label"]
            self.file_extensions = retailer_info["file_extensions"]
        else:
            raise AirflowException(f'Retailer not found {self.name}')

        self.download_path = os.path.join(app_config.download_path, self.name)
        self._clear_cache()
        self.status = []

    def _clear_cache(self):
        """
        Clears the directory of S3 and it's contents
        """

        if os.path.exists(self.download_path):
            logging.info(
                f"The {self.download_path} directory already exists. Clearing its contents ..")
            files = glob.glob(f"{self.download_path}/*")
            for f in files:
                os.remove(f)
                logging.info(f"Removed file {f}")

    def _extract_email_reports(self, email_label, file_extnsions, mark_seen):
        """
        Access to email account and read unread mails and download attachments to download_path
        Uses credentials.json to log to email. Download attachments in each mail to _data/ADI/
        :param email_label: 'Retail_Reports-ADI'
        :param file_extnsions:
        :param mark_seen: True
        :return:
            list of dictionaries:
                Following is a snapshot of dictionary in reports list
                each dictionary item has the following format
            {
                'ID': '1784721fdb3cad7d',
                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                'Subject': 'ADI Reports',
                'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                'Timestamp': 1616101111,
                'local_path': 'C:\\Users\\Dell\\Beacon Data\\OLAPLEX\\Olaplex-Retail-Dev\\_data\\ADI\\1616101111_ADI Inventory Report 2020-12-31.xlsx'
            }
        """
        email_config = app_config.reports_email_config.copy()
        email_config['download_path'] = self.download_path
        gmail = Gmail(**email_config)
        return gmail.get_attachment_with_metadata(search_label=email_label,
                                                  extensions=self.file_extensions,
                                                  mark_as_seen=mark_seen)

    # This method can be transfered to retailer.py
    def parse_reports(self, mark_as_seen=True):
        """
        Use extract_email_reports to reports[] variable
        to each item in reports use map_file in sub class
        :param mark_as_seen: True
        """
        reports = self._extract_email_reports(email_label=self.email_label,
                                              file_extnsions=self.file_extensions,
                                              mark_seen=mark_as_seen)
        for report_dict in reports:
            self._map_file(report_dict)

    def upload_to_s3(self):
        """
        Uploads the file to S3 bucket after clearing the contents in the folder
        """
        s3 = S3()
        if not self.status:
            logging.info("No matching data Found! Skipping file upload to S3")
            return

        raw_files = set()
        for item in self.status:
            raw_files.add((item['local_path'], item['ID']))

        logging.info("Uploading raw files to S3 ")

        for raw_file in raw_files:
            logging.info(f"Uploading {raw_file[0]} to S3 ")
            s3_path = f"{app_config.s3_raw_dir}/{self.name}/{raw_file[1]}/{os.path.basename(raw_file[0])}"
            s3.upload_file(raw_file[0], app_config.s3_bucket_name, s3_path)
        logging.info("Uploading processed files to S3 ")

        for item in self.status:
            if 'sheet_name' in item.keys():
                logging.info(
                    f"Uploading output of sheet {item['sheet_name']} of file {os.path.basename(item['local_path'])} ")
            else:
                logging.info(f"Uploading output of file {os.path.basename(item['local_path'])} ")

            s3.upload_dataframe(df=item['output_df'],
                                bucket_name=app_config.s3_bucket_name,
                                filename=item['s3_location'].replace(
                                    f"s3://{app_config.s3_bucket_name}/", ""),
                                file_format='json',
                                compress=False,
                                lowercase_headers=True)

    @classmethod
    def create_tables(cls, type='staging'):
        """
        Create staging tables for sales and inventory in redshift database
        :param type: 'staging'
        """
        sales_sql = ''
        inventory_sql = ''
        if type == 'final':
            sales_table = app_config.redshift_final_sales_table
            invent_table = app_config.redshift_final_inventory_table
            if app_config.drop_recreate_final_table:
                sales_sql += f"DROP TABLE IF EXISTS {sales_table};"
                inventory_sql += f"DROP TABLE IF EXISTS {invent_table};"
        else:
            sales_table = app_config.redshift_stg_sales_table
            invent_table = app_config.redshift_stg_inventory_table
            if app_config.drop_recreate_stg_table:
                sales_sql += f"DROP TABLE IF EXISTS {sales_table};"
                inventory_sql += f"DROP TABLE IF EXISTS {invent_table};"

        sales_sql += sales_table_ddl.format(SALES_TABLE=sales_table)
        inventory_sql += inventory_table_ddl.format(INVENTORY_TABLE=invent_table)

        rdsft = Redshift(**app_config.redshift_creds)
        logging.info(f"Creating {type} table {sales_table} (if it doesn't exists)")
        success = rdsft.run_sql_command(sql_command=sales_sql, close_on_return=False)
        if not success:
            raise AirflowException(f'Failed creating {type} table {sales_table}')

        logging.info(f"Creating {type} table {invent_table} (if it doesn't exists)")
        success = rdsft.run_sql_command(sql_command=inventory_sql, close_on_return=True)
        if not success:
            raise AirflowException(f'Failed creating {type} table {invent_table}')

    @classmethod
    def load_data(cls, s3_location, redshift_table):
        """
        Load data to tables in Redshift database from S3 bucket
        :param s3_location: 's3://olaplex-retail-data-test/ADI/json/17846482fb05eabc/
                                Inventory_1616086825_ADI Inventory Report 2021-01-31.json'
        :param redshift_table: 'dev_retail_data.tmp_sales_adi'
        :return sql_success: True
        """
        rdsft = Redshift(**app_config.redshift_creds)
        load_sql = f"""
            BEGIN;
            TRUNCATE TABLE {redshift_table};
            COPY {redshift_table} 
            FROM '{s3_location}'
            IAM_ROLE '{app_config.redshift_iam_role}' 
            TRUNCATECOLUMNS            
            json 'auto'; 
            COMMIT;"""
        logging.info(f"Running command: {load_sql}")
        sql_success = rdsft.run_sql_command(load_sql)
        if sql_success:
            logging.info(f"Successfully loaded data to {redshift_table} from {s3_location}")
        else:
            raise AirflowException(f"Error loading data to {redshift_table} from {s3_location}")

        return sql_success

    @classmethod
    def load_to_staging_tables(cls):
        """
        Loading data to staging tables if there are files present in the unprocessed sales/inventory folders
        """
        s3_bucket = S3()
        Retailer.create_tables(type='staging')
        sales_s3_loc = f"s3://{app_config.s3_bucket_name}/{app_config.s3_unprocessed_dir}/sales/"
        inventory_s3_loc = f"s3://{app_config.s3_bucket_name}/{app_config.s3_unprocessed_dir}/inventory/"

        if s3_bucket.is_folder_present(bucket_name=app_config.s3_bucket_name,
                                       prefix=f'{app_config.s3_unprocessed_dir}/sales'):
            logging.info(f"Loading sales data from {sales_s3_loc} to sales staging table")
            Retailer.load_data(sales_s3_loc, app_config.redshift_stg_sales_table)

        else:
            logging.info("No unprocessed sales data found!")

        if s3_bucket.is_folder_present(bucket_name=app_config.s3_bucket_name,
                                       prefix=f'{app_config.s3_unprocessed_dir}/inventory'):
            logging.info(f"Loading inventory data from {inventory_s3_loc} to inventory staging table")
            Retailer.load_data(inventory_s3_loc, app_config.redshift_stg_inventory_table)

        else:
            logging.info("No unprocessed inventory data found!")

    def _map_file(self, report_dict):
        """
        This function reads the sheet
        :param report_dict:
                            {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID': '<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': 'ADI Reports',
                                'From': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'Date': 'Fri, 19 Mar 2021 02:28:31 +0530',
                                'Timestamp': 1616101111,
                                'local_path': 'C:\\Users\\Dell\\Beacon Data\\OLAPLEX\\Olaplex-Retail-Dev\\_data\\ADI\\
                                                1616101111_ADI Inventory Report 2020-12-31.xlsx'
                            }
        """
        file_name = os.path.basename(report_dict["local_path"])
        _, file_extension = os.path.splitext(os.path.basename(report_dict["local_path"]))

        # calling the map_destination method from sub class which will be implemented in the sub class
        try:
            if file_extension.lower() in [".xlsx", ".xls"]:
                xl = pd.read_excel(report_dict["local_path"], sheet_name=None)
                sheet_list = xl.keys()
                for sheet_name in sheet_list:
                    self.map_destination(report_dict, file_name, sheet_name)

            elif file_extension == ".csv":
                self.map_destination(report_dict, file_name, None)

            else:
                pass
        except Exception as e:
            self.handle_parse_error(
                exc=e,
                report_dict=report_dict
            )

    @abstractmethod
    def map_destination(self, report_dict, file_name, sheet_name=None):
        # this will be implemented in sub class
        pass

    @classmethod
    def archive_s3_data(cls):
        """
        This is a class method used to move unprocessed data to processed folder
        after adding to the redshift tables
        """
        if os.environ.get('LC_CTYPE', '') == 'UTF-8':
            os.environ['LC_CTYPE'] = 'en_US.UTF-8'
        processed = f's3://{app_config.s3_bucket_name}/{app_config.s3_processed_dir}/'
        unprocessed = f's3://{app_config.s3_bucket_name}/{app_config.s3_unprocessed_dir}/'
        s3 = S3()
        if s3.is_folder_present(bucket_name=app_config.s3_bucket_name,
                                prefix=app_config.s3_unprocessed_dir):
            driver = create_clidriver()
            logging.info(f"Archiving the files on S3 ie: Moving From {unprocessed} to {processed} ")
            driver.main(f"s3 mv {unprocessed} {processed} --recursive".split())
        else:
            pass

    def append_metadata(self, df, report_dict, report_type, sheet=None):
        """
        This method used in child classes to append the metadata to dataframe
        Here we call the methods to create necessary id values to help deduplication issue
        :param df: DataFrame
        :param report_dict:{
                                'Date': 'Thu, 18 Mar 2021 22:30:25 +0530',
                                'From': 'Kasun Sampath <kasun.sampath@beacondata.com>',
                                'ID': '17846482fb05eabc',
                                'Message-ID': '<CAN3bJza9MW4K-m0PNDMmz59_bkrR66gcidiqEhB=08A0rK_h_A@mail.gmail.com>',
                                'Subject': 'ADI Reports',
                                'Timestamp': 1616086825,
                                'To': 'Sehan De Silva <sehan.desilva@beacondata.com>',
                                'local_path': 'C:\\Users\\Dell\\Beacon Data\\OLAPLEX\\Olaplex-Retail-Dev\\_data\\ADI\\
                                                1616086825_ADI Inventory Report 2021-01-31.xlsx',
                                'output_df':
                                'report_type': 'inventory',
                                'sheet_name' : 'Inventory',
                                's3_location': 's3://olaplex-retail-data-test/ADI/json/17846482fb05eabc/
                                                Inventory_1616086825_ADI Inventory Report 2021-01-31.json'
                            }
        :param report_type: 'inventory'
        :param sheet: 'Inventory'
        """
        file_name = os.path.basename(report_dict['local_path'])

        df["retailer_id"] = self.retailer_id
        df['retailer_name'] = self.retailer_id.split(' ', maxsplit=1)[1]
        df["retailer_internal_id"] = self.retailer_internal_id

        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

        uuid_list = []
        report_id_list = []
        for index in range(len(df)):
            md5_text = ''.join([val for val in df.iloc[index].astype(str).values])
            uuid_list.append(generate_uuid(md5_text))

            report_id_list.append(
                generate_report_id(
                    file_name=file_name,
                    reporting_period=df.reporting_period.iloc[index],
                    end_date=df.reporting_period_end.iloc[index] if report_type == 'sales'
                    else df.effective_date.iloc[index],
                    retailer_id=self.retailer_id,
                    num_records=len(df),
                    sheet_name=sheet
                )
            )

        df["uuid"] = uuid_list
        df['report_id'] = report_id_list

        df['record_id'] = [generate_record_id(report_id=report_id,row_number=index)
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

    def handle_parse_error(self, exc, report_dict, sheet=None, send_email=True):
        """
        This method handles the errors that occur while processing the files
        Here we send a mail when a error occurred along with the error details
        :param exc:
        :param report_dict: dictionary{}
        :param sheet: 'Sales'
        :param send_email: 'reports@olaplex.com'
        """
        logging.error(exc)
        file_name = os.path.basename(report_dict['local_path'])
        report_dict['Error in file'] = file_name
        report_dict['Error in sheet'] = sheet
        report_dict['Traceback'] = traceback.format_exc()
        error_msg = f"Error in file: {file_name} "
        if sheet:
            error_msg += f"sheet name: {sheet}"
        logging.error(error_msg)
        logging.error(report_dict['Traceback'])

        # send email message with error details
        if send_email:
            message = "Hello \n \n"
            skip_fields = ['ID', 'Message-ID', 'local_path', 'Timestamp']
            for k, v in report_dict.items():
                if k not in skip_fields:
                    message += f"{k} : {v} \n"
            message += "\nThank You"
            email_config = app_config.sender_email_config.copy()
            email_config['download_path'] = self.download_path
            gmail = Gmail(**email_config)
            gmail.send_email(
                from_email=app_config.sender_email_address,
                to_email=app_config.receiver_email_address,
                subject=f"Error in the retail pipline (retailer = {self.retailer_id})",
                message_text=message,
                attachment=[report_dict['local_path']]
            )
        error_file = os.path.join(self.download_path, 'error.log')
        with open(error_file, 'a') as f:
            f.write(f"File name: {file_name}\n"
                    f"Sheet name: {sheet}\n"
                    f"Traceback: {traceback.format_exc()}")
        logging.info(f"Error log written to {error_file}")

    @classmethod
    def load_to_final_table(cls, final_table=None):
        """
        This method loads data from staging tables to final tables. Here we handle the deduplicate data using
        FIRST VALUE window function.
        :param final_table: 'retail_sales'
        """
        rdsft = Redshift(**app_config.redshift_creds)
        Retailer.create_tables(type='final')  # Creates empty final tables if they don't exist

        if final_table is None or final_table.lower() == 'sales':
            logging.info(f"Loading Data to {app_config.redshift_final_sales_table}")
            sql_success = rdsft.run_sql_command(load_retail_sales_sql.format(
                REDSHIFT_STG_SALES_TABLE=app_config.redshift_stg_sales_table,
                REDSHIFT_FINAL_SALES_TABLE=app_config.redshift_final_sales_table))

            if sql_success:
                logging.info(f"Successfully Loaded data to {app_config.redshift_final_sales_table} from "
                             f"{app_config.redshift_stg_sales_table}")
            else:
                logging.error(f"Error Loading data to {app_config.redshift_final_sales_table} from "
                              f"{app_config.redshift_stg_sales_table}")

        if final_table is None or final_table.lower() == 'inventory':
            logging.info(f"Loading Data to  {app_config.redshift_final_inventory_table}")
            sql_success = rdsft.run_sql_command(load_retail_inventory_sql.format(
                REDSHIFT_STG_INVENTORY_TABLE=app_config.redshift_stg_inventory_table,
                REDSHIFT_FINAL_INVENTORY_TABLE=app_config.redshift_final_inventory_table))

            if sql_success:
                logging.info(
                    f"Successfully Loaded data to {app_config.redshift_final_inventory_table} from "
                    f"{app_config.redshift_stg_inventory_table}")
            else:
                logging.error(f"Error Loading data to {app_config.redshift_final_inventory_table} from "
                              f"{app_config.redshift_stg_inventory_table}")
