import os
import logging
import configparser
from airflow.models import Variable


class AppConfig:

    def __init__(self, config_file=None, config_str=None, airflow_var='retail_pipeline_config'):
        self.config = configparser.ConfigParser()
        if config_file:
            self.config.read(config_file)
        elif config_str:
            self.config.read_string(config_str)
        else:
            # read application properties from Airflow
            try:
                app_props = Variable.get(airflow_var)
                self.config.read_string(app_props)
            except:
                logging.error(f"Failed to get the variable value from airflow : {airflow_var}")

    @property
    def s3_bucket_name(self):
        return self.config['S3']['BUCKET_NAME']

    @property
    def s3_unprocessed_dir(self):
        return self.config['S3']['UNPROCESSED_DIR']

    @property
    def s3_processed_dir(self):
        return self.config['S3']['PROCESSED_DIR']

    @property
    def s3_raw_dir(self):
        return self.config['S3']['RAW_DIR']

    @property
    def redshift_iam_role(self):
        return self.config['REDSHIFT']['IAM_ROLE']

    @property
    def redshift_schema(self):
        return self.config['REDSHIFT']['SCHEMA']

    @property
    def redshift_stg_sales_table(self):
        return f"{self.redshift_schema}.{self.config['REDSHIFT']['STG_SALES_TABLE']}"

    @property
    def redshift_stg_inventory_table(self):
        return f"{self.redshift_schema}.{self.config['REDSHIFT']['STG_INVENTORY_TABLE']}"

    @property
    def redshift_final_sales_table(self):
        return f"{self.redshift_schema}.{self.config['REDSHIFT']['FINAL_SALES_TABLE']}"

    @property
    def redshift_final_inventory_table(self):
        return f"{self.redshift_schema}.{self.config['REDSHIFT']['FINAL_INVENTORY_TABLE']}"

    @property
    def redshift_sephora_ca_stores_table(self):
        return f"{self.redshift_schema}.{self.config['REDSHIFT']['SEPHORA_CA_STORES_TABLE']}"

    @property
    def asos_zip_password(self):
        return self.config['RETAILER']['ASOS_ZIP_PWD'].encode()

    @property
    def redshift_creds(self):
        return {'host': self.config['REDSHIFT']['HOST'],
                'port': self.config['REDSHIFT']['PORT'],
                'dbname': self.config['REDSHIFT']['DBNAME'],
                'user': self.config['REDSHIFT']['user'],
                'password': self.config['REDSHIFT']['PASSWORD']
                }

    @property
    def download_path(self):
        return self.config['LOCAL']['DOWNLOAD_PATH']

    @property
    def reports_email_config(self):
        config_path = os.path.join(self.download_path, '_reports')
        if not os.path.exists(config_path):
            logging.info(f"Creating folder {config_path}")
            os.makedirs(config_path, exist_ok=True)

        return {
            'credentials_file': os.path.join(config_path, 'credentials.json'),
            'token_file': os.path.join(config_path, 'token.pickle'),
            'credentials_var': self.config['EMAIL']['REPORTS_EMAIL_VAR']
        }

    @property
    def sender_email_config(self):
        config_path = os.path.join(self.download_path, '_sender')
        if not os.path.exists(config_path):
            logging.info(f"Creating folder {config_path}")
            os.makedirs(config_path, exist_ok=True)

        return {
            'credentials_file': os.path.join(config_path, 'credentials.json'),
            'token_file': os.path.join(config_path, 'token.pickle'),
            'credentials_var': self.config['EMAIL']['SENDER_EMAIL_VAR']
        }

    @property
    def sender_email_address(self):
        return self.config['EMAIL']['SENDER_EMAIL']

    @property
    def receiver_email_address(self):
        return self.config['EMAIL']['RECEIVER_EMAIL']

    @property
    def project_home(self):
        return os.path.abspath(os.path.join(os.getcwd(), '../../'))

    @property
    def drop_recreate_stg_table(self):
        return self.config['REDSHIFT']['DROP_CREATE_STG_TABLES'] == 'True'

    @property
    def drop_recreate_final_table(self):
        return self.config['REDSHIFT']['DROP_CREATE_FINAL_TABLES'] == 'True'


app_config = AppConfig()
