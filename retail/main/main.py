import logging
import sys
from retailer import Retailer
from airflow import AirflowException
from retailers import *


def extract_retailer_data(retailer_name):
    try:
        retailer_obj = eval(retailer_name)()
        retailer_obj.parse_reports()
        retailer_obj.upload_to_s3()
    except Exception as e:
        raise AirflowException(e)


if __name__ == "__main__":

    if len(sys.argv) > 2 and sys.argv[1] == 'extract_retailer_data':
        retailer = sys.argv[2]
        extract_retailer_data(retailer)

    elif len(sys.argv) > 1 and sys.argv[1] == 'load_to_staging_tables':
        Retailer.load_to_staging_tables()
        Retailer.archive_s3_data()

    elif len(sys.argv) >= 2 and sys.argv[1] == 'load_to_final_table':
        if len(sys.argv) > 2:
            table_name = sys.argv[2].lower()
            if table_name not in ['sales', 'inventory']:
                logging.error(
                    f"Invalid table name {table_name} encountered, valid table names are - 'sales','inventory'")
                sys.exit(-1)
            else:
                Retailer.load_to_final_table(table_name)
        else:
            Retailer.load_to_final_table()

    else:
        logging.error("""Invalid argument. Valid arguments are : 
        1. extract_retailer_data <retailer_class_name>
        2. load_to_staging_tables
        3. load_to_final_table <table_type>
        """)
        sys.exit(-1)
