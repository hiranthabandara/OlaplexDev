import logging
import calendar
import pandas as pd
from datetime import date
from retail.main.retailer import Retailer
from airflow.models import Variable


def get_filtered_df(df, date_column, num_months=1):
    """
    The file sent by sender contains data with filtered values. It also contains past values. Using this method we
    filter out the records for the latest month or according to the required amount of months
    :param df:
    :param date_column:
    :param num_months:
    :return df:
    """
    recent_date = pd.to_datetime(df[date_column].unique().max()) - pd.DateOffset(months=num_months)
    recent_date = date(recent_date.year, recent_date.month, 1)
    df['is_latest_date'] = [a >= recent_date for a in df[date_column]]
    df = df[df.is_latest_date == True]
    df = df.reset_index(drop=True)
    return df


def get_start_date(input_date):
    """
    This method returns the start date of the report
    :param input_date:
    :return start_date: '2021-03-01'
    """
    try:
        year = input_date.year
        month = input_date.month
        start_date = date(year, month, 1).strftime("%Y-%m-%d")
        return start_date
    except Exception as e:
        logging.error(e)


def get_end_date(input_date):
    """
    This method returns the end date of the report
    :param input_date: '2021-03-01'
    :return start_date: '2021-03-31'
    """
    try:
        year = input_date.year
        month = input_date.month
        num_days = int(calendar.monthrange(year, month)[1])
        end_date = date(year, month, num_days).strftime("%Y-%m-%d")
        return end_date
    except Exception as e:
        logging.error(e)


def get_num_months():
    """
    This method retrieves value from Airflow Variables. Decides how many months we should retrieve data from the
    report
    :return num_months: 1
    """
    retailers = Variable.get("retailer_config", deserialize_json=True)
    try:
        return retailers['SallyBeautyMexico']['num_months']
    except:
        return 1


class SallyBeautyMexico(Retailer):
    def __init__(self):
        super().__init__()

        # Define the mappings here. The number of mapping variables could be more than 2
        self.sales_of_stores_mapping = {
            "Store ID": "store_id",
            "Store name": "store_name",
            "SKU": "product_sku",
            "Product": "product_name",
            "Quantity": "total_quantity",
            "Price": "total_value",
        }
        self.sales_of_stores_hardcoded = {
            "reporting_period": "Monthly",
            "currency": "USD",
            "sell_through_channel": "store",
            "type": "by_channel_sku"
        }
        self.sales_of_professional_mapping = {
            "Client": "store_name",
            "SKU": "product_sku",
            "Product": "product_name",
            "Quantity": "total_quantity",
            "Price": "total_value",
        }
        self.sales_of_professional_hardcoded = {
            "reporting_period": "Monthly",
            "currency": "USD",
            "sell_through_channel": "professional",
            "type": "by_channel_sku"
        }

        self.inventory_mapping = {
            "effective_date": "effective_date",
            "plant_Id": "plant_Id",
            "product_sku": "product_sku",
            "product_name": "product_name",
            "product_size": "product_size",
            "product_line": "product_line",
            "currency": "currency",
            "quantity_warehouse": "quantity_warehouse",
            "quantity_physical": "quantity_physical",
            "quantity_intransit": "quantity_intransit",
            "value_warehouse": "value_warehouse",
            "value_physical": "value_physical",
            "value_intransit": "value_intransit",
            "Tags": "tags"
        }
        self.inventory_hardcoded = {
            "reporting_period": "Monthly",
            "type": "by_sku"
        }

    def map_destination(self, report_dict, file_name, sheet_name=None):
        """
        The purpose of this map method is to figure out which method to call based on file_name and sheet_name pattern
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Olaplex Sales 20-21, March 21',
                                'From': 'Steven Ihms <steven.lhms@haircareaust.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_Olaplex Sales 20-21.XLSX'
                            }
        :param file_name: 'Olaplex Sales 20-21.XLSX'
        :param sheet_name: 'Sales of stores'
        """
        if "Olaplex Sales".lower() in file_name.lower():
            if sheet_name.lower() == 'Sales of stores'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sales_of_stores_mapping,
                    hardcoded_dict=self.sales_of_stores_hardcoded
                )
            elif sheet_name.lower() == 'Sales of Professional'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='sales',
                    mapping_dict=self.sales_of_professional_mapping,
                    hardcoded_dict=self.sales_of_professional_hardcoded
                )
            elif sheet_name.lower() == 'Inventory'.lower():
                self.parse_sales_inventory(
                    report_dict=report_dict.copy(),
                    sheet=sheet_name,
                    report_type='inventory',
                    mapping_dict=self.inventory_mapping,
                    hardcoded_dict=self.inventory_hardcoded
                )
            else:
                pass
        else:
            pass

    def parse_sales_inventory(self, report_dict, sheet, report_type, mapping_dict, hardcoded_dict):
        """
        Processing the data according to the report type and detects any Exceptions caused
        :param report_dict: {
                                'ID': '1784721fdb3cad7d',
                                'Message-ID':'<CAPkHq_d1emfprT9of1_cYtZ9NKz=8H+wSxMsfxdTbaoqzTLU2Q@mail.gmail.com>',
                                'Subject': '[EXTERNAL SENDER] Olaplex Sales 20-21, March 21',
                                'From': 'Steven Ihms <steven.lhms@haircareaust.com>',
                                'To': 'Olaplex Reports <reports@olaplex.com>',
                                'Date': 'Mon, Mar 8, 2021 at 10:46 AM',
                                'Timestamp': 1616101111,
                                'local_path': '_data\\HaircareAustralia\\1616101111_Olaplex Sales 20-21.XLSX'
                            }
        :param sheet_name: 'Sales of stores'
        :param report_type: 'sales'
        :param mapping_dict: sales_of_stores_mapping
        :param hardcoded_dict: sales_of_stores_hardcoded
        """
        try:
            df_original = pd.read_excel(report_dict['local_path'], sheet)
            df = pd.DataFrame(columns=df_original.columns)

            if report_type == 'sales':
                df = get_filtered_df(df=df_original, date_column='Month', num_months=get_num_months())
                df.rename(columns=mapping_dict, inplace=True)
                df["reporting_period_start"] = df.apply(lambda x: get_start_date(x.Month), axis=1)
                df["reporting_period_end"] = df.apply(lambda x: get_end_date(x.Month), axis=1)

            elif report_type == 'inventory':
                df = get_filtered_df(df=df_original, date_column='effective_date', num_months=get_num_months())
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
