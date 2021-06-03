load_retail_sales_sql = '''
    INSERT INTO {REDSHIFT_FINAL_SALES_TABLE} (
        SELECT DISTINCT record_id,
            report_id,
            uuid,
            FIRST_VALUE(created_at) OVER(partition by record_id, report_id, uuid 
                order by created_at desc rows between unbounded preceding and unbounded following),
            FIRST_VALUE (file_name) OVER(partition by record_id, report_id, uuid 
                order by created_at desc rows between unbounded preceding and unbounded following),
            sheet_name,
            number_of_records_in_sheet ,
            FIRST_VALUE(sender_email_address)  OVER(partition by record_id, report_id, uuid 
                order by created_at desc rows between unbounded preceding and unbounded following),
            FIRST_VALUE(email_subject) OVER(partition by record_id, report_id, uuid 
                order by created_at desc rows between unbounded preceding and unbounded following),
            FIRST_VALUE(email_received_date) OVER(partition by record_id, report_id, uuid 
                order by created_at desc rows between unbounded preceding and unbounded following),
            reporting_period,
            reporting_period_start,
            reporting_period_end,
            retailer_id,
            retailer_name,
            retailer_internal_id,
            sell_through_channel,
            store_id,
            store_name,
            region,
            country,
            state,
            product_retailer_sku,
            product_sku,
            product_name,
            product_size,
            product_line,
            currency,
            total_quantity,
            total_value,
            return_quantity,
            return_value,
            free_replacements_quantity,
            free_replacements_value,
            tags ,
            "type",
            note
        FROM {REDSHIFT_STG_SALES_TABLE}
        WHERE record_id NOT IN (SELECT DISTINCT record_id FROM {REDSHIFT_FINAL_SALES_TABLE})
    );

'''
