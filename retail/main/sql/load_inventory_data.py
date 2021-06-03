load_retail_inventory_sql = '''
    INSERT INTO {REDSHIFT_FINAL_INVENTORY_TABLE} (
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
            effective_date,
            retailer_id ,
            retailer_name ,
            retailer_internal_id,
            plant_Id,
            plant_name,
            region,
            country ,
            state ,
            product_retailer_sku ,
            product_sku ,
            product_name ,
            product_size ,
            product_line ,
            currency ,
            quantity_warehouse ,
            quantity_physical ,
            quantity_intransit ,
            value_warehouse ,
            value_physical ,
            value_intransit ,
            tags ,
            "type",
            note
        FROM {REDSHIFT_STG_INVENTORY_TABLE} 
        WHERE record_id NOT IN (SELECT DISTINCT record_id FROM {REDSHIFT_FINAL_INVENTORY_TABLE})
    );
'''
