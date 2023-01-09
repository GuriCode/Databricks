def write_into_synapse(source_dataframe, target_schema, target_table, pre_actions, post_actions):
    try:
        if target_schema == default_config_schema:
            v_post_actions = post_actions
        else:
            v_post_actions = f'update statistics {target_schema}.{target_table};{post_actions}'
        source_dataframe.write \
            .mode('append') \
            .format('com.databricks.spark.sqldw') \
            .option('url', synURL) \
            .option('tempDir', synTmpDir) \
            .option('enableServicePrincipalAuth', True) \
            .option('maxStrLength', 4000) \
            .option('preActions', pre_actions) \
            .option('postActions', v_post_actions) \
            .option('applicationName', f'{application_name}-databricks') \
            .option('dbTable', f'{target_schema}.{target_table}') \
            .save()
        return 0
    except:
        log(sys.exc_info()[0])
        log(sys.exc_info()[1])
        raise

# COMMAND ----------

def check_for_upsert_failure(table_name):
    try:
        df = create_dataframe_from_query(f'select count(*) as record_count from sys.tables as t, sys.schemas s where t.schema_id = s.schema_id and s.name = \'{default_audit_schema}\' and t.name = \'{table_name}\'')
        r = df.first().record_count
        if r >= 1:
            raise Exception(f'Failure in upsert process. Check intermediate table - {table_name}')
        return 0
    except:
        log(sys.exc_info()[0])
        log(sys.exc_info()[1])
        raise

# COMMAND ----------

# Load data into Synapse table using upsert strategy
def merge_into_synapse(source_dataframe, target_schema, target_table, key_columns, sk_column):
    try:
        tmp_table = f'{target_schema}_{target_table}_{start_time.replace(" ", "").replace("-", "").replace(":", "")}_{get_job_run_id()}'
        keys = ', '.join(key_columns)
        post_actions = f'exec {default_config_schema}.sp_perform_upsert N\'{default_audit_schema}.{tmp_table}\', N\'{target_schema}.{target_table}\', N\'{keys}\', N\'{sk_column}\';'
        write_into_synapse(source_dataframe, default_audit_schema, tmp_table, '', post_actions)
        log('Merge complete')
        capture_audit_info(f'{target_schema}.{target_table}', 'table', source_dataframe.count(), 'output')
        check_for_upsert_failure(tmp_table)
    except:
        log(sys.exc_info()[0])
        log(sys.exc_info()[1])
        raise

# COMMAND ----------

# Load data into Synapse table using truncate/load strategy
def truncate_into_synapse(source_dataframe, target_schema, target_table):
    try:
        pre_actions = f'truncate table {target_schema}.{target_table};'
        post_actions = f'update statistics {target_schema}.{target_table};'
        write_into_synapse(source_dataframe, target_schema, target_table, pre_actions, post_actions)
        log('Load complete')
        capture_audit_info(f'{target_schema}.{target_table}', 'table', source_dataframe.count(), 'output')
    except:
        log(sys.exc_info()[0])
        log(sys.exc_info()[1])
        raise
