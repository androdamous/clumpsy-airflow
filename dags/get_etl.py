# get_etl.py
def get_etl(**kwargs):
    # Pull output from 'get_source_name'
    ti = kwargs['ti']
    source = ti.xcom_pull(task_ids='get_source_name')
    result = f"ETL configuration received from ({source})"
    print(result)
    return result
