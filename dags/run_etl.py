# run_etl.py
def run_etl(**kwargs):
    ti = kwargs['ti']
    etl_config = ti.xcom_pull(task_ids='get_etl')
    result = f"ETL run completed with configuration ({etl_config})"
    print(result)
    return result
