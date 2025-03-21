# update_etl.py
def update_etl(**kwargs):
    ti = kwargs['ti']
    etl_run = ti.xcom_pull(task_ids='run_etl')
    result = f"ETL updated based on run result ({etl_run})"
    print(result)
    return result
