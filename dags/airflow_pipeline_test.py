from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# ========== DAG config ==========
dag = DAG(
    'DE_project',
    description='DAG with branching logic using PythonOperator',
    schedule='0 6 * * *',
    start_date=datetime(2025, 7, 20),
    catchup=False
)

# ========== Task 1: Chạy check_new_data.py ==========

# check_new_data = BashOperator(
#     task_id='check_new_data',
#     bash_command="spark-submit /opt/spark/jars/check_new_data.py",
#     do_xcom_push=True,   # để lấy stdout làm input cho nhánh
#     dag=dag,
# )

check_new_data = SparkSubmitOperator(
    task_id='check_new_data',
    application='/opt/airflow/scripts/check_new_data.py',  # đường dẫn trong container
    conn_id='spark_default',      # connection Spark trong Airflow
    verbose=True,
    do_xcom_push=True,            # lấy stdout làm input cho nhánh
    deploy_mode='client',
    dag=dag,
)


# ========== Task 2: Quyết định nhánh ==========
def decide_next(**kwargs):
    output = kwargs['ti'].xcom_pull(task_ids='check_new_data')
    if output and "NEW_DATA=True" in output:
        return 'run_etl'
    return 'no_new_data'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next,
    dag=dag,
)

# ========== Task 3: Chạy pyspark_etl_auto_test.py.py nếu có dữ liệu ==========
# run_etl = BashOperator(
#     task_id='run_etl',
#     bash_command="spark-submit /opt/spark/jars/pyspark_etl_auto_test.py",
#     dag=dag,
# )

run_etl = SparkSubmitOperator(
    task_id='run_etl',
    application='/opt/airflow/scripts/pyspark_etl_auto_test.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# ========== Task 4: In ra nếu không có dữ liệu ==========
def log_no_data(**kwargs):
    print("No new data found")

no_new_data = PythonOperator(
    task_id='no_new_data',
    python_callable=log_no_data,
    dag=dag,
)

# ========== Empty task kết thúc ==========
end = EmptyOperator(task_id='end', dag=dag)

# ========== Logic ==========
check_new_data >> branch_task
branch_task >> run_etl >> end
branch_task >> no_new_data >> end
