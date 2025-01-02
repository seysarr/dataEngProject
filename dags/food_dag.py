import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import libs.ingestion as ingestionLib
import libs.wrangling as wranglingLib
import libs.production as productionLib

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

food_dag = DAG(
    dag_id='food_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

task_check_ingestion_errors = BranchPythonOperator(
    task_id='check_ingestion_errors',
    python_callable=ingestionLib.check_task_status,
    dag=food_dag,
    op_kwargs={'success_task_id': 'WRANGLING'},
    trigger_rule='all_done'
)

task_check_wrangling_errors = BranchPythonOperator(
    task_id='check_wrangling_errors',
    python_callable=ingestionLib.check_task_status,
    dag=food_dag,
    op_kwargs={'success_task_id': 'PROD'},
    trigger_rule='all_done'
)

task_check_prod_errors = BranchPythonOperator(
    task_id='check_prod_errors',
    python_callable=ingestionLib.check_task_status,
    dag=food_dag,
    op_kwargs={'success_task_id': 'finish_well'},
    trigger_rule='all_done'# all_done
)

task_handle_ingestion_error = PythonOperator(
    task_id='handle_ingestion_error',
    python_callable=lambda: print("AN ERROR OCCURED DURING INGESTION !!!"),
    dag=food_dag,
    trigger_rule='all_done'#one_failed
)

task_handle_wrangling_error = PythonOperator(
    task_id='handle_wrangling_error',
    python_callable=lambda: print("AN ERROR OCCURED DURING WRANGLING!!!"),
    dag=food_dag,
    trigger_rule='all_done'
)

task_handle_prod_error = PythonOperator(
    task_id='handle_prod_error',
    python_callable=lambda: print("AN ERROR OCCURED IN PRODUCTION PIPELINE!!!"),
    dag=food_dag,
    trigger_rule='all_done'
)

task_init_hummus_data = BashOperator(
    task_id='get_hummus_data',
    dag=food_dag,
    bash_command="curl -s -o /opt/airflow/dags/outputs/temp.zip \"https://gitlab.com/felix134/connected-recipe-data-set/-/raw/master/data/hummus_data/preprocessed/pp_recipes.zip\" && python3 -c \"import zipfile; zipfile.ZipFile('/opt/airflow/dags/outputs/temp.zip').extractall('/opt/airflow/dags/outputs')\" && rm /opt/airflow/dags/outputs/temp.zip"
)

task_fetch_spooncular_recipes = PythonOperator(
    task_id='fetch_spooncular_recipes',
    dag=food_dag,
    python_callable=ingestionLib.get_spooncular_recipes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    depends_on_past=False
)

task_start_wrangling = DummyOperator(
    task_id='WRANGLING',
    trigger_rule='none_failed',
    dag=food_dag
)

task_sample_hummus_data = PythonOperator(
    task_id='sample_hummus_data',
    dag=food_dag,
    python_callable=wranglingLib.sample_hummus,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_fix_hummus_rows = PythonOperator(
    task_id='fix_hummus_rows',
    dag=food_dag,
    python_callable=wranglingLib.rearrange_broken_lines,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_clean_data = PythonOperator(
    task_id='clean_data',
    dag=food_dag,
    python_callable=wranglingLib.clean_data,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_add_diets_hummus = PythonOperator(
    task_id='add_diets_hummus',
    dag=food_dag,
    python_callable=wranglingLib.add_diets_column,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_merge_recipes = PythonOperator(
    task_id='merge_recipes',
    dag=food_dag,
    python_callable=wranglingLib.merge_recipes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_add_health_effects_column = PythonOperator(
    task_id='add_health_effects_column',
    dag=food_dag,
    python_callable=wranglingLib.add_health_effects_column,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_save_to_mongodb = PythonOperator(
    task_id= 'save_to_mongodb',
    dag=food_dag,
    python_callable=wranglingLib.save_to_mongoDB,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/outputs",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

task_move_to_prod = DummyOperator(
    task_id='PROD',
    trigger_rule='none_failed',
    dag=food_dag
)

task_fill_graph_DB = PythonOperator(
    task_id= 'fill_graph_DB',
    dag=food_dag,
    python_callable= productionLib.move_to_graph_DB,
    op_kwargs={
        "user": "neo4j",
        "password": "adminPass"
    },
    trigger_rule='all_success',
    depends_on_past=False
)

last_task = DummyOperator(
    task_id='finish_well',
    # trigger_rule='none_failed',
    dag=food_dag
)

task_Cleanup = BashOperator(
    task_id='cleanup',
    dag=food_dag,
    bash_command="find /opt/airflow/dags/outputs -type f -name \"*{{ execution_date.int_timestamp }}*\" -delete",
    trigger_rule='all_done'
)


# TODO: 
# Manage error nodes, verify nodes trigger_rule, rearrange dags
# Delete test files calls

INGESTION
[task_init_hummus_data, task_fetch_spooncular_recipes] >> task_check_ingestion_errors
task_check_ingestion_errors >> [task_handle_ingestion_error, task_start_wrangling]
task_handle_ingestion_error >> task_Cleanup

# WRANGLING
task_start_wrangling >> task_sample_hummus_data >> task_fix_hummus_rows >> task_clean_data >> task_add_diets_hummus 
task_add_diets_hummus >> task_merge_recipes >> task_add_health_effects_column >> task_save_to_mongodb >> task_check_wrangling_errors
task_check_wrangling_errors >> [task_handle_wrangling_error, task_move_to_prod]
task_handle_wrangling_error >> task_Cleanup

# PRODUCTION
task_move_to_prod >> task_fill_graph_DB >> task_check_prod_errors >> [task_handle_prod_error, last_task]
[task_handle_prod_error, last_task] >> task_Cleanup