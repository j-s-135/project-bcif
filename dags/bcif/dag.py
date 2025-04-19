from airflow.models.dag import DAG
from hydra import compose, initialize
from bcif.tasks import *
import datetime

with DAG(
    "bcif",
    schedule="@once",
    start_date=datetime.datetime.now(),
    catchup=True,
    is_paused_upon_creation=True,
    params={
        "config_path": "config",
        "config_file": "config",
    }
) as dag:

    initialize(version_base=None, config_path=dag.params['config_path'])
    params = compose(config_name=dag.params['config_file'])

    route = int(params.settings.route)
    list_file_base = params.paths.listFileBase

    start_task >> status_start(list_file_base) >> branching(route) >> [local_branch(params), sfapi_branch(), k8s_branch()] >> end_task >> status_complete(list_file_base)

