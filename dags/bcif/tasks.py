from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import sys
import datetime
from bcif.task_functions import *


start_task = EmptyOperator(task_id="start")


@task(task_id="status_start")
def status_start(listFileBase: str) -> bool:
    return status_start_(listFileBase)


@task.branch(task_id="branch_task")
def branching(r: int) -> str:
    return branching_(r)


@task_group
def local_branch(*args, **kwargs):

    @task(task_id="make_dirs")
    def make_dirs(listFileBase: str, updateBase: str, outputContentType: bool) -> bool:
        return makeDirs(listFileBase, updateBase, outputContentType)

    @task(task_id="split_tasks")
    def split_tasks(pdbHoldingsFilePath: str, csmHoldingsFilePath: str, listFileBase: str, updateBase: str, incrementalUpdate: bool, outfileSuffix: str, numSublistFiles: int, configPath: str, outputContentType: bool, outputHash: bool, result: bool):
        return splitRemoteTaskLists(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, updateBase, incrementalUpdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash)

    @task(task_id="get_list_files")
    def get_list_files(listFileBase: str, result: bool):
        return getListFiles(listFileBase)

    @task(task_id="compute_bcif")
    def compute_bcif(listFileBase: str, listFileName: str, remotePath: str, outputPath: str, outfileSuffix: str, contentType: str, outputContentType: bool, outputHash: bool, inputHash: bool, batchSize: int, nfiles: int):
        return computeBcif(listFileBase, listFileName, remotePath, outputPath, outfileSuffix, contentType, outputContentType, outputHash, inputHash, batchSize, nfiles)

    @task(task_id="validate_output")
    def validate_output(listFileBase: str, updateBase: str, outfileSuffix: str, outputContentType: bool, outputHash: bool, result: bool) -> bool:
        return validateOutput(listFileBase=listFileBase, updateBase=updateBase, outfileSuffix=outfileSuffx, outputContentType=outputContentType, outputHash=outputHash)

    @task(task_id="remove_retracted_entries")
    def remove_retracted_entries(listFileBase: str, updateBase: str, outputContentType: bool, outputHash: bool, result: bool) -> bool:
        return removeRetractedEntries(listFileBase=listFileBase, updateBase=updateBase, outputContentType=outputContentType, outputHash=outputHash)

    @task(task_id="tasks_done")
    def tasks_done(result: bool) -> bool:
        return tasksDone()

    listFileBase = kwargs['listFileBase']
    updateBase = kwargs['outputPath']
    pdbRemotePath = kwargs['pdbRemotePath']
    csmRemotePath = kwargs['csmRemotePath']
    pdbHoldingsFilePath = kwargs['pdbHoldingsFilePath']
    csmHoldingsFilePath = kwargs['csmHoldingsFilePath']
    incrementalUpdate = bool(kwargs['incremental_update'])
    outfileSuffix = kwargs['out_file_suffix']
    numSublistFiles = int(kwargs['num_sublist_files'])
    configPath = kwargs['config_path']
    outputContentType = bool(kwargs['output_content_type'])
    outputHash = bool(kwargs['output_hash'])

    result = make_dirs(listFileBase, updateBase, outputContentType)

    result = split_tasks(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, updateBase, incrementalUPdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash, result)

    contentType = "pdb"
    remotePath = pdbRemotePath
    filepaths = get_list_files(listFileBase, contentType, result)
    result = compute_bcif.partial(listFileBase, remotePath, outputPath, outfileSuffix, contentType, outputContentType, outputHash, inputHash, batchSize, nfiles).expand(listFileName=filepaths)

    contentType = "csm"
    remotePath = csmRemotePath
    filepaths = get_list_files(listFileBase, contentType, result)
    result = compute_bcif.partial(listFileBase, remotePath, outputPath, outfileSuffix, contentType, outputContentType, outputHash, inputHash, batchSize, nfiles).expand(listFileName=filepaths)

    result = validate_output(listFileBase, updateBase, outfileSuffix, outputContentType, outputHash, result)

    result = remove_retracted_entries(listFileBase, updateBase, outputContentType, outputHash, result)

    tasks_done(result)


@task_group
def k8s_branch(*args, **kwargs):
    pass

@task_group
def sfapi_branch(*args, **kwargs):
    pass

end_task = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ONE_SUCCESS,
)

@task(task_id="status_complete")
def status_complete(listFileBase: str) -> bool:
    return statusComplete(listFileBase)


