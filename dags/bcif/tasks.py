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
    return statusStart(listFileBase)


@task.branch(task_id="branch_task")
def branching(r: int) -> str:
    return branching_(r)


@task_group
def local_branch(params):

    @task(task_id="make_dirs")
    def make_dirs(listFileBase: str, updateBase: str, outputContentType: bool) -> bool:
        return makeDirs(listFileBase, updateBase, outputContentType)

    @task(task_id="split_tasks")
    def split_tasks(pdbHoldingsFilePath: str, csmHoldingsFilePath: str, listFileBase: str, updateBase: str, incrementalUpdate: bool, outfileSuffix: str, numSublistFiles: int, configPath: str, outputContentType: bool, outputHash: bool, result: bool):
        return splitRemoteTaskLists(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, updateBase, incrementalUpdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash)

    @task(task_id="get_list_files")
    def get_list_files(listFileBase: str, contentType: str, result: bool) -> bool:
        return getListFiles(listFileBase, contentType)

    @task(task_id="compute_bcif")
    def compute_bcif(listFileName: str, listFileBase: str, remotePath: str, outputPath: str, outfileSuffix: str, contentType: str, outputContentType: bool, outputHash: bool, inputHash: bool, batchSize: int, nfiles: int):
        return computeBcif(listFileName, listFileBase, remotePath, outputPath, outfileSuffix, contentType, outputContentType, outputHash, inputHash, batchSize, nfiles)

    @task(task_id="validate_output")
    def validate_output(listFileBase: str, updateBase: str, outfileSuffix: str, outputContentType: bool, outputHash: bool, result: bool) -> bool:
        return validateOutput(listFileBase=listFileBase, updateBase=updateBase, outfileSuffix=outfileSuffix, outputContentType=outputContentType, outputHash=outputHash)

    @task(task_id="remove_retracted_entries")
    def remove_retracted_entries(listFileBase: str, updateBase: str, outputContentType: bool, outputHash: bool, result: bool) -> bool:
        return removeRetractedEntries(listFileBase=listFileBase, updateBase=updateBase, outputContentType=outputContentType, outputHash=outputHash)

    @task(task_id="tasks_done")
    def tasks_done(result: bool) -> bool:
        return tasksDone()

    listFileBase = params.paths.listFileBase
    updateBase = params.paths.outputPath
    configPath = params.paths.config_path
    pdbRemotePath = params.urls.pdbRemotePath
    csmRemotePath = params.urls.csmRemotePath
    pdbHoldingsFilePath = params.urls.pdbHoldingsFilePath
    csmHoldingsFilePath = params.urls.csmHoldingsFilePath
    incrementalUpdate = bool(params.settings.incremental_update)
    outfileSuffix = params.settings.out_file_suffix
    numSublistFiles = int(params.settings.num_sublist_files)
    outputContentType = bool(params.settings.output_content_type)
    outputHash = bool(params.settings.output_hash)
    inputHash = bool(params.settings.input_hash)
    batchSize = int(params.settings.batch_size)
    nfiles = int(params.settings.nfiles)
    sequential = bool(params.settings.sequential)

    result1 = make_dirs(listFileBase, updateBase, outputContentType)

    result2 = split_tasks(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, updateBase, incrementalUpdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash, result1)

    result3 = get_list_files(listFileBase, "pdb", result2)

    if sequential:
        result4 = compute_bcif(listFileName=result3, listFileBase=listFileBase, remotePath=pdbRemotePath, outputPath=updateBase, outfileSuffix=outfileSuffix, contentType="pdb", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles)
    else:
        result4 = compute_bcif.partial(listFileBase=listFileBase, remotePath=pdbRemotePath, outputPath=updateBase, outfileSuffix=outfileSuffix, contentType="pdb", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles).expand(listFileName=result3)

    result5 = get_list_files(listFileBase, "csm", result4)

    if sequential:
        result6 = compute_bcif(listFileName=result5, listFileBase=listFileBase, remotePath=csmRemotePath, outputPath=updateBase, outfileSuffix=outfileSuffix, contentType="csm", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles)
    else:
        result6 = compute_bcif.partial(listFileBase=listFileBase, remotePath=remotePath, outputPath=updateBase, outfileSuffix=outfileSuffix, contentType="csm", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles).expand(listFileName=result5)

    result7 = validate_output(listFileBase, updateBase, outfileSuffix, outputContentType, outputHash, result6)

    result8 = remove_retracted_entries(listFileBase, updateBase, outputContentType, outputHash, result7)

    tasks_done(result8)


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


