##
# File:    tasks.py
# Author:  James Smith
# Date:    21-Apr-2025
##

"""
Airflow workflow tasks.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
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
    def make_dirs(listFileBase: str, outputPath: str, outputContentType: bool) -> bool:
        return makeDirs(listFileBase, outputPath, outputContentType)

    @task(task_id="split_tasks")
    def split_tasks(pdbHoldingsFilePath: str, csmHoldingsFilePath: str, listFileBase: str, outputPath: str, incrementalUpdate: bool, outfileSuffix: str, numSublistFiles: int, configPath: str, outputContentType: bool, outputHash: bool, result: bool = None):
        return splitRemoteTaskLists(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, outputPath, incrementalUpdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash)

    @task(task_id="get_list_files")
    def get_list_files(listFileBase: str, contentType: str, result: bool = None) -> bool:
        return getListFiles(listFileBase, contentType)

    @task(task_id="compute_bcif")
    def compute_bcif(listFileName: str, listFileBase: str, remotePath: str, outputPath: str, outfileSuffix: str, contentType: str, outputContentType: bool, outputHash: bool, inputHash: bool, batchSize: int, nfiles: int):
        return computeBcif(listFileName, listFileBase, remotePath, outputPath, outfileSuffix, contentType, outputContentType, outputHash, inputHash, batchSize, nfiles)

    @task(task_id="validate_output")
    def validate_output(listFileBase: str, outputPath: str, outfileSuffix: str, outputContentType: bool, outputHash: bool, result: bool = None) -> bool:
        return validateOutput(listFileBase=listFileBase, outputPath=outputPath, outfileSuffix=outfileSuffix, outputContentType=outputContentType, outputHash=outputHash)

    @task(task_id="remove_retracted_entries")
    def remove_retracted_entries(listFileBase: str, outputPath: str, result: bool = None) -> bool:
        return removeRetractedEntries(listFileBase=listFileBase, outputPath=outputPath)

    @task(task_id="tasks_done")
    def tasks_done(result: bool = None) -> bool:
        return tasksDone()

    listFileBase = params.paths.listFileBase
    outputPath = params.paths.outputPath
    configPath = params.paths.configPath
    pdbRemotePath = params.urls.pdbRemotePath
    csmRemotePath = params.urls.csmRemotePath
    pdbHoldingsFilePath = params.urls.pdbHoldingsFilePath
    csmHoldingsFilePath = params.urls.csmHoldingsFilePath
    incrementalUpdate = bool(params.settings.incrementalUpdate)
    outfileSuffix = params.settings.outFileSuffix
    numSublistFiles = int(params.settings.numSublistFiles)
    outputContentType = bool(params.settings.outputContentType)
    outputHash = bool(params.settings.outputHash)
    inputHash = bool(params.settings.inputHash)
    batchSize = int(params.settings.batchSize)
    nfiles = int(params.settings.nfiles)
    sequential = bool(params.settings.sequential)

    result1 = make_dirs(listFileBase, outputPath, outputContentType)

    result2 = split_tasks(pdbHoldingsFilePath, csmHoldingsFilePath, listFileBase, outputPath, incrementalUpdate, outfileSuffix, numSublistFiles, configPath, outputContentType, outputHash)

    result3 = get_list_files(listFileBase, "pdb", result2)

    if sequential:
        result4 = compute_bcif(listFileName=result3, listFileBase=listFileBase, remotePath=pdbRemotePath, outputPath=outputPath, outfileSuffix=outfileSuffix, contentType="pdb", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles)
    else:
        result4 = compute_bcif.partial(listFileBase=listFileBase, remotePath=pdbRemotePath, outputPath=outputPath, outfileSuffix=outfileSuffix, contentType="pdb", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles).expand(listFileName=result3)

    result5 = get_list_files(listFileBase, "csm", result4)

    if sequential:
        result6 = compute_bcif(listFileName=result5, listFileBase=listFileBase, remotePath=csmRemotePath, outputPath=outputPath, outfileSuffix=outfileSuffix, contentType="csm", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles)
    else:
        result6 = compute_bcif.partial(listFileBase=listFileBase, remotePath=csmRemotePath, outputPath=outputPath, outfileSuffix=outfileSuffix, contentType="csm", outputContentType=outputContentType, outputHash=outputHash, inputHash=inputHash, batchSize=batchSize, nfiles=nfiles).expand(listFileName=result5)

    result7 = validate_output(listFileBase, outputPath, outfileSuffix, outputContentType, outputHash, result6)

    result8 = remove_retracted_entries(listFileBase, outputPath, result7)

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


