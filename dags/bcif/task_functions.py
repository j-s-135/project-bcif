##
# File:    task_functions.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Workflow task descriptors.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import multiprocessing
import os
import shutil
import glob
import pathlib
import tempfile
import datetime
import logging
from typing import List
import time
import requests

logger = logging.getLogger(__name__)


def statusStart(listFileBase: str) -> bool:
    statusStartFile = "status.start"
    startFile = os.path.join(listFileBase, statusStartFile)
    dirs = os.path.dirname(startFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(startFile, "w", encoding="utf-8") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def makeDirs(
    listFileBase: str, updateBase: str, outputContentType: bool
) -> bool:
    try:
        if not os.path.exists(listFileBase):
            os.mkdir(listFileBase)
            os.chmod(listFileBase, 0o777)
        if not os.path.exists(updateBase):
            os.mkdir(updateBase)
            os.chmod(updateBase, 0o777)
        if outputContentType:
            for contentType in ["pdb", "csm"]:
                path = os.path.join(updateBase, contentType)
                if not os.path.exists(path):
                    os.mkdir(path)
                    os.chmod(path, 0o777)
    except Exception as e:
        logger.error(str(e))
        return False
    return True


def branching_(r: int) -> str:
    routes = ["local", "sfapi", "k8s"]
    route = routes[r]
    if route == "sfapi":
        return "sfapi_tasks"
    elif route == "k8s":
        return "k8s_tasks"
    else:
        return "local_branch"


def splitRemoteTaskLists(
    pdbHoldingsFilePath: str,
    csmHoldingsFilePath: str,
    loadFileListDir: str,
    targetFileDir: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    holdingsFilePath = pdbHoldingsFilePath
    databaseName = "pdbx_core"
    result1 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    holdingsFilePath = csmHoldingsFilePath
    databaseName = "pdbx_comp_model_core"
    result2 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    if not result1:
        logger.error("exp list failed to load")
    if not result2:
        logger.error("comp list failed to load")
    # enable skipping one or the other
    if result1 or result2:
        return True
    return False


def splitRemoteTaskList(
    loadFileListDir: str,
    holdingsFilePath: str,
    targetFileDir: str,
    databaseName: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    op = "pdbx_id_list_splitter"
    loadFileListPrefix = databaseName + "_ids"
    if numSublistFiles == 0:
        numSublistFiles = multiprocessing.cpu_count()
    incremental = ""
    if incrementalUpdate:
        incremental = "--incremental_update"
    outContentType = ""
    if outputContentType:
        outContentType = "--prepend_output_content_type"
    outHash = ""
    if outputHash:
        outHash = "--prepend_output_hash"
    cmd = f"python3 -m rcsb.db.cli.RepoLoadExec --op {op} --database {databaseName} --load_file_list_dir {loadFileListDir} --holdings_file_path {holdingsFilePath} --num_sublists {numSublistFiles} --target_file_dir {targetFileDir} --target_file_suffix {outfileSuffix} --config_path {configPath} {incremental} {outContentType} {outHash}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def getListFiles(listFileBase: str, contentType: str) -> list:
    filepaths = []
    if contentType == "pdb":
        for filepath in glob.glob(os.path.join(listFileBase, "pdbx_core_ids-*.txt")):
            filepaths.append(filepath)
    elif contentType == "csm":
        for filepath in glob.glob(os.path.join(listFileBase, "pdbx_comp_model_core_ids-*.txt")):
            filepaths.append(filepath)
    return filepaths


def computeBcif(
    listFileName,
    listFileBase,
    remotePath,
    outputPath,
    outfileSuffix,
    contentType,
    outputContentType,
    outputHash,
    inputHash,
    batchSize,
    nfiles,
) -> bool:
    outContentType = ""
    outHash = ""
    inHash = ""
    if outputContentType:
        outContentType = "--outputContentType"
    if outputHash:
        outHash = "--outputHash"
    if inputHash:
        inHash = "--inputHash"
    options = [
        "python3 -m rcsb.workflow.cli.BcifExec",
        f"--batchSize {batchSize}",
        f"--nfiles {nfiles}",
        f"--listFileBase {listFileBase}",
        f"--listFileName {listFileName}",
        f"--remotePath {remotePath}",
        f"--outputPath {outputPath}",
        f"--outfileSuffix {outfileSuffix}",
        f"--contentType {contentType}",
        f"{outContentType}",
        f"{outHash}",
        f"{inHash}",
    ]
    cmd = " ".join(options)
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def validateOutput(
    *,
    listFileBase: str,
    updateBase: str,
    outfileSuffix: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    missingFileName = "missing.txt"
    missing = []
    for path in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        for line in open(path, "r", encoding="utf-8"):
            pdbId = line.strip()
            # list files have upper case for all model types
            # experimental models stored with lower case file name and hash
            if path.find("comp_model") < 0:
                pdbId = line.strip().lower()
            contentType = "pdb"
            dividedPath = pdbId[-3:-1]
            # csms stored with upper case file name and hash
            if path.find("comp_model") >= 0:
                contentType = "csm"
                dividedPath = os.path.join(pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2])
            if outputContentType and outputHash:
                out = os.path.join(
                    updateBase,
                    contentType,
                    dividedPath,
                    "%s%s" % (pdbId, outfileSuffix),
                )
            elif outputContentType:
                out = os.path.join(
                    updateBase, contentType, "%s%s" % (pdbId, outfileSuffix)
                )
            elif outputContentType and outputHash:
                out = os.path.join(
                    updateBase, dividedPath, "%s%s" % (pdbId, outfileSuffix)
                )
            else:
                out = os.path.join(updateBase, "%s%s" % (pdbId, outfileSuffix))
            if not os.path.exists(out):
                missing.append(out)
    if len(missing) > 0:
        missingFile = os.path.join(listFileBase, missingFileName)
        with open(missingFile, "w", encoding="utf-8") as w:
            for line in missing:
                w.write(line)
                w.write("\n")
    return True


def removeRetractedEntries(
    *,
    listFileBase: str,
    updateBase: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    removedFileName = "removed.txt"
    removed = []
    t = time.time()
    infiles = []
    for filepath in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        """uncomment to test
        if filepath.find("comp_model") >= 0:
            os.unlink(filepath)
            continue
        """
        with open(filepath, "r", encoding="utf-8") as r:
            infiles.extend(r.read().split("\n"))
    infiles = [file for file in infiles if file != ""]
    infiles = set(infiles)
    outfiles = {
        os.path.basename(path)
        .replace(".bcif.gz", "")
        .replace(".bcif", "")
        .upper(): str(path)
        for path in pathlib.Path(updateBase).rglob("*.bcif*")
    }
    outcodes = set(outfiles.keys())
    obsoleted = outcodes.difference(infiles)
    removed = []
    filepaths = [outfiles[key] for key in obsoleted if key in outfiles]
    for filepath in filepaths:
        try:
            if filepath.find(updateBase) >= 0:
                os.unlink(filepath)
                removed.append(filepath)
        except Exception as e:
            logger.error(str(e))
    if len(removed) > 0:
        removedFile = os.path.join(listFileBase, removedFileName)
        with open(removedFile, "w", encoding="utf-8") as w:
            for line in removed:
                w.write(line)
                w.write("\n")
    logger.info("removed retracted entries in %.2f s", time.time() - t)
    return True


def tasksDone() -> bool:
    logger.info("task maps completed")
    return True


def statusComplete(listFileBase: str) -> bool:
    """
    must occur after end_task
    """
    statusCompleteFile = "status.complete"
    completeFile = os.path.join(listFileBase, statusCompleteFile)
    dirs = os.path.dirname(completeFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(completeFile, "w", encoding="utf-8") as w:
        w.write(
            "Binary cif run completed successfully at %s."
            % str(datetime.datetime.now())
        )
    return True

