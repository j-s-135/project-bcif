import os
import glob
from hydra import compose, initialize
from bcif.task_functions import *

if __name__ == "__main__":

    config_path = "config"
    config_file = "config.yaml"
    initialize(version_base=None, config_path=config_path)
    params = compose(config_name=config_file)

    route = int(params.settings.route)
    incremental_update = bool(params.settings.incremental_update)
    out_file_suffix = params.settings.out_file_suffix
    num_sublist_files = int(params.settings.num_sublist_files)
    output_content_type = bool(params.settings.output_content_type)
    output_hash = bool(params.settings.output_hash)
    input_hash = bool(params.settings.input_hash)
    batch_size = int(params.settings.batch_size)
    nfiles = int(params.settings.nfiles)
    config_path = params.paths.config_path
    list_file_base = params.paths.listFileBase
    output_path = params.paths.outputPath
    pdb_remote_path = params.urls.pdbRemotePath
    csm_remote_path = params.urls.csmRemotePath
    pdb_holdings_path = params.urls.pdbHoldingsFilePath
    csm_holdings_path = params.urls.csmHoldingsFilePath

    statusStart(list_file_base)

    makeDirs(list_file_base, output_path, output_content_type)

    splitRemoteTaskLists(pdb_holdings_path, csm_holdings_path, list_file_base, output_path, incremental_update, out_file_suffix, num_sublist_files, config_path, output_content_type, output_hash)

    filepaths = getListFiles(list_file_base, "pdb")

    for filepath in glob.glob(os.path.join(list_file_base, "pdbx_core_ids-*.txt")):
        list_file_name = os.path.basename(filepath)
        content_type = "pdb"
        computeBcif(list_file_name, list_file_base, pdb_remote_path, output_path, out_file_suffix, content_type, output_content_type, output_hash, input_hash, batch_size, nfiles)

    validateOutput(listFileBase=list_file_base, updateBase=output_path, outfileSuffix=out_file_suffix, outputContentType=output_content_type, outputHash=output_hash)

    removeRetractedEntries(listFileBase=list_file_base, updateBase=output_path, outputContentType=output_content_type, outputHash=output_hash)

    statusComplete(list_file_base)


