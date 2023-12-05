from prefect import flow, task, get_run_logger
from tiled.client import from_profile
from tiled.structures.table import TableStructure
from tpx3utils import extract_fpaths_from_sid, raw_to_sorted_df
import os

tiled_client = from_profile("nsls2")["chx"]
tiled_client_chx = tiled_client["raw"]
# tiled_client_sandbox = tiled_client["sandbox"]
tiled_client_processed = tiled_client["processed"]

def get_df_uncent(run):
    raw_file_paths = run['primary']['data']['tpx3_files_raw_filepaths'][0]
    for file in raw_file_paths:
        if (os.path.exists(file)):
            yield raw_to_sorted_df(file)

def insert_to_tiled(container, run):
    structure = None
    node = None
    num_img = run['primary'].metadata['descriptors'][0]['configuration']['tpx3']['data']['tpx3_cam_num_images']
    
    node = container.create(key=run.start['uid'], metadata={"raw_uid": run.start['uid'], "raw_sid": run.start['scan_id']})
    raw_node = None
    cent_node = None

    for partition_num, df in enumerate(get_df_uncent(run)):
        if (structure == None):
            structure = TableStructure.from_pandas(df)
            structure.npartitions = num_img
            # structure2 = TableStructure.from_pandas(df)
            # structure2.npartitions = num_img
            # structs = [structure1, structure2]
            cent_node = node.new("table", structure=structure, key="cent")
            raw_node = node.new("table", structure=structure, key="raw")
            # node = container.new("table", structure=structure, key=run.start['uid'], metadata={"raw_uid": run.start['uid'], "raw_sid": run.start['scan_id']})
        
        raw_node.write_partition(df, partition_num)
        cent_node.write_partition(df, partition_num)
        
    

@task
def process_run(ref):
    """
    Do processing on a BlueSky run.

    Parameters
    ----------
    ref : int, str
        reference to BlueSky. It can be scan_id, uid or index
    """
    
    print("REFERENCE: ")
    print(ref)
    logger = get_run_logger()
    # Grab the BlueSky run
    run = tiled_client_chx[ref]
    
    # Grab the full uid for logging purposes
    full_uid = run.start["uid"]
    logger.info(f"{full_uid = }")
    logger.info("Do something with this uid")
    
    insert_to_tiled(tiled_client_processed, run)


@flow
def processing_flow(ref):
    """
    Prefect flow to do processing on a BlueSky run.

    Parameters
    ----------
    ref : int, str
        reference to BlueSky. It can be scan_id, uid or index
    """
    run = tiled_client_chx[ref]
    
    if (run.start['detectors'][0] == 'tpx3'):
        process_run(ref)
