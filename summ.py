import env
import sys
import urllib3
import requests
import getpass
import json
from collections import defaultdict, OrderedDict
import subprocess
import os
import time

# Fix prettytable
sys.path.append('/home/nutanix/ncc/panacea/lib/py/prettytable-0.7.2-py2.7.egg')
from prettytable import PrettyTable

# for Ui
import datetime
import curses
from enum import Enum

# For fetching data
from pithos.client.pithos_client import PithosClient
import cdp.client.curator.client.curator_interface_client as curator_client
from cdp.client.stargate.stargate_interface.stargate_interface_pb2 import *
from cdp.client.stargate.client import StargateClient
from util.interfaces.interfaces import NutanixInterfaces
from stats.arithmos.interface.arithmos_type_pb2 import *
from stats.arithmos.interface.arithmos_interface_pb2 import (
    AgentGetEntitiesArg, MasterGetEntitiesArg)
from serviceability.interface.analytics.arithmos_rpc_client import (
    ArithmosDataProcessing)

# Fix InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Open RPC clients
curator_rpc_client = curator_client.CuratorInterfaceClient()

try:
    get_master_location_arg = curator_client.GetMasterLocationArg()
    get_master_location_ret = curator_rpc_client.GetMasterLocation(get_master_location_arg)
    (curator_ip, curator_port) = str(get_master_location_ret.master_handle).split(":")
except curator_client.CuratorInterfaceError as error:
    print("RPC failed. Couldn't acquire the curator master information. Error: %s" % error)

curator_master_client = curator_client.CuratorInterfaceClient(curator_ip, curator_port)
pithos_client = PithosClient()
pithos_client.initialize()
stargate_client = StargateClient()

# Default values
cluster_ip = '127.0.0.1'
calculate_usage = False
use_cache = True
duration = 5

vms_columns = ["(N)ame", "UUID", "(D)isk size", "NFS (F)iles", "Exclusive (U)sage", "Usage 5 Days (B)efore",
               "(C)urrent Usage"]
snap_columns = ["(N)ame", "Ent(I)ty_name", "(T)ype", "(C)reated Time", "(E)xpired Time", "Reclaimable (S)pace"]


def get_gb(size_in_bytes):
    return round(size_in_bytes / 1024.0 / 1024.0 / 1024.0, 2)


def get_secs(usecs):
    return usecs / 1000 / 1000


def get_data(endpoint, params={}):
    """ rest API result from either cache or v2 API """

    def rest_api_call():
        """ interface to send rest_api call wiht mentioned params """
        try:
            header = {"X-Nutanix-Preauth-User": "admin"}
            url = 'https://%s:9440/PrismGateway/services/rest/v2.0/%s' % (cluster_ip, endpoint)
            return requests.get(url,
                                params=params,
                                # auth=(username, password),
                                headers=header,
                                verify=False
                                ).json()
        except NameError:
            print("Please run the script with skip_cache option first")
            exit(1)

    filename = '/tmp/summ_%s.json' % endpoint.split('/')[0]
    result = {}

    if not use_cache or not os.path.exists(filename):
        result = rest_api_call()
        with open(filename, 'w') as outfile:
            json.dump(result, outfile)
    else:
        with open(filename) as outfile:
            result = json.load(outfile)

    return result


def get_container_id_uuid(container_info):
    """
    Returns container Name container ID and container UUID for a container Arithmos entity..
    Args:
      container_info (arithmos_type_pb2.Container): The container information
        returned by Arithmos.
    Returns
      (int, str): Container ID and UUID.
    """
    for gen_attr in container_info.generic_attribute_list:
        if gen_attr.attribute_name == "container_uuid":
            # Associate container ID to container UUID
            container_name = container_info.container_name
            container_id = int(container_info.id)
            container_uuid = gen_attr.attribute_value_str
            return container_name, container_id, container_uuid
    return None, None, None


def map_ctr_name_to_uuid():
    """
    Queries Arithmos for container info and returns them.
    Returns:
      container_dict{container_name: [container_id, container_uuid}:
    A dictionary that maps from container name to
    container IDs and container UUIDs.
    Raises:
      ArithmosClientError
      RpcClientError
    """
    interfaces = NutanixInterfaces()
    arithmos_client = interfaces.arithmos_client
    container_dict = defaultdict()
    arg = MasterGetEntitiesArg()
    arg.request.entity_type = ArithmosEntityProto.kContainer
    arg.request.include_stats = False
    for container_info in arithmos_client.iter_master_get_entities(arg):
        container_name, container_id, container_uuid = get_container_id_uuid(container_info)
        if container_name is not None and container_id is not None and container_uuid is not None:
            container_dict[container_name] = [container_id, container_uuid]
    return container_dict


def get_container_uuid(container_name):
    """
    Queries map_ctr_name_to_uuid() for container info and returns them.
    Returns:
      container_uuid
    """
    ctr_name_to_uuid = map_ctr_name_to_uuid()

    try:
        container_uuid = ctr_name_to_uuid[container_name][1]
    except KeyError:
        sys.exit("Non existing container name")

    return container_uuid


def get_execution_id_last_successful_scan(curator_master_client):
    """
    Queries curator master rpc for Id of last full scan job.
    Returns:
      Job Number
    """
    err_execution_id = -1
    try:
        query_curator_job_arg = curator_client.QueryCuratorJobArg()
        query_curator_job_ret = curator_master_client.QueryCuratorJob(query_curator_job_arg)
    except curator_client.CuratorInterfaceError as error:
        log.ERROR("RPC failed. Couldn't get curator job information."
                  "Error: %s" % error)
    for index, is_full_scan in enumerate(query_curator_job_ret.full_scan):
        if is_full_scan and query_curator_job_ret.end_time_secs[index] > 0:
            return query_curator_job_ret.execution_id[index]
    return err_execution_id


def get_job_details():
    """
    Queries curator master rpc for Id of last full scan job detail.
    Returns:
      Job details
    """
    execution_id = get_execution_id_last_successful_scan(curator_master_client)
    try:
        query_curator_job_arg = curator_client.QueryMapReduceJobsArg()
        query_curator_job_arg.execution_id_list.append(execution_id)
        query_curator_job_ret = curator_master_client.QueryMapReduceJobs(query_curator_job_arg)
    except curator_client.CuratorInterfaceError as error:
        # Error in rpc call.
        log.ERROR("RPC failed. Couldn't get MapReduce job information."
                  "Error: %s" % error)
        return None
    return query_curator_job_ret


def get_container_garbage_info_for_mapreduce_job():
    """
    Map values from last full scan job detail.
    Returns:
      Job garbage_info_map
    """
    container_counter_map = {
        "ContainerAllReplicasEgroupFallocatedBytes":
            CounterId.kEgroupFallocatedSize,
        "ContainerAllReplicasFallocatedGarbageBytes":
            CounterId.kFallocatedGarbage,
        "ContainerAllReplicasInternalGarbageBytes":
            CounterId.kInternalGarbage,
        "ContainerErasureCodedInternalGarbageBytes":
            CounterId.kEcInternalGarbage,
        "ContainerAllReplicasDeadExtentsGarbageBytes":
            CounterId.kDeadExtentsGarbage,
        "ContainerErasureCodedDeadExtentsGarbageBytes":
            CounterId.kEcDeadExtentsGarbage,
        "ContainerAllReplicasDeadExtentsWithRefGarbageBytes":
            CounterId.kDeadExtentsWithRefGarbage,
        "ContainerErasureCodedDeadExtentsWithRefGarbageBytes":
            CounterId.kEcDeadExtentsWithRefGarbage,
        "ContainerAllReplicasPartialExtentsGarbageBytes":
            CounterId.kPartialExtentsGarbage,
        "ContainerAllReplicasDeadCushionBytes":
            CounterId.kDeadCushionGarbage,
        "ContainerAllReplicasLiveCushionBytes":
            CounterId.kLiveCushionOverhead,
        "ContainerCompressionPreReductionBytes":
            CounterId.kContainerPreCompression,
        "ContainerCompressionPostReductionBytes":
            CounterId.kContainerPostCompression,
        "ContainerErasureCodingPreReductionBytes":
            CounterId.kPreECReduction,
        "ContainerErasureCodingPostReductionBytes":
            CounterId.kPostECReduction,
        "ContainerCompressionSnappyPreReductionBytes":
            CounterId.kSnappyPreReduction,
        "ContainerCompressionSnappyPostReductionBytes":
            CounterId.kSnappyPostReduction,
        "ContainerCompressionLZ4PreReductionBytes":
            CounterId.kLZ4PreReduction,
        "ContainerCompressionLZ4PostReductionBytes":
            CounterId.kLZ4PostReduction,
        "ContainerCompressionLZ4HCPreReductionBytes":
            CounterId.kLZ4HCPreReduction,
        "ContainerCompressionLZ4HCPostReductionBytes":
            CounterId.kLZ4HCPostReduction,
        "ContainerCompressionOthersPreReductionBytes":
            CounterId.kOtherPreReduction,
        "ContainerCompressionOthersPostReductionBytes":
            CounterId.kOtherPostReduction,
        "ContainerAllReplicasSnapshotReclaimableBytes":
            CounterId.kContainerAllReplicasSnapshotReclaimableBytes,
        "ContainerAllReplicasRecycleBinUsageBytes":
            CounterId.kContainerAllReplicasRecycleBinUsageBytes
    }
    query_mapreduce_jobs_ret = get_job_details()
    job_info_proto = query_mapreduce_jobs_ret.mapreduce_job_info[0]
    garbage_info_map = {}
    for values_proto in job_info_proto.counter_values:
        counter_name = values_proto.counter.counter_name
        if not container_counter_map.has_key(counter_name):
            continue
        counter_id = container_counter_map[counter_name]
        for value_proto in values_proto.counter_value:
            if garbage_info_map.has_key(value_proto.subcounter_id):
                counterid_to_value_map = garbage_info_map[value_proto.subcounter_id]
            else:
                counterid_to_value_map = {}
            counterid_to_value_map[counter_id] = value_proto.counter_value
            garbage_info_map[value_proto.subcounter_id] = counterid_to_value_map
    return garbage_info_map


def get_counter_value(counter_id_to_value_map, counter_id):
    if not counter_id_to_value_map.has_key(counter_id):
        return -1
    elif counter_id_to_value_map[counter_id] < 0:
        return -1
    return counter_id_to_value_map[counter_id]


def add_counter_values(values):
    """
    Computes the sum of the given counter 'values'. Negative values are not
    considered. If all values are negative or no arguments are supplied,
    returns -1.
    """

    sum = -1
    for val in values:
        if val >= 0:
            if sum < 0:
                sum = val
            else:
                sum += val
    return sum


def subtract_counter_values(val1, val2):
    """
    Given counter values 'val2' and 'val2' returns their difference
    ('val1' - 'val2') if they are both non-negative. Otherwise returns 'val1'.
    """
    if val1 >= 0 and val2 >= 1:
        return val1 - val2
    return val1


def get_container_info(requested_container_name):
    """ Get info about specific container
    returns [container_name, max_capacity, storage_capacity_bytes]"""

    # source_data = get_data('storage_containers') # replace with get_garbage_stats_ctr from curator_get_data.py

    arithmos_entity_type = ArithmosEntityProto.kContainer
    garbage_info_map = get_container_garbage_info_for_mapreduce_job()
    ctr_name_to_uuid = map_ctr_name_to_uuid()
    ctr_id = ctr_name_to_uuid[requested_container_name][0]
    # result = []
    for container_id, counterid_to_value_map in garbage_info_map.iteritems():
        if ctr_id != container_id:
            continue

        garbage_stats = {
            # 'container_id': container_id,
            'container_thick_reservation':
                get_arithmos_data(str(ctr_id), arithmos_entity_type, 'storage.user_reserved_free_bytes', duration)[-1],
            'fallocated': get_counter_value(counterid_to_value_map, CounterId.kFallocatedGarbage),
            'internal_ec': get_counter_value(counterid_to_value_map, CounterId.kInternalGarbage),
            'internal_nec': subtract_counter_values(
                get_counter_value(counterid_to_value_map, CounterId.kInternalGarbage),
                get_counter_value(counterid_to_value_map, CounterId.kEcInternalGarbage)
            ),
            'deadextent_wo_ref_ec': get_counter_value(counterid_to_value_map, CounterId.kEcDeadExtentsGarbage),
            'deadextent_wo_ref_nec': subtract_counter_values(
                get_counter_value(counterid_to_value_map, CounterId.kDeadExtentsGarbage),
                get_counter_value(counterid_to_value_map, CounterId.kEcDeadExtentsGarbage)
            ),
            'deadextent_w_ref_ec': get_counter_value(counterid_to_value_map, CounterId.kDeadExtentsWithRefGarbage),
            'deadextent_w_ref_nec': subtract_counter_values(
                get_counter_value(counterid_to_value_map, CounterId.kDeadExtentsWithRefGarbage),
                get_counter_value(counterid_to_value_map, CounterId.kEcDeadExtentsWithRefGarbage)
            ),
            'partial_extents': get_counter_value(counterid_to_value_map, CounterId.kPartialExtentsGarbage),
            'dead_cushion': get_counter_value(counterid_to_value_map, CounterId.kDeadCushionGarbage),
            'live_cushion': get_counter_value(counterid_to_value_map, CounterId.kLiveCushionOverhead),
            'total_garbage': add_counter_values(
                [get_counter_value(counterid_to_value_map, CounterId.kFallocatedGarbage),
                 get_counter_value(counterid_to_value_map, CounterId.kInternalGarbage),
                 get_counter_value(counterid_to_value_map, CounterId.kDeadExtentsGarbage),
                 get_counter_value(counterid_to_value_map, CounterId.kPartialExtentsGarbage),
                 get_counter_value(counterid_to_value_map, CounterId.kDeadExtentsWithRefGarbage),
                 get_counter_value(counterid_to_value_map, CounterId.kDeadCushionGarbage)]
            ),
            # 'stargate_reported_garbage': add_counter_values(
            #    [get_counter_value(counterid_to_value_map, CounterId.kFallocatedGarbage),
            #    get_counter_value(counterid_to_value_map, CounterId.kInternalGarbage),
            #    get_counter_value(counterid_to_value_map, CounterId.kDeadCushionGarbage),
            #    get_counter_value(counterid_to_value_map, CounterId.kLiveCushionOverhead)]
            #    ),
            'snapshot_reclaimable_bytes': get_counter_value(counterid_to_value_map,
                                                            CounterId.kContainerAllReplicasSnapshotReclaimableBytes),
            'recyclebin_usage': get_counter_value(counterid_to_value_map,
                                                  CounterId.kContainerAllReplicasRecycleBinUsageBytes)
        }

        _savings_stats = {
            'compression_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kContainerPreCompression),
            'compression_post_reduction': get_counter_value(counterid_to_value_map,
                                                            CounterId.kContainerPostCompression),
            'ec_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kPreECReduction),
            'ec_post_reduction': get_counter_value(counterid_to_value_map, CounterId.kPostECReduction),
            'snappy_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kSnappyPreReduction),
            'snappy_post_reduction': get_counter_value(counterid_to_value_map, CounterId.kSnappyPostReduction),
            'lz4_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kLZ4PreReduction),
            'lz4_post_reduction': get_counter_value(counterid_to_value_map, CounterId.kLZ4PostReduction),
            'lz4hc_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kLZ4HCPreReduction),
            'lz4hc_post_reduction': get_counter_value(counterid_to_value_map, CounterId.kLZ4HCPostReduction),
            'others_pre_reduction': get_counter_value(counterid_to_value_map, CounterId.kOtherPreReduction),
            'others_post_reduction': get_counter_value(counterid_to_value_map, CounterId.kOtherPostReduction)
        }

        savings_stats = {
            'compression': _savings_stats['compression_pre_reduction'] - _savings_stats['compression_post_reduction'],
            'ec': _savings_stats['ec_pre_reduction'] - _savings_stats['ec_post_reduction'],
            'lz4': _savings_stats['lz4_pre_reduction'] - _savings_stats['lz4_post_reduction'],
            'lz4hc': _savings_stats['lz4hc_pre_reduction'] - _savings_stats['lz4hc_post_reduction'],
            'others': _savings_stats['others_pre_reduction'] - _savings_stats['others_post_reduction']
        }

        breakdown = OrderedDict()
        breakdown['replication_factor'] = get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer,
                                                            "data_reduction.replication_factor", duration)
        breakdown['max_capacity'] = "/ ".join([str(_) for _ in
                                               get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer,
                                                                 "storage.user_capacity_bytes", duration)])
        breakdown['free_space_logical_prev_curr'] = "/ ".join([str(_) for _ in get_arithmos_data(str(container_id),
                                                                                                 ArithmosEntityProto.kContainer,
                                                                                                 "storage.user_free_bytes",
                                                                                                 duration)])
        breakdown['storage_prev_curr_usage'] = "/ ".join([str(_) for _ in get_arithmos_data(str(container_id),
                                                                                            ArithmosEntityProto.kContainer,
                                                                                            "storage.user_usage_bytes",
                                                                                            duration)])
        breakdown['reserved_capacity_bytes'] = get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer,
                                                                 "storage.container_reserved_capacity_bytes", duration)[
            0]
        breakdown['savings_on_container'] = "/ ".join([str(_) for _ in get_arithmos_data(str(container_id),
                                                                                         ArithmosEntityProto.kContainer,
                                                                                         "data_reduction.user_saved_bytes",
                                                                                         duration)])
        breakdown['data_reduction_ratio'] = str(
            get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer, "data_reduction.saving_ratio_ppm",
                              duration)) + " : 1"
        breakdown['overall_efficiency'] = str(get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer,
                                                                "data_reduction.overall.saving_ratio_ppm",
                                                                duration)) + " : 1"
        breakdown['clone_usage'] = "/ ".join([str(_) for _ in
                                              get_arithmos_data(str(container_id), ArithmosEntityProto.kContainer,
                                                                "storage.clone_usage_bytes", duration)])

        result = {
            "garbage": OrderedDict(sorted(garbage_stats.items(), key=lambda item: item[1], reverse=True)),
            "savings": OrderedDict(sorted(savings_stats.items(), key=lambda item: item[1], reverse=True)),
            "breakdown": breakdown
        }

        return result


def get_vdisk_name(ndfs_filepath):
    nfs_path_attr_arg = NfsFetchPathAttrArg()
    nfs_path_attr_arg.path = ndfs_filepath
    get_nfs_attr_ret = stargate_client.NfsFetchPathAttr(nfs_path_attr_arg)
    return get_nfs_attr_ret.attr.vdisk_name


def query_vdisk_usage(vdisk_id):
    query_vdisk_usage_arg = curator_client.QueryVDiskUsageArg()
    query_vdisk_usage_arg.vdisk_id_list.append(vdisk_id)
    ret = curator_master_client.QueryVDiskUsage(query_vdisk_usage_arg)
    # print(ret)
    return {
        "live_usage": -1,
        "snapshot_usage": -1,
        "clone_usage": get_gb(ret.logical_shared_clone_usage_bytes[0]),
        "exclusive_usage": get_gb(ret.exclusive_usage_bytes[0])
    }


def get_vdisk_id(vdisk_name_list):
    # Not sure if it works see general workflow in comments below
    keys = [('vdisk_name', vdisk_name, 'data') for vdisk_name in vdisk_name_list]
    entries = pithos_client.lookup(keys)
    config = entries[0]
    vdisk_config = pithos_client.entry_to_vdisk_config(config, include_deleted=True)
    return vdisk_config.vdisk_id if vdisk_config.HasField('vdisk_id') else None


def get_arithmos_data(entity_id, arithmos_entity_type, filter_field, duration=5):
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=duration)
    usec_end = int(end.strftime("%s") + "000000")
    usec_start = int(start.strftime("%s") + "000000")
    ret = ArithmosDataProcessing().MasterGetTimeRangeStats(entity_id, arithmos_entity_type, filter_field, usec_start,
                                                           usec_end, 86400)
    if ret.response_list[0].error == ArithmosErrorProto.kNoError:
        if filter_field == "data_reduction.replication_factor":
            return ret.response_list[0].time_range_stat.value_list[0]
        if filter_field == "data_reduction.saving_ratio_ppm" or filter_field == "data_reduction.overall.saving_ratio_ppm":
            return round(float(ret.response_list[0].time_range_stat.value_list[0]) / 1000000, 2)
        if len(ret.response_list[0].time_range_stat.value_list) > 1:
            return [get_gb(ret.response_list[0].time_range_stat.value_list[0]),
                    get_gb(ret.response_list[0].time_range_stat.value_list[-1])]
        else:
            return [-1, get_gb(int(ret.response_list[0].time_range_stat.value_list[0]))]
    else:
        return [-1, -1]


def get_entity_space_usage_stat(ndfs_filepath):
    """ Execute entity_space_usage_stat on a node
    returns [live_usage, snapshot_usage, clone_usage, exclusive_usage]"""

    # TODO better exeption handling
    vdisk_name = ""
    vdisk_id = 0

    result = {
        'exclusive_usage': -1
    }

    vdisk_name = get_vdisk_name(ndfs_filepath)
    # TODO better exeption handling
    if vdisk_name and vdisk_name != "":
        vdisk_id = get_vdisk_id([vdisk_name])

    if vdisk_id and vdisk_id != 0:
        result = query_vdisk_usage(vdisk_id)

    # TODO revert back if needed
    # return  [result['live_usage'], result['snapshot_usage'],
    #        result['clone_usage'], result['exclusive_usage']]
    return [result['exclusive_usage']]


def get_vms_in_container(requested_container_name):
    """ get info about VMs in the requested_container_name
    returns list of lists like [[vm1 data][vm2 data][vm3 data]]
    where vmX data = [name, size, ndfs_filepath]"""

    result = defaultdict(list)
    source_data = get_data('vms', params={'include_vm_disk_config': 'True'})

    for vm in source_data.get('entities'):
        name = vm.get("name")
        vm_disk_info = vm.get('vm_disk_info')
        vm_id = vm.get("uuid")

        if vm_disk_info is None:
            continue

        for disk in vm_disk_info:

            if disk.get('is_cdrom'):  # or disk.get('is_empty'):
                continue
            if disk['disk_address'].get('volume_group_uuid'):
                continue

            size = float(get_gb(disk.get('size', 0)))
            ndfs_filepath = disk.get('disk_address').get('ndfs_filepath')

            if not ndfs_filepath:
                vmdisk_uuid = disk.get('disk_address').get('vmdisk_uuid')
                vmdisk_config = requests.get(
                    'https://%s:9440/PrismGateway/services/rest/v2.0/virtual_disks/%s' % (cluster_ip, vmdisk_uuid),
                    auth=(username, password), verify=False).json()
                ndfs_filepath = vmdisk_config.get('nutanix_nfsfile_path')

            # We don't expect to be here
            if ndfs_filepath is None:
                print "Can't find nfs_filepath for", name, disk
                break

            container_name = ndfs_filepath.split('/')[1]
            result[container_name].append([name, vm_id, size, ndfs_filepath])

    return result[requested_container_name]


def get_vg_in_container(requested_container_name):
    """ get info about VG in the requested_container_name
    returns list of VGs like [[VG1 data][VG2 data][VG3 data]]
    where VGs data = [name, size, ndfs_filepath]"""

    result = defaultdict(list)
    source_data = get_data('volume_groups')

    for vg in source_data.get("entities"):
        name = "VG " + vg.get("name")
        disk_list = vg.get('disk_list')
        vg_id = vg.get("uuid")

        if disk_list is None:
            continue

        for disk in disk_list:
            size = float(get_gb(disk.get('vmdisk_size_bytes', 0)))

            ndfs_filepath = disk.get('vmdisk_path')
            if not ndfs_filepath:
                vmdisk_uuid = disk.get('vmdisk_uuid')
                vmdisk_config = requests.get(
                    'https://%s:9440/PrismGateway/services/rest/v2.0/virtual_disks/%s' % (cluster_ip, vmdisk_uuid),
                    auth=(username, password), verify=False).json()
                ndfs_filepath = vmdisk_config.get('nutanix_nfsfile_path')

            # We don't expect to be here
            if ndfs_filepath is None:
                print "Can't find nfs_filepath for", name, disk
                break

            container_name = ndfs_filepath.split('/')[1]
            result[container_name].append([name, vg_id, size, ndfs_filepath])

    return result[requested_container_name]


def get_images_in_container(requested_container_name):
    """ get info about images in the requested_container_name
    returns list of images like [[image1 data][image2 data][image3 data]]
    where image1 data = [name, size, ndfs_filepath]"""

    result = defaultdict(list)
    source_data = get_data('images', params={'include_vm_disk_paths': 'True'})

    for image in source_data.get("entities", {}):
        name = "Image " + image.get("name")
        size = float(get_gb(image.get("vm_disk_size", 0)))
        ndfs_filepath = image.get("vm_disk_path")
        if ndfs_filepath is None:
            continue
        container_name = ndfs_filepath.split('/')[1]
        data_range = [-1, -1]
        result[container_name].append([name, "fake_id", size, ndfs_filepath, data_range[0], data_range[1]])

    return result[requested_container_name]


def get_snapshots_in_container(requested_container_uuid):
    """ get info about snapshots in the requested_container_name
    returns list of snapshots like [[snap1 data][snap2 data][snap3 data]]
    where pd1 data = [protection_domain_name, vm_name, type, snapshot_create_time, snapshot_expiry_time]"""

    result = defaultdict(list)
    source_data = get_data('snapshots')

    for snapshot in source_data.get("entities", {}):
        snapshot_name = snapshot.get('snapshot_name', "none")
        vm_name = snapshot["vm_create_spec"]["name"]
        vm_disks = snapshot["vm_create_spec"].get("vm_disks")
        created_time = get_secs(snapshot['created_time'])

        if vm_disks is None:
            continue

        for disk in vm_disks:
            # skip empty cdrom
            if disk.get('is_cdrom') and disk.get('is_empty'):
                continue

            try:
                storage_container_uuid = disk['vm_disk_clone']['storage_container_uuid']
                # last two are Expiration date and Size since they are not available for Acropolis snapshots
                result[storage_container_uuid].append(
                    [snapshot_name, vm_name, 'Acropolis', created_time, 9999999999, -1])
            except KeyError:
                print "Snapshot " + snapshot_name + " created on " + time.ctime(created_time) \
                      + " for vm " + vm_name + " doesn't belong to any existing container"

    return result[requested_container_uuid]


def get_pd_in_container(requested_container_name):
    """
    get info about PD in the requested_container_name
    returns list of pd like [[pd1 data][pd2 data][pd3 data]]
    where pd1 data = [protection_domain_name, vm_name, type, snapshot_create_time, snapshot_expiry_time]"""

    result = defaultdict(list)
    source_data = get_data('protection_domains/dr_snapshots')

    for pd in source_data["entities"]:
        protection_domain_name = pd.get('protection_domain_name')

        protection_domain_size = get_gb(pd.get('exclusive_usage_in_bytes'))
        snapshot_create_time = get_secs(pd['snapshot_create_time_usecs'])
        snapshot_expiry_time = 9999999999
        if pd['snapshot_expiry_time_usecs']:
            snapshot_expiry_time = get_secs(pd['snapshot_expiry_time_usecs'])
        vms = pd.get('vms')
        nfs_files = pd.get('nfs_files')

        volume_groups = pd.get('volume_groups')

        for vm in vms:
            vm_name = vm['vm_name']
            if vm['vm_files'] is None:
                continue

            for ndfs_filepath in vm['vm_files']:
                container_name = ndfs_filepath.split('/')[1]
                result[container_name].append([protection_domain_name, vm_name,
                                               'Protection Domain', snapshot_create_time, snapshot_expiry_time,
                                               protection_domain_size])

        for file in nfs_files:
            container_name = file['nfs_file_path'].split('/')[1]
            result[container_name].append([protection_domain_name, file['nfs_file_path'],
                                           'Protection Domain', snapshot_create_time, snapshot_expiry_time,
                                           protection_domain_size])

        for vg in volume_groups:
            vg_name = vg['name']
            nfs_file_paths = vg['nfs_file_paths']
            for file in nfs_file_paths:
                container_name = file.split('/')[1]
                result[container_name].append([protection_domain_name, vg_name, 'Protection Domain',
                                               snapshot_create_time, snapshot_expiry_time, protection_domain_size])

    return result[requested_container_name]


class CounterId(Enum):
    """
    Counter ids.
    """
    # Total size of egroup files including fallocated space.
    kEgroupFallocatedSize = 1
    # Garbage due to unused fallocated space in an extent group.
    kFallocatedGarbage = 2
    # Internal garbage.
    kInternalGarbage = 3
    # Erasure coded internal garbage.
    kEcInternalGarbage = 4
    # Garbage due to dead extents.
    kDeadExtentsGarbage = 5
    # Erasure coded dead extents garbage.
    kEcDeadExtentsGarbage = 6
    # Garbage due to dead extents which are only referenced by unreachable
    # vdisks.
    kDeadExtentsWithRefGarbage = 7
    # Garbage due to dead extents in erasure coded extent groups that are only
    # referenced by unreachable vdisks.
    kEcDeadExtentsWithRefGarbage = 8
    kPartialExtentsGarbage = 9
    # Garbage due to cushion of immutable extents.
    kDeadCushionGarbage = 10
    # Overhead due to cushion of mutable extents.
    kLiveCushionOverhead = 11
    # ContainerVblockRSnapshotPreReductionBytes
    kContainerPreCompression = 12
    kContainerPostCompression = 13
    kPreECReduction = 14
    kPostECReduction = 15
    kSnappyPreReduction = 16
    kSnappyPostReduction = 17
    kLZ4PreReduction = 18
    kLZ4PostReduction = 19
    kLZ4HCPreReduction = 20
    kLZ4HCPostReduction = 21
    kOtherPreReduction = 22
    kOtherPostReduction = 23
    kContainerAllReplicasSnapshotReclaimableBytes = 24
    kContainerAllReplicasRecycleBinUsageBytes = 25


class VmsVgsImagesTable(object):
    """ Our main strategy to print table with VMs/VGs/Images
    contains headers of a table, collects data in the target_container
    and has a function to print itself"""

    def __init__(self):
        self.init_headers()
        self.load_data()

    def init_headers(self):
        self.headers = vms_columns

    def load_data(self):
        self.data = []

        _vms = get_vms_in_container(target_container)
        self.extend_stats(_vms, ArithmosEntityProto.kVM)
        _vg = get_vg_in_container(target_container)
        self.extend_stats(_vg, ArithmosEntityProto.kVolumeGroup)
        _images = get_images_in_container(target_container)
        for entity in _images:
            # NFS FILES
            ndfs_filepath = entity[3]
            entity.extend(get_entity_space_usage_stat(ndfs_filepath))

        self.data.extend(_vms)
        self.data.extend(_vg)
        self.data.extend(_images)
        # GET /images/{uuid}?include_vm_disk_paths=true doesn't work for 5.10
        # If that is the case - we don't have a way to know ndfs_filepath yet
        # If you don't see any image, check manually via acli.
        # For the rest cases
        # self.data.extend(get_images_in_container(target_container))

    def extend_stats(self, l, proto):
        for entity in l:
            uuid = entity[1]
            ndfs_filepath = entity[3]
            entity.extend(get_entity_space_usage_stat(ndfs_filepath))
            entity.extend(get_arithmos_data(str(uuid), proto, "controller_user_bytes", duration))

    def sort_data(self, sortby, reverse):
        """ Sorting data to display """

        index = {v: k for (k, v) in enumerate(vms_columns)}
        self.data.sort(key=lambda x: x[index[sortby]], reverse=reverse)

    def print_table(self, sortby="(N)ame", reverse=False):
        """ Create and print PrettyTable based on a rule """

        if not self.data:
            return

        self.sort_data(sortby=sortby, reverse=reverse)
        for entity in self.data:
            entity[-1] = str(entity[-1]) + " GiB"
            entity[-2] = str(entity[-2]) + " GiB"
            entity[-3] = str(entity[-3]) + " GiB"
            entity[-5] = str(entity[-5]) + " GiB"

        table = PrettyTable(self.headers)
        for i in self.data:
            table.add_row(i)

        print "\nVM, VG and Images"
        print(table)

    def export_data(self, sortby="(N)ame", reverse=False):

        if not self.data:
            return []

        self.sort_data(sortby=sortby, reverse=reverse)
        return self.data


class SnapshotsAndPdTable(object):
    """ Our main strategy to print table with Snapshots/PDs
    contains headers of a table, collects data in the target_container
    and has a function to print itself"""

    def __init__(self):
        self.init_headers()
        self.load_data()

    def init_headers(self):
        self.headers = snap_columns

    def load_data(self):
        self.data = []

        ctr_name_to_uuid = map_ctr_name_to_uuid()

        try:
            target_container_uuid = ctr_name_to_uuid[target_container][1]
        except KeyError:
            sys.exit("Non existing container name")

        self.data.extend(get_snapshots_in_container(target_container_uuid))
        self.data.extend(get_pd_in_container(target_container))

    def sort_data(self, sortby, reverse):
        """ Sorting data to display """
        index = {v: k for (k, v) in enumerate(snap_columns)}
        self.data.sort(key=lambda x: x[index[sortby]], reverse=reverse)

    def print_table(self, sortby="(C)reated Time", reverse=False):

        if not self.data:
            return

        self.sort_data(sortby=sortby, reverse=reverse)
        # Change time in usec to human-readable time
        for entity in self.data:
            entity[-3] = time.ctime(entity[-3])
            entity[-2] = time.ctime(entity[-2])
            entity[-1] = str(entity[-1]) + " GiB"

        table = PrettyTable(self.headers)

        for i in self.data:
            table.add_row(i)
        print "\nSnapshots and PD"
        print(table)

    def export_data(self, sortby="(C)reated Time", reverse=False):

        if not self.data:
            return []

        self.sort_data(sortby=sortby, reverse=reverse)
        return self.data


class ContainerStatsTable(object):
    """ Parent class which contains logic for load data and headers. Not implemented yet """

    def __init__(self):
        self.init_headers()
        self.load_data()

    def init_headers(self):
        self.headers = ["Container-level garbage/Overhead", "Values", "Savings", "Diff", "Overall", "Stats"]

    def sort_data(self, sortby, reverse):
        """ Sorting data to display """
        index = {v: k for (k, v) in enumerate(snap_columns)}
        self.data.sort(key=lambda x: x[index[sortby]], reverse=reverse)

    def load_data(self):

        self.container_info = {}

        self.container_info = get_container_info(target_container)
        # replacing savings_stats.
        # self.data =  self.container_info["garbage"] #{garbage:x, peg:x, savings:x}
        self.data = []

        for type_k in self.container_info.keys():
            if type_k == "breakdown":
                continue
            for k, v in self.container_info[type_k].items():
                if k == "container_id":
                    continue
                self.container_info[type_k][k] = get_gb(v)

    def print_table(self, sortby="Diff", reversesort=True):

        if not self.container_info:
            return

        """ Create and print PrettyTable based on a rule """
        table = PrettyTable(self.headers)

        info = [
            self.container_info.get("garbage").items(),
            self.container_info.get("savings").items(),
            self.container_info.get("breakdown").items()
        ]

        max_len = max(len(info[0]), len(info[1]), len(info[2]))

        for i in info:
            while len(i) < max_len:
                i.append(("", ""))

        self.data = zip(info[0], info[1], info[2])
        _list = []
        for row in self.data:
            t = []
            for r in row:
                t.extend(list(r))
            _list.append(t)

        self.data = _list

        for row in self.data:
            table.add_row(row)

            # table.reversesort = reversesort
        # table.sortby = sortby

        print(table)

    def export_data(self, type="garbage"):

        if not self.container_info:
            return []
        # Return dummy values
        return self.container_info.get(type)


class Ui(object):
    """Display base"""

    def __init__(self):

        # Initializing objects for pads
        # We will have 2 of them atm
        # Curator pad should be here as well
        self.container_stats = ContainerStatsTable()
        self.vms_usage = VmsVgsImagesTable()
        self.snap_usage = SnapshotsAndPdTable()

    def time_validator(self, start_time, end_time,
                       sec=None):
        """
        Check start_time, end_time and sec are valid. Returns sec or a valid
        value for sec if possible, if there is no valid value for sec returns -1.
        """
        if start_time >= end_time:
            parser.print_usage()
            print("ERROR: Invalid date: Start time must be before end time")
            return -1

        if ((end_time - start_time).days < 1
                and (end_time - start_time).seconds < 30):
            parser.print_usage()
            print("ERROR: Invalid dates: difference between start and "
                  "end is less than 30 seconds.\n"
                  "       Minimum time difference for historic report is 30 seconds.")
            return -1
        elif not sec:
            print("INFO: Not interval indicated, setting "
                  "interval to 60 seconds.")
            sec = 60
        elif sec < 30:
            print("INFO: Invalid interval: minimum value 30 seconds for "
                  "historic report. \n"
                  "      Setting interval to 30 seconds.")
            sec = 30

        delta_time = start_time + datetime.timedelta(seconds=sec)
        if delta_time > end_time:
            sec = int(end_time.strftime("%s")) - int(start_time.strftime("%s"))
            print("INFO: Invalid interval: greater than the difference "
                  "between start and end time.\n"
                  "      Setting interval to difference between start and end."
                  " Interval = " + str(sec))
            return sec
        return sec


class UiInteractive(Ui):
    """Interactive interface"""

    def __init__(self):
        Ui.__init__(self)

        self.title = " SUMM "
        self.stdscr = curses.initscr()
        self.screenh, self.screenw = self.stdscr.getmaxyx()
        self.vms_count = len(self.vms_usage.export_data())
        self.snap_count = len(self.snap_usage.export_data())
        # Initializing pads
        # Replace 387 with some meaningful
        self.container_stats_pad = curses.newpad(19, self.screenw / 3)
        self.container_stats_pad.border()

        self.container_savings_pad = curses.newpad(19, self.screenw / 3)
        self.container_savings_pad.border()

        self.container_peg_pad = curses.newpad(19, self.screenw / 3)
        self.container_peg_pad.border()

        self.vms_pad = curses.newpad(self.vms_count + 3, self.screenw)
        self.vms_pad.border()

        self.snap_pad = curses.newpad(self.snap_count + 3, self.screenw)
        self.snap_pad.border()

        self.overall_pad = curses.newpad(self.vms_count + self.snap_count + 3, self.screenw)
        self.overall_pad.border()

        # Have it looks like NARF
        self.initialize_colors()

        self.key = 0
        self.main_pad = "vms"
        self.height = 0
        self.width = 0
        self.pos = 0
        self.vms_pad_sortby = "Exclusive (U)sage"
        self.reverse = True
        self.snap_pad_sortby = "Reclaimable (S)pace"
        self.overall_pad_sortby = "Exclusive (U)sage"

    def initialize_colors(self):
        # Color pair constants
        self.RED = 1
        self.GREEN = 2
        self.YELLOW = 3
        self.BLUE = 4
        self.MAGENTA = 5
        self.CYAN = 6
        self.WHITE_BLACK = 7
        self.BLACK_WHITE = 8

        # Start colors in curses
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(self.RED, curses.COLOR_RED, -1)
        curses.init_pair(self.GREEN, curses.COLOR_GREEN, -1)
        curses.init_pair(self.YELLOW, curses.COLOR_YELLOW, -1)
        curses.init_pair(self.BLUE, curses.COLOR_BLUE, -1)
        curses.init_pair(self.MAGENTA, curses.COLOR_MAGENTA, -1)
        curses.init_pair(self.CYAN, curses.COLOR_CYAN, -1)
        curses.init_pair(self.WHITE_BLACK, curses.COLOR_WHITE,
                         curses.COLOR_BLACK)
        curses.init_pair(self.BLACK_WHITE, curses.COLOR_BLACK,
                         curses.COLOR_WHITE)

    def safe_noautorefresh(self, pad,
                           pad_min_y, pad_min_x,
                           screen_y, screen_x,
                           pad_desired_height, pad_desired_width):
        """
        It safely display a pad without going beyond window boundaries.
        Avoid to crash if window is resized.
        TODO:
          + For the time being it assume the pad is displayed from 0, 0
            pad position. This means it will always receive pad_min_y
            and pad_min_x with 0 value. Other values may make this
            function to crash.
        """
        main_screen_max_absolute_y = screen_y + pad_desired_height
        main_screen_max_absolute_x = screen_x + pad_desired_width

        if self.height < main_screen_max_absolute_y:
            main_screen_max_absolute_y = self.height - 2

        if self.width < main_screen_max_absolute_x:
            main_screen_max_absolute_x = self.width - 2

        pad.noutrefresh(pad_min_y, pad_min_x,
                        screen_y, screen_x,
                        main_screen_max_absolute_y, main_screen_max_absolute_x)

    # Ignore get_nodes_sort_label
    # Ignore get_vm_sort_label
    # Ignore toggle_nodes_pad
    # Ignore toggle_active_node
    def handle_key_press(self):
        self.key = self.stdscr.getch()
        # self.nodes_sort = self.get_nodes_sort_label(self.key)
        # self.vm_sort = self.get_vm_sort_label(self.key)
        self.toggle_main_pad(self.key)
        # self.toggle_active_node(self.key)
        self.scroll_main_pad(self.key)
        self.vms_pad_sort(self.key)
        self.snap_pad_sort(self.key)

    def toggle_main_pad(self, toggle_key):
        if toggle_key == ord('v'):
            self.main_pad = "vms"
            self.pos = 0
        if toggle_key == ord('s'):
            self.main_pad = "snapshots"
            self.pos = 0
        if toggle_key == ord('o'):
            self.main_pad = "overall_pad"
            self.pos = 0

    def scroll_main_pad(self, toggle_key):
        if toggle_key == curses.KEY_DOWN:
            self.pos += 10
        elif toggle_key == curses.KEY_UP:
            self.pos -= 10

    def vms_pad_sort(self, toggle_key):
        if self.main_pad != "vms":
            return
        keymap = {
            ord('N'): "(N)ame",
            ord('D'): "(D)isk size",
            ord('F'): "NFS (F)iles",
            ord('U'): "Exclusive (U)sage",
            ord('B'): "Usage 5 Days (B)efore",
            ord('C'): "(C)urrent Usage"
        }
        column = keymap.get(toggle_key)
        if not column:
            return
        if self.vms_pad_sortby != column:
            self.vms_pad_sortby = column
        else:
            self.reverse = not self.reverse

    def snap_pad_sort(self, toggle_key):
        if self.main_pad != "snapshots":
            return
        keymap = {
            ord('N'): "(N)ame",
            ord('I'): "Ent(I)ty_name",
            ord('T'): "(T)ype",
            ord('C'): "(C)reated Time",
            ord('E'): "(E)xpired Time",
            ord('S'): "Reclaimable (S)pace"
        }

        column = keymap.get(toggle_key)
        if not column:
            return
        if self.snap_pad_sortby != column:
            self.snap_pad_sortby = column
        else:
            self.reverse = not self.reverse

    # Have it looks like NARF
    def render_header(self):
        # Turning on attributes for title
        self.stdscr.attron(curses.color_pair(self.RED))
        self.stdscr.attron(curses.A_BOLD)

        # Rendering title
        self.stdscr.addstr(0, 5, self.title)
        self.stdscr.addstr(0, self.width - 12,
                           datetime.datetime.now().strftime(" %H:%M:%S "))

        # Turning off attributes for title
        self.stdscr.attroff(curses.color_pair(self.RED))
        self.stdscr.attroff(curses.A_BOLD)

    # Actual render of pads with data
    def render_container_stats_pad(self, y, x, type):

        padtypemap = {
            "garbage": self.container_stats_pad,
            "savings": self.container_savings_pad,
            "breakdown": self.container_peg_pad
        }
        _pad = padtypemap.get(type)

        self.stdscr.noutrefresh()
        pad_size_y, pad_size_x = _pad.getmaxyx()

        _pad.attron(curses.A_BOLD)
        _pad.addstr(0, 3, " Container {0} {1} stats ".format(target_container, type))
        _pad.attroff(curses.A_BOLD)

        for i, (stat, value) in enumerate(self.container_stats.export_data(type).items()):
            if stat == "data_reduction_ratio" or stat == "replication_factor" or stat == "overall_efficiency":
                _pad.addstr(i + 2, 1, "{0:<40} {1:>20}".format(stat, value))
            else:
                _pad.addstr(i + 2, 1, "{0:<40} {1:>20}".format(stat, str(value) + " GiB"))

        self.safe_noautorefresh(_pad, 0, 0, y, x,
                                pad_size_y, pad_size_x)

        return (y + pad_size_y, pad_size_x + x)

    def render_overall_pad(self, y, x, pos=0):
        # Same as vms+snapshots but we drop some of column to sort per usage

        self.stdscr.noutrefresh()
        pad_size_y, pad_size_x = self.overall_pad.getmaxyx()

        self.overall_pad.attron(curses.A_BOLD)
        self.overall_pad.addstr(0, 3, " Overall usage ")
        self.overall_pad.attroff(curses.A_BOLD)

        # Line on right side of a border
        # self.vms_pad.addstr(0, pad_size_x - 15, " Sort: {0:<4} "
        #                          .format(self.nodes_sort))

        self.overall_pad.attron(curses.A_BOLD)
        self.overall_pad.addstr(1, 1, "{0:<60} {1:<20} {2:<20}"
                                .format("Name",
                                        "Type",
                                        "Exclusive/Reclaimable Usage"))
        self.overall_pad.attroff(curses.A_BOLD)

        # WTF is going on
        overall = [[e[0], "VM", e[5]] for e in self.vms_usage.export_data()]
        overall.extend([[e[0], "Snapshot", e[5]] for e in self.snap_usage.export_data()])

        # No sorting for overall tab atm
        overall.sort(key=lambda x: x[2], reverse=True)

        for i, entity in enumerate(overall):
            self.overall_pad.addstr(i + 2, 1, "{0:<60} {1:<20} {2:<20}"
                                    .format(entity[0][:60],
                                            entity[1],
                                            str(entity[2]) + " GiB")
                                    )

        max_pos = len(overall)
        self.safe_noautorefresh(self.overall_pad, pos if pos < max_pos else max_pos, 0, y, x,
                                pad_size_y, pad_size_x)
        return y + pad_size_y

    def render_vms_pad(self, y, x, pos=0):

        self.stdscr.noutrefresh()
        pad_size_y, pad_size_x = self.vms_pad.getmaxyx()

        self.vms_pad.attron(curses.A_BOLD)
        self.vms_pad.addstr(0, 3, " VMs/Images/VG usage. Total {0} ".format(self.vms_count))
        # self.vms_pad.addstr(0, self.width - 40, "Cluster wide utilization")
        self.vms_pad.attroff(curses.A_BOLD)

        # Line on right side of a border
        # self.vms_pad.addstr(0, pad_size_x - 15, " Sort: {0:<4} "
        #                          .format(self.nodes_sort))

        self.vms_pad.attron(curses.A_BOLD)
        self.vms_pad.addstr(1, 1, "{0:<60} {1:<11} {2:<80} {3:<20} {4:<25} {5:<25}"
                            .format("(N)ame", "(D)isk size", "NFS (F)iles", "Exclusive (U)sage",
                                    "Usage 5 Days (B)efore",
                                    "(C)urrent Usage"))
        self.vms_pad.attroff(curses.A_BOLD)
        # ["Name", "UUID", "Disk size", "NFS Files", "Exclusive Usage", "Usage 5 days ago", "Current Usage"]

        for i, entity in enumerate(self.vms_usage.export_data(sortby=self.vms_pad_sortby, reverse=self.reverse)):
            self.vms_pad.addstr(i + 2, 1, "{0:<60} {1:<11} {2:<80} {3:<20} {4:<25} {5:<25}"
                                .format(entity[0][:60],
                                        str(entity[2]) + " GiB",
                                        entity[3],
                                        entity[4],
                                        str(entity[5]) + " GiB",
                                        str(entity[6]) + " GiB"
                                        ))

        max_pos = len(self.vms_usage.export_data())
        self.safe_noautorefresh(self.vms_pad, pos if pos < max_pos else max_pos, 0, y, x,
                                pad_size_y, pad_size_x)
        return y + pad_size_y

    def render_snap_pad(self, y, x, pos):

        self.stdscr.noutrefresh()
        pad_size_y, pad_size_x = self.snap_pad.getmaxyx()

        self.snap_pad.attron(curses.A_BOLD)
        self.snap_pad.addstr(0, 3, " Snapshots/PD usage. Total {0} ".format(self.snap_count))
        self.snap_pad.attroff(curses.A_BOLD)

        # Line on right side of a border
        # self.vms_pad.addstr(0, pad_size_x - 15, " Sort: {0:<4} "
        #                          .format(self.nodes_sort))

        self.snap_pad.attron(curses.A_BOLD)
        self.snap_pad.addstr(1, 1, "{0:<60} {1:<60} {2:<20} {3:<25} {4:<25} {5:<20}"
                             .format(*snap_columns))
        self.snap_pad.attroff(curses.A_BOLD)

        for i, entity in enumerate(self.snap_usage.export_data(sortby=self.snap_pad_sortby, reverse=self.reverse)):
            # Fix time to human-readable format via time.ctime
            self.snap_pad.addstr(i + 2, 1, "{0:<60} {1:<60} {2:<20} {3:<25} {4:<25} {5:<20}"
                                 .format(entity[0][:60],
                                         entity[1][:60],
                                         entity[2],
                                         time.ctime(entity[3]),
                                         time.ctime(entity[4]),
                                         str(entity[5]) + " GiB"
                                         ))

        max_pos = len(self.snap_usage.export_data())
        self.safe_noautorefresh(self.snap_pad, pos if pos < max_pos else max_pos, 0, y, x,
                                pad_size_y, pad_size_x)
        return y + pad_size_y

    def render_main_screen(self, stdscr):

        self.stdscr.clear()
        # self.stdscr.nodelay(1)
        curses.curs_set(0)

        refresh_time = datetime.datetime.now() - datetime.timedelta(0, 2)
        while (self.key != ord('q')):

            self.handle_key_press()

            if refresh_time < datetime.datetime.now() or self.key != -1:
                current_y_position, current_x_position = 2, 1

                # Initialization
                self.stdscr.clear()
                self.height, self.width = stdscr.getmaxyx()
                self.stdscr.border()

                self.render_header()

                # Display nodes pad #BROKEN
                if current_y_position < self.height:
                    # WTF
                    _current_y_position = current_y_position
                    _dummy_y, current_x_position = self.render_container_stats_pad(_current_y_position,
                                                                                   current_x_position, "garbage")
                    _dummy_y, current_x_position = self.render_container_stats_pad(_current_y_position,
                                                                                   current_x_position, "savings")
                    current_y_position, current_x_position = self.render_container_stats_pad(_current_y_position,
                                                                                             current_x_position,
                                                                                             "breakdown")

                # Display VMs pad
                if current_y_position < self.height - 2:
                    if self.main_pad == "vms":
                        current_y_position = self.render_vms_pad(
                            current_y_position, 1, self.pos)
                    elif self.main_pad == "snapshots":
                        current_y_position = self.render_snap_pad(
                            current_y_position, 1, self.pos)
                    elif self.main_pad == "overall_pad":
                        current_y_position = self.render_overall_pad(
                            current_y_position, 1, self.pos)
                # Refresh the screen
                self.stdscr.noutrefresh()

                # Stage all updates
                curses.doupdate()

                # Calculate time for next screen refresh.
                # TODO: Enable hability to change refresh rate.
                refresh_time = datetime.datetime.now() + datetime.timedelta(0, 3)


if __name__ == '__main__':

    target_container = sys.argv[1]

    if len(sys.argv) > 2:
        use_cache = False if sys.argv[2] == 'skip_cache' else True
        calculate_usage = True if sys.argv[2] == 'calculate_usage' else False
        ui = True if sys.argv[2] == 'ui' else False
        if len(sys.argv) > 3:
            try:
                duration = int(sys.argv[3])
            except ValueError as e:
                print(e)
                print("Please Input valid number of days")
                exit(0)

    # if not use_cache:
    #    username = raw_input('Prism Element Username: ')
    #    password = getpass.getpass('Prism Element Password: ')

    if not ui:
        # MainContainerRule().print_table()
        ContainerStatsTable().print_table()
        VmsVgsImagesTable().print_table("Exclusive (U)sage", reverse=True)
        SnapshotsAndPdTable().print_table("Reclaimable (S)pace", reverse=True)
    else:
        ui_interactive = UiInteractive()
        curses.wrapper(ui_interactive.render_main_screen)
