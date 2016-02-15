#!/usr/bin/python

import os
import re
import json
import time
import classad
import datetime
import htcondor
import elasticsearch

string_vals = set([ \
  "Args",
  "AccountingGroup",
  "Cmd",
  "CMSGroups",
  "CMS_JobType",
  "DESIRED_Archs",
  "DESIRED_CMSDataLocations",
  "DESIRED_CMSDataset",
  "DESIRED_Sites",
  "ExtDESIRED_Sites",
  "GlobalJobId",
  "LastRemoteHost",
  "MATCH_EXP_JOB_GLIDECLIENT_Name",
  "MATCH_EXP_JOB_GLIDEIN_ClusterId",
  "MATCH_EXP_JOB_GLIDEIN_CMSSite",
  "MATCH_EXP_JOB_GLIDEIN_Entry_Name",
  "MATCH_EXP_JOB_GLIDEIN_Factory",
  "MATCH_EXP_JOB_GLIDEIN_Name",
  "MATCH_EXP_JOB_GLIDEIN_Schedd",
  "MATCH_EXP_JOB_GLIDEIN_SEs",
  "MATCH_EXP_JOB_GLIDEIN_Site",
  "MATCH_EXP_JOB_GLIDEIN_SiteWMS",
  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_JobId",
  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Queue",
  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Slot",
  "Owner",
  "RemoteHost",
  "REQUIRED_OS",
  "ShouldTransferFiles",
  "StartdIpAddr",
  "StartdPrincipal",
  "User",
  "WhenToTransferOutput",
  "WMAgent_AgentName",
  "WMAgent_RequestName",
  "WMAgent_SubTaskName",
  "x509UserProxyEmail",
  "x509UserProxyFirstFQAN",
  "x509UserProxyFQAN",
  "x509userproxysubject",
  "x509UserProxyVOName",
  "InputData",
  "Original_DESIRED_Sites",
  "WMAgent_TaskType",
  "Campaign",
  "TaskType",
  "DataLocations",
  "Workflow",
  "Site",
  "Tier",
  "Country",
])

int_vals = set([ \
  "BytesRecvd",
  "BytesSent",
  "ClusterId",
  "CommittedSlotTime",
  "CumulativeSlotTime",
  "CumulativeSuspensionTime",
  "CurrentHosts",
  "DelegatedProxyExpiration",
  "DiskUsage_RAW",
  "ExecutableSize_RAW",
  "ExitStatus",
  "ImageSize_RAW",
  "JobPrio",
  "JobRunCount",
  "JobStatus",
  "JobUniverse",
  "LastJobStatus",
  "LocalSysCpu",
  "LocalUserCpu",
  "MachineAttrCpus0",
  "MachineAttrSlotWeight0",
  "MATCH_EXP_JOB_GLIDEIN_Job_Max_Time",
  "MATCH_EXP_JOB_GLIDEIN_MaxMemMBs",
  "MATCH_EXP_JOB_GLIDEIN_Max_Walltime",
  "MATCH_EXP_JOB_GLIDEIN_Memory",
  "MATCH_EXP_JOB_GLIDEIN_ProcId",
  "MATCH_EXP_JOB_GLIDEIN_ToDie",
  "MATCH_EXP_JOB_GLIDEIN_ToRetire",
  "MaxHosts",
  "MaxWallTimeMins_RAW",
  "MemoryUsage",
  "MinHosts",
  "NumJobMatches",
  "NumJobStarts",
  "NumRestarts",
  "NumShadowStarts",
  "NumSystemHolds",
  "PostJobPrio1",
  "PostJobPrio2",
  "ProcId",
  "Rank",
  "RecentBlockReadKbytes",
  "RecentBlockReads",
  "RecentBlockWriteKbytes",
  "RecentBlockWrites",
  "RemoteSlotID",
  "RemoteSysCpu",
  "RemoteUserCpu",
  "RemoteWallClockTime",
  "RequestCpus",
  "RequestDisk_RAW",
  "RequestMemory_RAW",
  "ResidentSetSize_RAW",
  "StatsLifetimeStarter",
  "TotalSuspensions",
  "TransferInputSizeMB",
  "WallClockCheckpoint",
  "WMAgent_JobID",
])

date_vals = set([ \
  "CompletionDate",
  "EnteredCurrentStatus",
  "JobCurrentStartDate",
  "JobCurrentStartExecutingDate",
  "JobLastStartDate",
  "JobStartDate",
  "LastMatchTime",
  "LastSuspensionTime",
  "LastVacateTime_RAW",
  "QDate",
  "ShadowBday",
  "JobFinishedHookDone",
  "LastJobLeaseRenewal",
  "GLIDEIN_ToDie",
  "GLIDEIN_ToRetire",
])

ignore = set([
  "accounting_group",
  "AcctGroup",
  "AcctGroupUser",
  "AllowOpportunistic",
  "AutoClusterAttrs",
  "AutoClusterId",
  "BufferBlockSize",
  "BufferSize",
  "CondorPlatform",
  "CondorVersion",
  "DiskUsage",
  "Err",
  "Environment",
  "ExecutableSize",
  "HasBeenRouted",
  "HasPrioCorrection",
  "ImageSize",
  "In",
  "Iwd",
  "JobAdInformationAttrs",
  "JOB_GLIDECLIENT_Name",
  "JOB_GLIDEIN_ClusterId",
  "JOB_GLIDEIN_CMSSite",
  "JOBGLIDEIN_CMSSite",
  "JOB_GLIDEIN_Entry_Name",
  "JOB_GLIDEIN_Factory",
  "JOB_GLIDEIN_Job_Max_Time",
  "JOB_GLIDEIN_MaxMemMBs",
  "JOB_GLIDEIN_Max_Walltime",
  "JOB_GLIDEIN_Memory",
  "JOB_GLIDEIN_Name",
  "JOB_GLIDEIN_ProcId",
  "JOB_GLIDEIN_Schedd",
  "JOB_GLIDEIN_SEs",
  "JOB_GLIDEIN_Site",
  "JOB_GLIDEIN_SiteWMS",
  "JOB_GLIDEIN_SiteWMS_JobId",
  "JOB_GLIDEIN_SiteWMS_Queue",
  "JOB_GLIDEIN_SiteWMS_Slot",
  "JOB_GLIDEIN_ToDie",
  "JOB_GLIDEIN_ToRetire",
  "JobLeaseDuration",
  "JobNotification",
  "JOB_Site",
  "MATCH_EXP_JOBGLIDEIN_CMSSite",
  "MATCH_EXP_JOB_Site",
  "MATCH_GLIDECLIENT_Name",
  "MATCH_GLIDEIN_ClusterId",
  "MATCH_GLIDEIN_CMSSite",
  "MATCH_GLIDEIN_Entry_Name",
  "MATCH_GLIDEIN_Factory",
  "MATCH_GLIDEIN_Job_Max_Time",
  "MATCH_GLIDEIN_MaxMemMBs",
  "MATCH_GLIDEIN_Max_Walltime",
  "MATCH_GLIDEIN_Name",
  "MATCH_GLIDEIN_ProcId",
  "MATCH_GLIDEIN_Schedd",
  "MATCH_GLIDEIN_SEs",
  "MATCH_GLIDEIN_Site",
  "MATCH_GLIDEIN_SiteWMS",
  "MATCH_GLIDEIN_SiteWMS_JobId",
  "MATCH_GLIDEIN_SiteWMS_Queue",
  "MATCH_GLIDEIN_SiteWMS_Slot",
  "MATCH_GLIDEIN_ToDie",
  "MATCH_GLIDEIN_ToRetire",
  "MATCH_Memory",
  "MyType",
  "NiceUser",
  "NumCkpts",
  "NumCkpts_RAW",
  "OnExitHold",
  "OnExitRemove",
  "OrigMaxHosts",
  "Out",
  "PeriodicHold",
  "PeriodicRelease",
  "PeriodicRemove",
  "Prev_DESIRED_Sites",
  "PublicClaimId",
  "RequestDisk",
  "RequestMemory",
  "ResidentSetSize",
  "REQUIRES_LOCAL_DATA",
  "RecentBlockReadKbytes",
  "RecentBlockReads",
  "RecentBlockWriteKbytes",
  "RecentBlockWrites",
  "RootDir",
  "ServerTime",
  "SpooledOutputFiles",
  "StreamErr",
  "StreamOut",
  "TargetType",
  "TransferIn",
  "TransferInput",
  "TransferOutput",
  "UserLog",
  "UserLogUseXML",
  "use_x509userproxy",
  "x509userproxy",
  "x509UserProxyExpiration",
  "WantCheckpoint",
  "WantRemoteIO",
  "WantRemoteSyscalls",
  "BlockReadKbytes",
  "BlockReads",
  "BlockWriteKbytes",
  "BlockWrites",
  "LocalSysCpu",
  "LeaveJobInQueue",
  "LocalUserCpu",
  "JobMachineAttrs",
  "LastRejMatchReason",
  "MachineAttrGLIDEIN_CMSSite0",
  "CMS_ALLOW_OVERFLOW",
  "LastPublicClaimId",
  "LastRemotePool",
])

no_idx = set([
  "Args",
  "BytesRecvd",
  "Cmd",
  "CondorPlatform",
  "CondorVersion",
  "CoreSize",
  "CurrentHosts",
  "DelegatedProxyExpiration",
  "DESIRED_Archs",
  "Environment",
  "RecentBlockReadKbytes",
  "RecentBlockReads",
  "RecentBlockWriteKbytes",
  "RecentBlockWrites",
  "RecentStatsLifetimeStarter",
  "LastMatchTime",
  "LastSuspensionTime",
  "LastVacateTime_RAW",
  "ShadowBday"
  "ClusterId",
  "CurrentHosts",
  "ExecutableSize_RAW",
  "ImageSize_RAW",
  "ShouldTransferFiles",
  "LastJobStatus",
  "LocalSysCpu",
  "LocalUserCpu",
  "MachineAttrCpus0",
  "MachineAttrSlotWeight0",
  "MATCH_EXP_JOB_GLIDEIN_Job_Max_Time",
  "MATCH_EXP_JOB_GLIDEIN_MaxMemMBs",
  "MATCH_EXP_JOB_GLIDEIN_Max_Walltime",
  "MATCH_EXP_JOB_GLIDEIN_Memory",
  "MATCH_EXP_JOB_GLIDEIN_ProcId",
  "MATCH_EXP_JOB_GLIDEIN_ToDie",
  "MATCH_EXP_JOB_GLIDEIN_ToRetire",
  "MaxHosts",
  "MinHosts",
  "ProcId",
  "RecentBlockReadKbytes",
  "RecentBlockReads",
  "RecentBlockWriteKbytes",
  "RecentBlockWrites",
  "RequestMemory_RAW",
  "ResidentSetSize_RAW",
  "StatsLifetimeStarter",
  "StartdIpAddr",
  "TotalSuspensions",
  "Cmd",
  "DESIRED_Archs",
  "LastRemoteHost",
  "MATCH_EXP_JOB_GLIDEIN_SEs",
  "REQUIRED_OS",
  "ShouldTransferFiles",
  "StartdIpAddr",
  "StartdPrincipal",
  "WhenToTransferOutput",
  "InputData",
  "User",
  "Owner",
  "Workflow",
  "RemoteHost",
  "GLIDEIN_Entry_Name",
  "x509UserProxyEmail",
])

bool_vals = set([
  "TransferQueued",
  "TransferringInput",
  "NiceUser",
  "ExitBySignal",
])

schedd_query = classad.ExprTree('CMSGWMS_Type =?="prodschedd" && Name =!= "vocms001.cern.ch" && Name =!= "vocms047.cern.ch" && Name=!="vocms015.cern.ch"')

_camp_re = re.compile("[A-Za-z0-9_]+_[A-Z0-9]+-([A-Za-z0-9]+)-")
_prep_re = re.compile("[A-Za-z0-9_]+_([A-Z]+-([A-Za-z0-9]+)-[0-9]+)")
_rval_re = re.compile("[A-Za-z0-9]+_(RVCMSSW_[0-9]+_[0-9]+_[0-9]+)")
_prep_prompt_re = re.compile("(PromptReco|Repack)_[A-Za-z0-9]+_([A-Za-z0-9]+)")
_split_re = re.compile("\s*,?\s*")
def convert_to_json(ad):
    result = {}
    ad.setdefault("MATCH_EXP_JOB_GLIDEIN_CMSSite", ad.get("MATCH_EXP_JOBGLIDEIN_CMSSite", "Unknown"))
    for key in ad.keys():
        if key in ignore:
            continue
        try:
            value = ad.eval(key)
        except:
            continue
        if isinstance(value, classad.Value):
            if value == classad.Value.Error:
                continue
            else:
                value = None
        elif key in bool_vals:
            value = bool(value)
        elif key in int_vals:
            try:
                value = int(value)
            except ValueError:
                if value == "Unknown":
                    value = None
                else:
                    print "Failed key:", key
                    raise
        elif key in string_vals:
            value = str(value)
        elif key in date_vals:
            if value == 0:
                value = None
        #elif key in date_vals:
        #    value = datetime.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
        if key.startswith("MATCH_EXP_JOB_"):
            key = key[len("MATCH_EXP_JOB_"):]
        if key.endswith("_RAW"):
            key = key[:-len("_RAW")]
        result[key] = value
    ttype = ad.get("WMAgent_SubTaskName", "/UNKNOWN").rsplit("/", 1)[-1]
    result["WMAgent_TaskType"] = ttype
    if "CleanupUnmerged" in ttype:
        ttype = "Cleanup"
    elif "Merge" in ttype:
        ttype = "Merge"
    elif "LogCollect" in ttype:
        ttype = "LogCollect"
    elif ttype == "StepOneProc":
        ttype = "DIGI-RECO"
    elif "MiniAODv2" in ttype:
        ttype = "MINIAOD"
    elif ttype.endswith("_0"):
        ttype = "DIGI"
    elif ttype.endswith("_1"):
        ttype = "RECO"
    result["TaskType"] = ttype
    camp = ad.get("WMAgent_RequestName", "UNKNOWN")
    m = _camp_re.match(camp)
    if camp.startswith("PromptReco"):
        camp = "PromptReco"
    elif camp.startswith("Repack"):
        camp = "Repack"
    elif "RVCMSSW" in camp:
        camp = "RelVal"
    elif m:
        camp = m.groups()[0]
    result["Campaign"] = camp
    prep = ad.get("WMAgent_RequestName", "UNKNOWN")
    m = _prep_re.match(prep)
    if m:
        prep = m.groups()[0]
    else:
        m = _prep_prompt_re.match(prep)
        if m:
            prep = m.groups()[0] + "_" + m.groups()[1]
        else:
            m = _rval_re.match(prep)
            if m:
                prep = m.groups()[0]
    result["Workflow"] = prep
    result["WallClockHr"] = ad.get("RemoteWallClockTime", 0)/3600.
    result["CoreHr"] = ad.get("RequestCpus", 1.0)*ad.get("RemoteWallClockTime", 0)/3600.
    result["CommittedCoreHr"] = ad.get("RequestCpus", 1.0)*ad.get("CommittedTime", 0)/3600.
    result["CommittedWallClockHr"] = ad.get("CommittedTime", 0)/3600.
    result["CpuTimeHr"] = (ad.get("RemoteSysCpu", 0)+ad.get("RemoteUserCpu", 0))/3600.
    result["DiskUsageGB"] = ad.get("DiskUsage_RAW", 0)/1000000.
    result["MemoryMB"] = ad.get("ResidentSetSize_RAW", 0)/1024.
    result["DataLocations"] = _split_re.split(ad.get("DESIRED_CMSDataLocations", "UNKNOWN"))
    result["DESIRED_Sites"] = _split_re.split(ad.get("DESIRED_Sites", "UNKNOWN"))
    result["Original_DESIRED_Sites"] = _split_re.split(ad.get("ExtDESIRED_Sites", "UNKNOWN"))
    result["Site"] = ad.get("MATCH_EXP_JOB_GLIDEIN_CMSSite", "UNKNOWN")
    info = result["Site"].split("_", 2)
    if len(info) == 3:
        result["Tier"] = info[0]
        result["Country"] = info[1]
    else:
        result["Tier"] = "Unknown"
        result["Country"] = "Unknown"
    if "Site" not in result or "DESIRED_Sites" not in result:
        result["InputData"] = "Unknown"
    elif result["Site"] in result["DESIRED_Sites"]:
        result["InputData"] = "Onsite"
    else:
        result["InputData"] = "Offsite"
    if result["WallClockHr"] == 0:
        result["CpuEff"] = 0
    else:
        result["CpuEff"] = 100*result["CpuTimeHr"] / result["WallClockHr"] / float(ad.get("RequestCpus", 1.0))
    return json.dumps(result)


es = elasticsearch.Elasticsearch()
idx = time.strftime("htcondor-%Y-%m-%d")
print es.indices.create(index=idx, ignore=400)
mappings = {}
def filter_name(keys):
    for key in keys:
        if key.startswith("MATCH_EXP_JOB_"):
            key = key[len("MATCH_EXP_JOB_"):]
        if key.endswith("_RAW"):
            key = key[:-len("_RAW")]
        yield key

for name in filter_name(int_vals):
  mappings[name] = {"type": "long"}
for name in filter_name(string_vals):
  mappings[name] = {"type": "string", "index": "not_analyzed"}
for name in filter_name(date_vals):
  mappings[name] = {"type": "date", "format": "epoch_second"}
for name in filter_name(bool_vals):
  mappings[name] = {"type": "boolean"}
mappings["Args"]["index"] = "no"
mappings["Cmd"]["index"] = "no"
mappings["StartdPrincipal"]["index"] = "no"
mappings["StartdIpAddr"]["index"] = "no"

print mappings
idx_clt = elasticsearch.client.IndicesClient(es)
print idx_clt.put_mapping(doc_type="job", index=idx, body=json.dumps({"properties": mappings}), ignore=400)

coll = htcondor.Collector("vocms099.cern.ch")

try:
    checkpoint = json.load(open("checkpoint.json"))
except:
    checkpoint = {}

starttime = time.time()
for schedd_ad in coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=["MyAddress", "ScheddIpAddr", "Name"]):
    schedd = htcondor.Schedd(schedd_ad)
    last_completion = int(checkpoint.get(schedd_ad["Name"], 0))
    history_query = classad.ExprTree("CompletionDate >= %d" % last_completion)
    if time.time() - starttime > 9*60:
        print "Crawler has been running for more than 9 minutes; exiting."
        break
    print "Querying %s for history." % schedd_ad["Name"]
    count = 0
    try:
        history_iter = schedd.history(history_query, [], -1)
        json_ad = '{}'
        for job_ad in history_iter:
            json_ad = convert_to_json(job_ad)
            #print job_ad
            #break
            print es.index(index=idx, doc_type="job", body=json_ad, id=job_ad["GlobalJobId"])
            count += 1
            job_completion = job_ad.get("CompletionDate")
            if job_completion > last_completion:
                last_completion = job_completion
            if time.time() - starttime > 9*60:
                print "Crawler has been running for more than 9 minutes; exiting."
                break
        print "Sample ad for", job_ad["GlobalJobId"]
        json_ad = json.loads(json_ad)
        keys = json_ad.keys()
        keys.sort()
        for key in keys:
            print key, "=", json_ad[key]
    except RuntimeError:
        print "Failed to query schedd for history:", schedd_ad["Name"]
        continue
    print "Schedd total response count:", count
    checkpoint[schedd_ad["Name"]] = last_completion
    fd = open("checkpoint.json.new", "w")
    json.dump(checkpoint, fd)
    fd.close()
    os.rename("checkpoint.json.new", "checkpoint.json")

    if time.time() - starttime > 9*60:
        break

fd = open("checkpoint.json.new", "w")
json.dump(checkpoint, fd)
fd.close()
os.rename("checkpoint.json.new", "checkpoint.json")

