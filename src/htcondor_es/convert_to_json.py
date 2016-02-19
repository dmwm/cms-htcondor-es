#!/usr/bin/python

import os
import re
import json
import time
import classad
import datetime
import htcondor

string_vals = set([ \
  "CRAB_JobType",
  "CRAB_JobSW",
  "CRAB_JobArch",
  "CRAB_ISB",
  "CRAB_Workflow",
  "CRAB_UserRole",
  "CMSGroups",
  "CRAB_UserHN",
  "CRAB_UserGroup",
  "CRAB_TaskWorker",
  "CRAB_SiteWhitelist",
  "CRAB_SiteBlacklist",
  "CRAB_PrimaryDataset",
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
  "Status",
  "Universe",
  "ExitReason",
  "LastHoldReason",
  "RemoveReason",
  "DESIRED_Overflow_Region",
  "DESIRED_OpSysMajorVers",
  "DESIRED_CMSDataset",
  "DAGNodeName",
  "DAGParentNodeNames",
  "OverflowType",
  "ScheddName",
])

int_vals = set([ \
  "CRAB_Id"
  "CRAB_Retry",
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
  "DesiredSiteCount",
  "DataLocationsCount",
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
  "DataCollectionDate",
])

ignore = set([
  "CRAB_UserDN",
  "CRAB_Destination",
  "CRAB_DBSURL",
  "CRAB_ASOURL",
  "CRAB_ASODB",
  "CRAB_AdditionalOutputFiles",
  "CRAB_EDMOutputFiles",
  "CRAB_TFileOutputFiles",
  "CRAB_oneEventMode",
  "CRAB_NumAutomJobRetries",
  "CRAB_localOutputFiles",
  "CRAB_ASOTimeout",
  "CRAB_OutTempLFNDir",
  "CRAB_PublishDBSURL",
  "CRAB_PublishGroupName",
  "CRAB_RestURInoAPI",
  "CRAB_RestHost",
  "CRAB_ReqName",
  "CRAB_RetryOnASOFailures",
  "CRAB_StageoutPolicy",
  "SubmitEventNotes",
  "DAGManNodesMask",
  "DAGManNodesLog",
  "DAGManJobId",
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
  "EnvDelim",
  "Env",
  "ExecutableSize",
  "HasBeenRouted",
  "HasPrioCorrection",
  "ImageSize",
  "In",
  "Iwd",
  "JobAdInformationAttrs",
  "job_ad_information_attrs",
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
  "Used_Gatekeeper",
  "DESIRED_OpSyses",
])

no_idx = set([
  "CRAB_OutLFNDir",
  "Args",
  "Cmd",
  "BytesRecvd",
  "CoreSize",
  "DelegatedProxyExpiration",
  "Environment",
  "RecentBlockReadKbytes",
  "RecentBlockReads",
  "RecentBlockWriteKbytes",
  "RecentBlockWrites",
  "RecentStatsLifetimeStarter",
  "CurrentHosts",
  "MachineAttrCpus0",
  "MachineAttrSlotWeight0",
  "LocalSysCpu",
  "LocalUserCpu",
  "MaxHosts",
  "MinHosts",
  "StartdIpAddr",
  "StartdPrincipal",
  "LastRemoteHost",
])

no_analysis = set([
  "CRAB_PublishName",
  "CRAB_PublishGroupName",
  "CondorPlatform",
  "CondorVersion",
  "CurrentHosts",
  "DESIRED_Archs",
  "ShouldTransferFiles",
  "TotalSuspensions",
  "REQUIRED_OS",
  "ShouldTransferFiles",
  "WhenToTransferOutput",
  "InputData",
  "DAGParentNodeNames",
])

bool_vals = set([
  "CRAB_Publish",
  "CRAB_SaveLogsFlag",
  "CRAB_TransferOutputs",
  "TransferQueued",
  "TransferringInput",
  "NiceUser",
  "ExitBySignal",
])

status = { \
  0: "Unexpanded",
  1: "Idle",
  2: "Running",
  3: "Removed",
  4: "Completed",
  5: "Held",
  6: "Error",
}

universe = { \
  1: "Standard",
  2: "Pipe",
  3: "Linda",
  4: "PVM",
  5: "Vanilla",
  6: "PVMD",
  7: "Scheduler",
  8: "MPI",
  9: "Grid",
 10: "Java",
 11: "Parallel",
 12: "Local",
}

_launch_time = int(time.time())
def get_data_collection_time():
    return _launch_time

_camp_re = re.compile("[A-Za-z0-9_]+_[A-Z0-9]+-([A-Za-z0-9]+)-")
_prep_re = re.compile("[A-Za-z0-9_]+_([A-Z]+-([A-Za-z0-9]+)-[0-9]+)")
_rval_re = re.compile("[A-Za-z0-9]+_(RVCMSSW_[0-9]+_[0-9]+_[0-9]+)")
_prep_prompt_re = re.compile("(PromptReco|Repack|Express)_[A-Za-z0-9]+_([A-Za-z0-9]+)")
_split_re = re.compile("\s*,?\s*")
def convert_to_json(ad):
    analysis = "CRAB_Id" in ad
    if ad.get("TaskType") == "ROOT":
        return None
    result = {}
    result['DataCollection'] = ad.get("CompletionDate", 0)
    result['DataCollectionDate'] = ad.get("CompletionDate", 0)
    if not result['DataCollection']:
        result['DataCollection'] = _launch_time
    if not result['DataCollectionDate']:
        result['DataCollectionDate'] = _launch_time
    result['ScheddName'] = ad.get("GlobalJobId", "UNKNOWN").split("#")[0]
    if analysis:
        result["Type"] = "analysis"
    else:
        result["Type"] = "production"
    ad.setdefault("MATCH_EXP_JOB_GLIDEIN_CMSSite", ad.get("MATCH_EXP_JOBGLIDEIN_CMSSite", "Unknown"))
    for key in ad.keys():
        if key in ignore or key.startswith("HasBeen"):
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
    if analysis:
        ttype = "Analysis"
    elif "CleanupUnmerged" in ttype:
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
    if analysis:
        camp = "crab_" + ad.get("CRAB_UserHN", "UNKNOWN")
    elif camp.startswith("PromptReco"):
        camp = "PromptReco"
    elif camp.startswith("Repack"):
        camp = "Repack"
    elif camp.startswith("Express"):
        camp = "Express"
    elif "RVCMSSW" in camp:
        camp = "RelVal"
    elif m:
        camp = m.groups()[0]
    result["Campaign"] = camp
    prep = ad.get("WMAgent_RequestName", "UNKNOWN")
    m = _prep_re.match(prep)
    if analysis:
        prep = ad.get("CRAB_Workflow", "UNKNOWN").split(":", 1)[-1]
    elif m:
        prep = m.groups()[0]
    else:
        m = _prep_prompt_re.match(prep)
        if m:
            prep = m.groups()[0] + "_" + m.groups()[1]
        else:
            m = _rval_re.match(prep)
            if m:
                prep = m.groups()[0]
    now = time.time()
    if ad.get("JobStatus") == 2 and (ad.get("EnteredCurrentStatus", now+1) < now):
        ad["RemoteWallClockTime"] = int(now - ad["EnteredCurrentStatus"])
        ad["CommittedTime"] = ad["RemoteWallClockTime"]
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
    result["DesiredSiteCount"] = len(result["DESIRED_Sites"])
    result["DataLocationsCount"] = len(result["DataLocations"])
    result["Original_DESIRED_Sites"] = _split_re.split(ad.get("ExtDESIRED_Sites", "UNKNOWN"))
    if analysis:
        result["CMSGroups"] = _split_re.split(ad.get("CMSGroups", "UNKNOWN"))
        result["OutputFiles"] = len(ad.get("CRAB_AdditionalOutputFiles", [])) + len(ad.get("CRAB_TFileOutputFiles", [])) + len(ad.get("CRAB_EDMOutputFiles", [])) + ad.get("CRAB_SaveLogsFlag", 0)
    if "x509UserProxyFQAN" in ad:
        result["x509UserProxyFQAN"] = str(ad["x509UserProxyFQAN"]).split(",")
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
    elif result["Site"] in result["DESIRED_CMSDataLocations"]:
        result["InputData"] = "Onsite"
    else:
        result["InputData"] = "Offsite"
        if analysis:
            if result["Site"] not in result["DESIRED_Sites"]:
                result["OverflowType"] = "FrontendOverflow"
            else:
                result["OverflowType"] = "IgnoreLocality"
        else:
            result["OverflowType"] = "Unified"
    if result["WallClockHr"] == 0:
        result["CpuEff"] = 0
    else:
        result["CpuEff"] = 100*result["CpuTimeHr"] / result["WallClockHr"] / float(ad.get("RequestCpus", 1.0))
    result["Status"] = status.get(ad.get("JobStatus"), "Unknown")
    result["Universe"] = universe.get(ad.get("JobUniverse"), "Unknown")
    result["QueueHrs"] = (ad.get("JobCurrentStartDate", time.time()) - ad["QDate"])/3600.
    result["Badput"] = max(result["CoreHr"] - result["CommittedCoreHr"], 0.0)
    result["CpuBadput"] = max(result["CoreHr"] - result["CpuTimeHr"], 0.0)
    return json.dumps(result)


