
ElasticSearch upload for CMS and HTCondor data
----------------------------------------------

This package contains a set of scripts that assist in uploading data from
a HTCondor pool to ElasticSearch.  It queries for both historical and current
job ClassAds, converts them to a JSON document, and uploads them to the
local ElasticSearch instance.


The majority of the logic is in the conversion of ClassAds to values that
are immediately useful in Kibana / ElasticSearch.  A significant portion
of the logic is very specific to the CMS global pool - but ultimately could
be reused for other HTCondor pools.

Important Attributes
--------------------

This service converts the raw ClassAds into JSON documents; one JSON document
per job in the system.  While most attributes are copied over verbatim, a few
new ones are computed on insertion; this allows easier construction of
ElasticSearch queries.   This section documents the most commonly used and
most useful attributes, along with their meaning.

Generic attributes:
- `RecordTime`: When the job exited the queue or when the JSON document was last
  updated, whichever came first.  Use this field for time-based queries.
- `GlobalJobId`: Use to uniquely identify individual jobs.
- `BenchmarkJobDB12`: An estimate of the per-core performance of the machine the job last
  ran on, based on the `DB12` benchmark.  Higher is better.
- `CoreHr`: The number of core-hours utilized by the job.  If the job lasted for
  24 hours and utilized 4 cores, the value of `CoreHr` will be 96.  This includes all
  runs of the job, including any time spent on preempted instance.
- `CpuTimeHr`: The amount of CPU time (sum of user and system) attributed to the job,
  in hours.
- `CommittedCoreHr`: The core-hours only for the last run of the job (excluding preempted
  attempts).
- `QueueHrs`: Number of hours the job spent in queue before last run.
- `WallClockHr`: Number of hours the job spent running.  This is invariant of the
  number of cores; most users will prefer `CoreHr` instead.
- `CpuEff`: The total scheduled CPU time (user and system CPU) divided by core hours,
  in percentage.  If the job lasted for 24 hours, utilized 4 cores, and used 72 hours
  of CPU time, then `CpuEff` would be 75.
- `CpuBadput`: The badput associated with a job, in hours.  This is the sum of all
  unsuccessful job attempts.  If a job runs for 2 hours, is preempted, restarts,
  then completes successfully after 3 hours, the `CpuBadput` is 2.
- `MemoryMB`: The amount of RAM used by the job.
- `Processor`: The processor model (from `/proc/cpuinfo`) the job last ran on.
- `RequestCpus`: Number of cores utilized by the job.
- `RequestMemory`: Amount of memory requested by the job, in MB.
- `ScheddName`: Name of HTCondor schedd where the job ran.
- `Status`: State of the job; valid values include `Completed`, `Running`, `Idle`,
  or `Held`.  *Note*: due to some validation issues, non-experts should only look
  at completed jobs.
- `x509userproxysubject`: The DN of the grid certificate associated with the job; for
  CMS jobs, this is not the best attribute to use to identify a user (prefer `CRAB_UserHN`).
- `DB12CommittedCoreHr`, `DB12CoreHr`, `DB12CpuTimeHr`: The job's `CommittedCoreHr`,
  `CoreHr`, and `CpuTimeHr` values multiplied by the `BenchmarkJobDB12` score.

CMS-specific attributes:
- `Campaign`: The campaign this job belongs to; derived from the WMAgent workflow
  name (so, this is an approximation; may have nonsense values for strangely-named
  workflows).  Only present for production jobs.
- `Workflow`: Human-readable workflow name.  Example: `HIG-RunIISpring16DR80-01026_0`
- `WMAgent_RequestName`: The WMAgent request name (for production jobs only); example:
  `pdmvserv_task_HIG-RunIISpring16DR80-01026__v1_T_160530_083522_4482`.
- `WMAgent_SubTaskName`: The WMAgent subtask name (for production jobs only); example:
  `/pdmvserv_task_HIG-RunIISpring16DR80-01026__v1_T_160530_083522_4482/HIG-RunIISpring16DR80-01026_0`
- `CMSGroups`: Name of the CMS group associated with this request (production only).
  Example: `HIG`.
- `Type`: the kind of job run; `analysis` or `production`.
- `TaskType`: A more detailed task type classification, based on the CMSSW config.
  Typically, `analysis`, `DIGI`, `RECO`, `DIGI-RECO`, `GEN-SIM`, or `Cleanup`.
- `MegaEvents`: The number of events processed by the job, in millions.
- `KEvents`: The number of events processed by the job, in thousands.
- `CMSSWKLumis`: The number of lumi sections processed by the job, in thousands.
- `ChirpCMSSWMaxEvents`, `ChirpCMSSWMaxFiles`, `ChirpCMSSWMaxLumis`: the maximum
  number of events, files, and lumis (respectively) a job will process before exiting.
  This may not be known for all jobs; in that case, `-1` is reported.  These can
  be used to estimate the percent completion of jobs; the veracity of these attributes
  are not known.
- `ExitCode`: Exit code of the job.  If available, this is actually the exit code
  of the last CMSSW step.
- `CRAB_AsyncDest`: The output destination CMS site name for a CRAB3 job.
- `DESIRED_CMSDataset`: The primary input dataset name.
- `CRAB_DataBlock`: The primary input block name.  For CRAB3 jobs only.
- `DESIRED_Sites`: The list of sites the job potentially could run on.
- `DataLocations`: The list of known primary input dataset locations.
- `CMSSWWallHrs`: The number of wall hours reported by `cmsRun`.
- `StageOutHrs`: An _estimate_ of the stageout time, in hours.  Calculated from
  `WallClockHr-CMSSWWallHrs`.  The veracity of this attribute is not known.
- `InputData`: `Onsite` if `Site` is in the `DataLocations` list; `Offsite` otherwise.
- `OutputGB`: Amount of output written by the CMSSW process, in gigabytes.
- `InputGB`: Amount of data read by the CMSSW process; in gigabytes.
- `ReadTimeHrs` and `ReadTimeMins`: Amount of time CMSSW spent in its IO subsystem
  for reads (in hours and minutes, respectively).
- `ReadOpPercent`: percentage of reads done via a single `read` operation (as
  opposed to a vectored `readv` operation).  If a job issues a single `read`
  and a `readv` of 9 segments, this value would be 50.
- `ReadOpSegmentPercent`: percentage of read segments done via a single `read`
  operation.   If a job issues a single `read` and a `readv` of 9 segments,
  this value would be 10.
- `ChirpCMSSWReadOps`: number of `read` operations performed (excludes `readv`
  activity).
- `Site`: CMS site where the job ran (for example, `T2_CH_CSCS`).
- `Country`: Country where the job ran (for example, `CH`).
- `Tier`: Tier where the job ran (for example, `T2`).
- `CRAB_UserHN`: for analysis jobs, CMS username of user that submitted the task.
- `CRAB_Workflow`: for analysis jobs, the CRAB task name.  It is of the form
  `170406_201711:ztu_crab_multicrab_CMEandMixedHarmonics_Pbp_HM185_250_q3_v3_185_250_1_3`;
  for the `Workflow` attribute, this is shortened to
  `ztu_crab_multicrab_CMEandMixedHarmonics_Pbp_HM185_250_q3_v3_185_250_1_3`.
  `CRAB_Workflow` is considered unique for a single CRAB task; depending on the
  user's naming scheme, there may multiple CRAB tasks per `Workflow`.
- `EventRate`, `CpuEventRate`: The number of events per second (or per CPU second)
  per core.
- `TimePerEventR`, `CpuTimePerEvent`:  The inverse of `EventRate` and `CpuEventRate`,
  respectively.
- `HasSingularity`: Set to `true` if the job was run inside a Singularity container.

In general, the data from CMSSW itself (number of events, data read and written)
may be missing.  This information depends on a sufficiently recent version of
CMSSW and HTCondor.
