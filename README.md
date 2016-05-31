
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
- `CoreHr`: The number of core-hours utilized by the job.  If the job lasted for
  24 hours and utilized 4 cores, the value of `CoreHr` will be 96.
- `CpuEff`: The total scheduled CPU time (user and system CPU) divided by core hours,
  in percentage.  If the job lasted for 24 hours, utilized 4 cores, and used 72 hours
  of CPU time, then `CpuEff` would be 75.
- `MemoryMB`: The amount of RAM used by the job.
- `RequestCpus`: Number of cores utilized by the job.
- `RequestMemory`: Amount of memory requested by the job, in MB.

CMS-specific attributes:
- `Campaign`: The campaign this job belongs to; derived from the WMAgent workflow
  name (so, this is an approximation; may have nonsense values for strangely-named
  workflows).  Only present for production jobs.
- `Type`: the kind of job run; `analysis` or `production`.
- `TaskType`: A more detailed task type classification, based on the CMSSW config.
  Typically, `analysis`, `DIGI`, `RECO`, `DIGI-RECO`, `GEN-SIM`, or `Cleanup`.
- `MegaEents`: The number of events processed by the job, in millions.
- `KEvents`: The number of events processed by the job, in thousands.
- `ExitCode`: Exit code of the job.  If available, this is actually the exit code
  of the last CMSSW step.
- `DESIRED_Sites`: The list of sites the job potentially could run on.
- `DataLocations`: The list of known primary input dataset locations.
- `OutputGB`: Amount of output written by the CMSSW process, in gigabytes.
- `ReadTimeHrs` and `ReadTimeMins`: Amount of time CMSSW spent in its IO subsystem
  for reads (in hours and minutes, respectively).
- `Site`: CMS site where the job ran (for example, `T2_CH_CSCS`).
- `Country`: Country where the job ran (for example, `CH`).
- `Tier`: Tier where the job ran (for example, `T2`).
- `CRAB_UserHN`: for analysis jobs, CMS username of user that submitted the task.

In general, the data from CMSSW itself (number of events, data read and written)
may be missing.  This information depends on a sufficiently recent version of
CMSSW and HTCondor.
