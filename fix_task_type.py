#!/usr/bin/python

import json
import time
import elasticsearch
import datetime

import re
import os
import sys
import calendar
if os.path.exists("src"):
    sys.path.append("src")
from htcondor_es.es import get_server_handle

es = get_server_handle()

body = {"query": {
  "bool": {
      "filter": [{"exists": {"field": "TaskType"}}, {"exists": {"field": "WMAgent_SubTaskName"}},
        {"bool": {
                   "should" : [{"bool": {"must_not": {"exists": {"field": "HasTaskTypeCorrectionV1"}}}},
                               {"term": {"TaskType": "steponeproc"}},
                               {"term": {"TaskType": "steptwoproc"}},
                               {"term": {"TaskType": "unknown"}},
                               {"term": {"TaskType": "production"}}],
                   "minimum_should_match" : 1,
                 }
        }
      ]
    }
  }
}
body = json.dumps(body)
now = int(time.time())

def task_type_logic(result):
    workflow = result.get("Workflow", "/UNKNOWN")
    if re.match('amaltaro_.+_Test_Backfill', workflow):
        result['TaskType'] = 'Backfill'
        result['Campaign'] = 'Backfill'
        return result
    if re.match('amaltaro_.+_HG1[0-9]+.*Val', workflow) or \
       re.match('amaltaro_.+_Tests', workflow) or re.match('amaltaro_.+_Agent.+_Validation', workflow) or \
       re.match('sryu_.+_reqmgr2', workflow) or re.match('amaltaro_.+_JDLMagic', workflow) or \
       re.match('amaltaro_MonteCarlo_eff_December_Patches', workflow) or re.match('amaltaro_.+_DMWM_Test', workflow) or \
       re.match('amaltaro_MonteCarlo_eff', workflow) or re.match('amaltaro_.+New_JSONs', workflow) or \
       re.match('amaltaro_MonteCarlo_.+_JDL_Magic', workflow) or re.match('amaltaro_.+_December_Patches', workflow) or \
       re.match('prozober_.+_CSCS_HPC__Commissioning', workflow) or re.match('amaltaro_.+_December_Abort', workflow) or \
       re.match('prozober_.+_HIP_Commissioning', workflow) or re.match('prozober_.+_JAN_Test', workflow) or \
       re.match('prozober_.+_Brunel_Commissioning', workflow) or re.match('prozober_.+_HPC_Test', workflow):
        result['TaskType'] = 'Validation'
        result['Campaign'] = 'Validation'
        return result
        
    if ('RVCMSSW' in workflow) or (workflow.startswith("Express")) or (result.get("Campaign") == "Repack") or \
       (result.get("Campaign") == "PromptReco"):
        return result

    ttype = result.get("WMAgent_SubTaskName", "/UNKNOWN").rsplit("/", 1)[-1]
    # Guess an alternate campaign name from the subtask
    camp2_info = ttype.split("-")
    if len(camp2_info) > 1:
        camp2 = camp2_info[1]
        result['Campaign'] = camp2
    else:
        camp2 = result.get("Campaign", ttype)
    workflow2_info = ttype.split("_")
    if (len(workflow2_info) > 1) and ('LogCollectFor' not in ttype) and (workflow2_info[0] not in  ["skim", "DigiFullPU", "RecoFullPU", "PREMIXUP15", "ProdQCD", "RecoFull", "DigiFull", "ALCAFull"]):
        result['Workflow'] = workflow2_info[0]

    if "CleanupUnmerged" in ttype:
        ttype = "Cleanup"
    elif "Merge" in ttype:
        ttype = "Merge"
    elif "LogCollect" in ttype:
        ttype = "LogCollect"
    elif ("MiniAOD" in result.get("WMAgent_RequestName", "UNKNOWN")) and (ttype == "StepOneProc"):
        ttype = "MINIAOD"
    elif ttype == "StepTwoProc" and (camp2 in ["RunIISummer16DR80", "PhaseIFall16DR", "pPb816Summer16DR", 'RunIISpring16DR80']):
        ttype = "RECO"
    # In the end, RunIISummer16DR80 used StepOneProc for BOTH DIGIRECO and DIGI, depending on the physics.
    # The tasks appear otherwise indistinguishable.  Accordingly, we're going with DIGI as this is the most
    # common approach.
    elif ttype == "StepOneProc" and (camp2 in ["RunIISummer16DR80", "TP2023HGCALDR", 'RunIISpring16DR80']):
        ttype = "DIGI"
    elif ttype == "Production" and (camp2 in ["RunIIHighPUTrainsDR", "RunIISummer16FSPremix", 'RunIISpring16FSPremix']): # FastSIM DIGI RECO jobs.
        ttype = "DIGIRECO"
    elif ttype == "Production" and (camp2 in ['RunIIWinter15wmLHE']): # FastSIM DIGI RECO jobs.
        ttype = "LHE"
    elif ttype == "Production" and (("15pLHE" in camp2) or ("16GS" in camp2) or ("15wmLHEGS" in camp2) or ("15GS" in camp2) or ("16wmLHEGS" in camp2) or ("17GS" in camp2) or (camp2 in ["PhaseIIFall16LHEGS82", "HiFall15", "pp502Fall15wmLHEGS", 'TP2023HGCALGS', 'TTI2023Upg14GS', 'Summer12', 'CosmicFall16PhaseIGS', 'pp502Fall15'])):
        ttype = "GENSIM"
    elif ttype == 'Production' and (camp2 in ['RunIISpring15PrePremix']):
        ttype = "DIGI"
    elif ttype == "StepOneProc" and (("15DR" in camp2) or ("16DR" in camp2) or ("17DR" in camp2) or (camp2 == "Summer12DR53X") or (camp2 == "TTI2023Upg14D") or (camp2 == "TTI2023Upg14D") or ("ReDigi" in camp2) or ("HINPbPbWinter16DR" == camp2) or ("RunIIHighPUTrainsDR" == camp2)):
        ttype = "DIGIRECO"
    elif "MiniAOD" in ttype:
        ttype = "MINIAOD"
    elif (("TTI2023Upg14GS" in camp2) or ("16wmLHEGS" in camp2) or ("15wmLHEGS" in camp2) or ("15GS" in camp2) or ("16GS" in camp2) or ("17GS" in camp2)) and ttype.endswith("_0"):
        ttype = "GENSIM"
    elif (("15wmLHE" in camp2) and ttype.endswith("_0")): # Note that this matches 15wmLHE but 15wmLHEGS case matches above.
        ttype = "LHE"
    elif ttype.endswith("_0"):
        ttype = "DIGI"
    elif ttype.endswith("_1"):
        ttype = "RECO"
    elif ttype == "MonteCarloFromGEN":
        ttype = "GENSIM"
    result["TaskType"] = ttype
    return result

if len(sys.argv) > 1:
    index = 'cms-%s-' % sys.argv[1]
else:
    index = 'cms-2017-03-'

print "Old job query:", body
print "Index wildcard:", index

year, month = re.match("cms-([0-9]+)-([0-9]+)-", index).groups()
nowd = datetime.datetime.now()
max_day = calendar.monthrange(int(year), int(month))[-1]
if (int(year) == nowd.year) and (int(month) == nowd.month) and (max_day > nowd.day):
    max_day = nowd.day
print "Querying up to cms-%s-%s-%d" % (year, month, max_day)
cur_day = 1
while True:
    cur_index = '%s%02d' % (index, cur_day)
    st = time.time()
    print "Querying for problem jobs in %s" % cur_index
    try:
        results = es.search(index=cur_index, size=1000, body=body, _source=['WMAgent_RequestName', 'WMAgent_SubTaskName', 'Workflow', 'Campaign', 'TaskType'])['hits']['hits']
    except Exception, e:
        print "Failure when querying; sleeping for 10s then trying again."
        print str(e)
        time.sleep(10)
        continue

    if not results:
        if cur_day < max_day:
            cur_day += 1
            continue
        print "No results found for updating ."
        break
    bulk = ''
    print results[0]
    for result in results:
        bulk += json.dumps({"update": {"_id": result["_id"]}}) + "\n"
        source = result["_source"]
        new_source = task_type_logic(dict(source))
        updates = {'HasTaskTypeCorrectionV1': True}
        for key in ['TaskType', 'Campaign', 'Workflow']:
            if (source[key] != new_source[key]):
                if (source['Workflow'].startswith("amaltaro_") or new_source['TaskType']=='Validation' or source['Workflow'].startswith("sryu_")) and key in ['TaskType', 'Campaign']:
                    updates[key] = new_source[key]
                    print key, source[key], "->", new_source[key]
                elif (key == 'Workflow') and ((\
                     (\
                      (source['Workflow'].startswith('pdmvserv_task_') and not new_source['Workflow'].startswith('pdmvserv_task_')) or \
                      (source['Workflow'].startswith('prozober_recovery') and not new_source['Workflow'].startswith('prozober_recovery')) or \
                      (source['Workflow'].startswith('prozober_task_') and not new_source['Workflow'].startswith('prozober_task_')) 
                     )\
                    ) or ( \
                        (re.match('[A-Z2]{3,3}-PhaseIIFall16DR82-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-PhaseIFall16MiniAOD-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-PhaseIIFall16DR82-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-PhaseIFall16DR-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIIFall15MiniAODv2-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIIFall15DR76-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-Summer12DR53X-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIIHighPUTrainsMiniAODv2-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-pPb502Winter16DR-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIISpring16MiniAODv2-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-PhaseIFall16wmLHEGS-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-TTI2023Upg14D-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIISummer15GS-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIISummer16DR80-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIISummer16DR80Premix-', new_source['Workflow'])) or \
                        (re.match('HIN-pPb816Summer16DR-', new_source['Workflow'])) or \
                        (source['Workflow'].startswith("HIN-HiFall15") and new_source['Workflow'].startswith('HIN-HINPbPbWinter16DR')) or \
                        (re.match('[A-Z2]{3,3}-RunIISummer16MiniAODv2-', new_source['Workflow'])) or \
                        (re.match('[A-Z2]{3,3}-RunIISummer16DR80Premix-', new_source['Workflow']))
                    )):
                    updates['Workflow'] = new_source['Workflow']
                    print "Workflow", source['Workflow'], "->", new_source['Workflow']
                elif (key == 'Campaign') and ( \
                        (source['Campaign'] == 'HiFall15' and new_source['Campaign'] == 'HINPbPbWinter16DR') or  \
                        (source['Campaign'].startswith('prozober_recovery') and new_source['Campaign'] == 'RunIISummer16DR80Premix') or  \
                        (source['Campaign'].startswith('prozober_recovery') and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'pPb816Spring16GS' and new_source['Campaign'] == 'pPb816Summer16DR') or  \
                        (source['Campaign'] == 'pPb502Winter16GS' and new_source['Campaign'] == 'pPb502Winter16DR') or  \
                        (source['Campaign'] == 'RunIIHighPUTrainsDR' and new_source['Campaign'] == 'RunIIHighPUTrainsMiniAODv2') or  \
                        (source['Campaign'] == 'PhaseIIFall16LHEGS82' and new_source['Campaign'] == 'PhaseIIFall16DR82') or  \
                        (source['Campaign'] == 'PhaseIFall16wmLHEGS' and new_source['Campaign'] == 'PhaseIFall16DR') or  \
                        (source['Campaign'] == 'PhaseIFall16wmLHEGS' and new_source['Campaign'] == 'PhaseIFall16MiniAOD') or  \
                        (source['Campaign'] == 'PhaseIFall16DR' and new_source['Campaign'] == 'PhaseIFall16MiniAOD') or  \
                        (source['Campaign'] == 'PhaseIFall16GS' and new_source['Campaign'] == 'PhaseIFall16DR') or  \
                        (source['Campaign'] == 'PhaseIFall16GS' and new_source['Campaign'] == 'PhaseIFall16MiniAOD') or  \
                        (source['Campaign'] == 'PhaseIIFall16GS82' and new_source['Campaign'] == 'PhaseIIFall16DR82') or  \
                        (source['Campaign'] == 'RunIIWinter15wmLHE' and new_source['Campaign'] == 'RunIISummer15GS') or  \
                        (source['Campaign'] == 'RunIISpring16DR80' and new_source['Campaign'] == 'RunIISpring16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIIWinter15wmLHE' and new_source['Campaign'] == 'RunIIFall15DR76') or  \
                        (source['Campaign'] == 'RunIIWinter15wmLHE' and new_source['Campaign'] == 'RunIIFall15MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISpring16FSPremix' and new_source['Campaign'] == 'RunIISpring16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer16FSPremix' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer15wmLHEGS' and new_source['Campaign'] == 'RunIISummer16DR80Premix') or  \
                        (source['Campaign'] == 'RunIIWinter15pLHE' and new_source['Campaign'] == 'RunIISummer16DR80Premix') or  \
                        (source['Campaign'] == 'TTI2023Upg14GS' and new_source['Campaign'] == 'TTI2023Upg14D') or  \
                        (source['Campaign'] == 'RunIISummer15wmLHEGS' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer16DR80Premix' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer16DR80' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer15GS' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer15GS' and new_source['Campaign'] == 'RunIISummer16DR80') or  \
                        (source['Campaign'] == 'RunIIFall15DR76' and new_source['Campaign'] == 'RunIIFall15MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer15GS' and new_source['Campaign'] == 'RunIIFall15MiniAODv2') or  \
                        (source['Campaign'] == 'Summer12' and new_source['Campaign'] == 'Summer12DR53X') or  \
                        (source['Campaign'] == 'RunIISummer15GS' and new_source['Campaign'] == 'RunIIFall15DR76') or  \
                        (source['Campaign'] == 'RunIIWinter15pLHE' and new_source['Campaign'] == 'RunIISummer15GS') or  \
                        (source['Campaign'] == 'RunIIWinter15pLHE' and new_source['Campaign'] == 'RunIISummer16MiniAODv2') or  \
                        (source['Campaign'] == 'RunIISummer15GS' and new_source['Campaign'] == 'RunIISummer16DR80Premix') \
                    ):
                    updates['Campaign'] = new_source['Campaign']
                    print "Campaign", source['Campaign'], '->', new_source['Campaign']
                elif (key == 'TaskType') and ( \
                        (new_source['Campaign'] == 'RunIISummer16DR80Premix' and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'DIGI') or \
                        (new_source['Campaign'] == 'pPb816Summer16DR' and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'DIGI') or \
                        (new_source['Campaign'] == 'pPb502Winter16DR' and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'DIGI') or \
                        (new_source['Campaign'] == 'PhaseIIFall16GS82' and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'GENSIM') or \
                        (new_source['Campaign'] == 'PhaseIFall16wmLHEGS' and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'GENSIM') or \
                        (new_source['Campaign'] == 'PhaseIFall16MiniAOD' and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'MINIAOD') or \
                        (new_source['Campaign'] == 'TTI2023Upg14D' and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'DIGI') or \
                        ((new_source['Campaign'] in ['RunIIWinter15wmLHE']) and source['TaskType'] == 'Production' and new_source['TaskType'] == 'LHE') or \
                        ((new_source['Campaign'] in ['RunIISpring15PrePremix']) and source['TaskType'] == 'Production' and new_source['TaskType'] == 'DIGI') or \
                        ((new_source['Campaign'] in ['pPb816Spring16GS', 'CosmicFall16PhaseIGS', 'Summer12', 'TP2023HGCALGS', 'PhaseIIFall16GS82', 'pp502Fall15wmLHEGS', 'pp502Fall15', 'TTI2023Upg14GS', 'PhaseIFall16wmLHEGS', "HiFall15", 'PhaseIIFall16LHEGS82', 'RunIIWinter15pLHE', 'RunIIWinter15GS', 'RunIISummer15GS', 'pPb502Winter16GS', 'PhaseIFall16GS', 'RunIISummer15wmLHEGS']) and source['TaskType'] == 'Production' and new_source['TaskType'] == 'GENSIM') or \
                        ((new_source['Campaign'] in ['RunIIHighPUTrainsDR', 'RunIISummer16FSPremix', 'RunIISpring16FSPremix']) and source['TaskType'] == 'Production' and new_source['TaskType'] == 'DIGIRECO') or \
                        ((new_source['Campaign'] in ['RunIIFall15DR76', 'PhaseIFall16DR']) and source['TaskType'] == 'GENSIM' and new_source['TaskType'] == 'DIGI') or \
                        (new_source['Campaign'] == 'TP2023HGCALDR' and source['TaskType'] == 'DIGIRECO' and new_source['TaskType'] == 'DIGI') or \
                        ((new_source['Campaign'] in ['RunIISpring16DR80', 'RunIISummer16DR80']) and source['TaskType'] == 'DIGIRECO' and new_source['TaskType'] == 'DIGI') or \
                        ((new_source['Campaign'] in ['RunIISummer15GS', 'RunIIWinter15pLHE']) and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'GENSIM') or \
                        (new_source['Campaign'] == 'RunIISummer15wmLHEGS' and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'GENSIM') or \
                        (new_source['Campaign'] == 'RunIIWinter15wmLHE' and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'LHE') or \
                        ((new_source['Campaign'] in ['RunIIHighPUTrainsDR', 'PhaseIFall16DR', 'PhaseIIFall16DR82', 'HINPbPbWinter16DR']) and source['TaskType'] == 'StepOneProc' and new_source['TaskType'] == 'DIGIRECO') or \
                        ((new_source['Campaign'] in ['RunIISpring16DR80']) and source['TaskType'] == 'StepOneProc' and new_source['TaskType'] == 'DIGI') or \
                        ((new_source['Campaign'] in ['RunIISpring16DR80', 'PhaseIFall16DR', 'RunIISummer16DR80', 'pPb816Summer16DR']) and source['TaskType'] == 'StepTwoProc' and new_source['TaskType'] == 'RECO') or \
                        (new_source['Campaign'] == 'PhaseIFall16MiniAOD' and source['TaskType'] == 'DIGI' and new_source['TaskType'] == 'MINIAOD')
                     ):
                    updates['TaskType'] = new_source['TaskType']
                    print "TaskType", source['TaskType'], '->', new_source['TaskType']
                else:
                    print "Mismatch on", key
                    print source
                    print new_source
                    raise Exception("Found task chain")
        if (source['TaskType'] == new_source['TaskType']) and (source['TaskType'] in ['Production', 'StepOneProc', 'StepTwoProc']):
            print source
            print new_source
            raise Exception("Failed to relabel production task type.")
        # Note: CERN ES doesn't support bulk uploads :(
        if len(updates) > 1:
            print result, updates
        else:
            #print "Marking", result["_id"], "as not needing updates."
            pass
        #try:
        #    es.update(id=result["_id"], doc_type="job", index=result["_index"], body={"doc": updates})
        #except Exception, e:
        #    print "Failure when updating; sleeping for 10s then trying again."
        #    print str(e)
        #    time.sleep(10)
        #    continue
        bulk += json.dumps({"doc": updates}) + '\n'

        #raise Exception("Partial update made.")
    print results[-1], updates

    # Set to False for debug mode.
    if True:
        try:
            es.bulk(body=bulk, doc_type="job", index=cur_index)
        except Exception, e:
            print "Failure when updating stale documents:", str(e)
            #raise
    else:
        print bulk
        raise Exception("Have a full set of jobs to update")
    print "Iteration took %.1f seconds" % (time.time()-st)

print "Modifications took %.1f minutes" % ((time.time()-now)/60.)

