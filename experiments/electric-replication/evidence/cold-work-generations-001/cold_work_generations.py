"""Final-source repeated-generation checks, using the unchanged paced probe.

Separate from ds-bench; baseline and candidate have equal observer RPCs. The
source gate of the historical cold_generations campaign remains unchanged.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys

import cold_benchmark as bench
import cold_generations as generations
from cold_work_benchmark import BASELINE, CANDIDATE
from lab import EXPERIMENT, ROOT, source_hashes


def run(args):
    os.sched_setaffinity(0,{4,5})
    output=Path(args.output).resolve()
    output.mkdir(parents=True,exist_ok=False)
    prior=json.loads((EXPERIMENT/"evidence/cold-runtime-tests-001/qualification.json").read_text())
    assert bench.digest(BASELINE)==prior["release_binary_sha256"]
    sources=source_hashes()
    shutil.copyfile(__file__,output/Path(__file__).name)
    bench.save(output/"provenance.json",dict(argv=sys.argv,
        sources=dict(baseline=prior["sources"],candidate=sources),
        binaries={str(p.relative_to(ROOT)):bench.digest(p) for p in (BASELINE,CANDIDATE,bench.NATIVE,bench.MINIO)},
        harness=bench.archive_harness(output),
        scope="paced 64 appends/s; independent byte/offset and reader-pin checks; NOT ds-bench or independent-host capacity"))
    choices=[(v,a) for v in args.variants for a in args.arms]
    ledger=[]
    for repeat in range(1,args.repetitions+1):
        rotate=(repeat-1)%len(choices)
        for variant,arm in choices[rotate:]+choices[:rotate]:
            executable=BASELINE if variant=="baseline" else CANDIDATE

            class SelectedLab(bench.BenchLab):
                def __init__(self,*pos,**kw):
                    super().__init__(*pos,**kw)
                    self.binary=executable

                def observe(self):
                    row=super().observe()
                    row["pressure"]={}
                    for n in sorted(self.nodes):
                        status,_,body=self.request(n,"POST","/_admin/0/pressure",b"null",{"x-electric-cluster":self.cluster})
                        assert status==(200 if variant=="candidate" else 404)
                        row["pressure"][str(n)]=json.loads(body) if status==200 else None
                    return row

            generations.BenchLab=SelectedLab
            name=f"{output.name}-{variant}-{arm}-r{repeat}"
            result=generations.run_cell(output/name,arm,True,args.generations,args.period)
            result.update(variant=variant,binary_sha256=bench.digest(executable))
            bench.save(output/name/"result.json",result)
            ledger.append(dict(cell=name,verdict=result["verdict"],error=result.get("error")))
            bench.save(output/"ledger.json",ledger)
    assert sources==source_hashes(),"runtime changed during campaign"
    return all(r["verdict"]=="PASS" for r in ledger)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--variants",nargs="+",choices=["baseline","candidate"],default=["baseline","candidate"])
    parser.add_argument("--arms",nargs="+",choices=["raft1","quorum3","async3"],default=["quorum3","async3"])
    parser.add_argument("--repetitions",type=int,default=3)
    parser.add_argument("--generations",type=int,default=32)
    parser.add_argument("--period",type=int,default=5)
    options=parser.parse_args()
    assert 1<=options.repetitions<=3 and 2<=options.generations<=48 and 2<=options.period<=15
    raise SystemExit(0 if run(options) else 1)
