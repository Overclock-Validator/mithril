from pathlib import Path
import hashlib,json,os,subprocess,time
root=Path('/srv/mithril-sigverify-streaming-20260912');out=root/'direct-cache-20260912';go='/usr/local/go/bin/go'
base=root/'bin/turbine-final.test';candidate=out/'candidate.test'
assert hashlib.sha256(base.read_bytes()).hexdigest()=='f36508c1d113ea8048ba8b516f4a62b0c15b54369efa41c6211bd4bd491c2da8'
method=json.loads((out/'method.json').read_text())
for name,digest in method['files'].items(): assert hashlib.sha256((out/name).read_bytes()).hexdigest()==digest
results=out/'results';results.mkdir(exist_ok=False)
env=os.environ|{'PATH':'/usr/local/go/bin:'+os.environ.get('PATH',''),'GOMAXPROCS':'8','CGO_ENABLED':'1','GOAMD64':'v1','MITHRIL_SIGVERIFY_FLOW_BACKEND':'r51','MITHRIL_SIGVERIFY_FLOW_COUNT':'33760'}
def health():
 return {'service':subprocess.check_output(['systemctl','show','mithril-alpenglow-pr259.service','--property=ActiveState,SubState,MainPID,ExecMainStartTimestamp'],text=True),'load':Path('/proc/loadavg').read_text().strip()}
def run(name,args,timeout=180):
 print('START',name,flush=True);start=time.time()
 with (results/(name+'.txt')).open('w') as log:
  p=subprocess.run(args,cwd=root/'source',env=env,stdout=log,stderr=subprocess.STDOUT,timeout=timeout)
 with (results/'commands.jsonl').open('a') as record: record.write(json.dumps({'name':name,'args':args,'seconds':time.time()-start,'exit_code':p.returncode})+'\n')
 if p.returncode: print((results/(name+'.txt')).read_text()[-8000:],flush=True);raise RuntimeError(name)
 print('PASS',name,flush=True)
(results/'health-before.json').write_text(json.dumps(health(),indent=2))
overlay='-overlay='+str(out/'overlay.json')
run('native-unit',['nice','-n','10',go,'test',overlay,'-p','2','./pkg/turbine'])
run('native-race',['nice','-n','10',go,'test',overlay,'-race','-p','2','./pkg/turbine','-run','TestDataShredBatchMatches|TestEntryDecode|TestEntryPrefetch','-count=3'])
run('build',['nice','-n','10',go,'test',overlay,'-c','-p','2','-o',str(candidate),'./pkg/turbine'])
(results/'binaries.json').write_text(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [base,candidate]},indent=2))
bench='^BenchmarkEntryPrefetchAssembly$/^generated_.*$/^workers_2$/^target_8$/.*'
for sample in range(6):
 order=[('baseline',base),('candidate',candidate)] if sample%2==0 else [('candidate',candidate),('baseline',base)]
 for label,binary in order:
  run(f'sample-{sample+1}-{label}',['nice','-n','10','taskset','-c','0-7',str(binary),'-test.run=^$','-test.bench='+bench,'-test.benchtime=5x','-test.count=1','-test.timeout=120s'])
(results/'health-after.json').write_text(json.dumps(health(),indent=2))
print('COMPLETE',flush=True)
