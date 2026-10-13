from common import *
import argparse, signal, time, shlex
ap=argparse.ArgumentParser()
ap.add_argument('--lights',type=int,default=1000)
ap.add_argument('--hours',type=int,default=5000)
ap.add_argument('--ranks',type=int,default=4)
ap.add_argument('--timeout',type=float,default=60)
a=ap.parse_args()
assert a.lights>0 and a.hours>0 and a.ranks>0 and a.timeout>0
file='data/traffic_stress.csv'
print('Generating stress input; this can take time and disk space.',flush=True)
run(['./generate_data',file,str(a.lights),str(a.hours)],timeout=1800)
cmd=command('optimised',file,a.ranks)
Path('results').mkdir(exist_ok=True)
start=time.monotonic()
with open('results/stress_output.txt','w') as f:
    proc=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    try:
        code=proc.wait(timeout=a.timeout); outcome='COMPLETED' if code==0 else 'FAILED'
    except subprocess.TimeoutExpired:
        outcome='DNF: imposed timeout'; os.killpg(proc.pid,signal.SIGTERM)
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: os.killpg(proc.pid,signal.SIGKILL); proc.wait()
        code=proc.returncode
elapsed=time.monotonic()-start
text=f'COMMAND: {shlex.join(cmd)}\nInput records: {a.lights*a.hours*12}\nTime limit: {a.timeout} seconds\nObserved wall time: {elapsed:.3f} seconds\nOutcome: {outcome}\nExit code: {code}\n'
Path('results/stress_summary.txt').write_text(text); print(text)
if outcome=='COMPLETED': print('This run is NOT qualifying DNF evidence. Increase the workload and test again.')
