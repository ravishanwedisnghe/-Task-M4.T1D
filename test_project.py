from common import *
from collections import defaultdict
import csv, shlex

def oracle(file):
    totals=defaultdict(int)
    with open(file,newline='') as f:
        for r in csv.DictReader(f): totals[(r['timestamp'][:13]+':00',r['traffic_light_id'])]+=int(r['cars'])
    return dict(totals)
def full(text):
    out={}
    for line in text.splitlines():
        if line.startswith('TOTAL '):
            _,hour,light,cars=shlex.split(line); out[(hour,light)]=int(cars)
    return out
Path('results').mkdir(exist_ok=True)
Path('data/empty.csv').write_text('timestamp,traffic_light_id,cars\n')
# Winner split across multiple byte partitions; also test spaces in IDs and tie order.
Path('data/adversarial.csv').write_text('timestamp,traffic_light_id,cars\n'+''.join(
    f'2026-01-01 08:00:00,{light},{cars}\n' for light,cars in
    [('A',8),('B',10),('C',11),('D',12),('A',8),('light with spaces',16),('A',8)]))
logs=[]
for file in ['data/traffic_handcheck.csv','data/traffic_demo.csv','data/traffic_uneven.csv','data/empty.csv','data/adversarial.csv']:
    ref=signature(run(command('seq',file)))
    for ranks in [2,4,8]:
        for mode in ['original','optimised']:
            cmd=command(mode,file,ranks)
            text=run(cmd)
            assert signature(text)==ref,(file,ranks,mode,'ranking mismatch')
            logs.append(shlex.join(cmd)+'\n'+text)
        text=run(command('optimised',file,ranks,['--all-totals']))
        assert full(text)==oracle(file),(file,ranks,'full totals mismatch')
        print('PASS:',file,'ranks=',ranks,'rankings and all totals')
# Malformed input must fail promptly rather than reporting partial success.
Path('data/invalid.csv').write_text('timestamp,traffic_light_id,cars\nbad record\n')
x=subprocess.run(command('optimised','data/invalid.csv',2),capture_output=True,text=True,timeout=20)
assert x.returncode!=0,'Invalid input was accepted'
Path('results/correctness.txt').write_text('\n'.join(logs)+'\nPASS malformed-input rejection\n')
print('ALL TESTS PASSED. Logs: results/correctness.txt')
