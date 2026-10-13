import os, re, shlex, subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
os.chdir(ROOT)
MPI=shlex.split(os.environ.get('MPI_LAUNCH','mpirun --bind-to none'))
def command(program,file,ranks=2,extra=()):
    if program=='seq': return ['./traffic_sim','seq',str(file),'3']
    return MPI+['-np',str(ranks),'./traffic_mpi_'+program,str(file),'3','1','1','1024']+list(extra)
def run(cmd,timeout=180):
    x=subprocess.run(cmd,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=timeout)
    if x.returncode: raise RuntimeError('Command failed: '+shlex.join(cmd)+'\n'+x.stdout)
    return x.stdout
def signature(text):
    return [s for s in text.splitlines() if s.startswith(('Hour:','Rank ','Records processed:','Hours processed:')) and not re.match(r'Rank \d+ records:',s)]
def seconds(text):
    return float(re.search(r'Execution time: ([\d.]+) seconds',text)[1])
