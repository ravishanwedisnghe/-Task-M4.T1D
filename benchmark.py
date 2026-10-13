from common import *
import argparse, csv, statistics, shlex
ap=argparse.ArgumentParser()
ap.add_argument('--lights',type=int,default=1000)
ap.add_argument('--hours',type=int,nargs=3,default=[10,100,500])
ap.add_argument('--ranks',type=int,nargs='+',default=[2,4])
ap.add_argument('--repeats',type=int,default=3)
a=ap.parse_args()
assert a.lights>0 and a.repeats>=2 and 0<a.hours[0]<a.hours[1]<a.hours[2]
Path('results').mkdir(exist_ok=True)
fields=['workload','records','mode','ranks','repeats','median_s','min_s','max_s','stdev_s','speedup_vs_seq','speedup_vs_original']
with open('results/performance.csv','w',newline='') as f:
    writer=csv.DictWriter(f,fieldnames=fields); writer.writeheader()
    for label,hours in zip(['small','medium','large'],a.hours):
        file=f'data/traffic_{label}.csv'
        run(['./generate_data',file,str(a.lights),str(hours)],timeout=600)
        ref=signature(run(command('seq',file)))
        medians={}
        for mode,ranks in [('seq',1)]+[(m,r) for r in a.ranks for m in ['original','optimised']]:
            times=[]
            for repeat in range(1,a.repeats+1):
                cmd=command(mode,file,ranks); text=run(cmd,timeout=600)
                assert signature(text)==ref,'Output mismatch'
                times.append(seconds(text))
                Path(f'results/{label}_{mode}_{ranks}_run{repeat}.txt').write_text('COMMAND: '+shlex.join(cmd)+'\n'+text)
            median=statistics.median(times); medians[(mode,ranks)]=median
            row=dict(workload=label,records=a.lights*hours*12,mode=mode,ranks=ranks,repeats=a.repeats,
                     median_s=median,min_s=min(times),max_s=max(times),stdev_s=statistics.stdev(times),
                     speedup_vs_seq=medians[('seq',1)]/median,
                     speedup_vs_original=medians[('original',ranks)]/median if mode=='optimised' else '')
            writer.writerow(row); f.flush(); print(row)
print('Saved results/performance.csv. Inspect runtimes and adjust sizes if necessary.')
