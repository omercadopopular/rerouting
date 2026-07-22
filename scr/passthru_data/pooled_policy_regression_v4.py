"""Same-sample policy substitution regressions for the final lock-in audit."""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from .config import PipelineConfig
from .io_utils import read_table, write_parquet, write_metadata_json, sha256_file
from .pooled_policy_replication_v4 import analysis_root as policy_root, verification_root
from .trade_regressions import _prepare_event_study, _prepare_dynamic, _run_event_study_one, _run_dynamic_one

VERSION='pooled_policy_regressions_v4'
MODES=('package_full_policy_anchor','independent_paper_full_policy','independent_legal_full_policy')
SPECS=('event','dynamic'); OUTCOMES=('val','q1','p','pduty')

def root(cfg):
    p=cfg.verification_dir/'raw_replication_imports'/VERSION; p.mkdir(parents=True,exist_ok=True); return p
def q(p): return str(p).replace("'","''")
def paths(cfg): return {m: root(cfg)/'panels'/f'{m}.parquet' for m in MODES}

def build_panels(cfg, overwrite=False):
    raw=cfg.verification_dir/'trade_regressions'/'package_benchmark_v5'/'common_sample_v5_cif'/'raw_outcomes_package_policy_cif.parquet'
    ppaths={
      'package_full_policy_anchor':policy_root(cfg)/'package_full_policy_anchor.parquet',
      'independent_paper_full_policy':policy_root(cfg)/'independent_paper_full_policy_panel.parquet',
      'independent_legal_full_policy':policy_root(cfg)/'independent_legal_full_policy_panel.parquet'}
    out={}
    con=duckdb.connect()
    try:
      for mode,dest in paths(cfg).items():
        if dest.exists() and not overwrite: continue
        src=ppaths[mode]; dest.parent.mkdir(parents=True,exist_ok=True)
        tmp=dest.with_name('.'+dest.name+'.tmp'); tmp.unlink(missing_ok=True)
        if mode=='package_full_policy_anchor':
          query=f"SELECT * FROM read_parquet('{q(src)}')"
        else:
          eff='independent_paper_effective_month' if 'paper' in mode else 'independent_legal_effective_month'
          rate='independent_paper_dayweighted_total_tariff' if 'paper' in mode else 'independent_legal_dayweighted_total_tariff'
          status='independent_paper_status' if 'paper' in mode else 'independent_legal_status'
          query=f"""SELECT r.* EXCLUDE(m_effective_mdate2,m_stattariff2,m_status2,m_ess),
            CASE WHEN p.{eff} IS NULL THEN NULL ELSE strptime(p.{eff}||'-01','%Y-%m-%d') END AS m_effective_mdate2,
            p.{rate} AS m_stattariff2, CAST(p.{status} AS TINYINT) AS m_status2,
            CAST(p.{status} AS TINYINT) AS m_ess, '{mode}' AS policy_mode,
            '{q(src)}' AS policy_source
            FROM read_parquet('{q(raw)}') r JOIN read_parquet('{q(src)}') p USING(cty_code,hs10,year,month)"""
        con.execute(f"COPY ({query}) TO '{q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        if con.execute(f"SELECT count(*) FROM read_parquet('{q(tmp)}')").fetchone()[0] <= 0: raise RuntimeError(mode+' empty')
        tmp.replace(dest); out[mode]={'path':str(dest),'rows':int(con.execute(f"SELECT count(*) FROM read_parquet('{q(dest)}')").fetchone()[0]),'sha256':sha256_file(dest)}
    finally: con.close()
    write_metadata_json(root(cfg)/'pooled_policy_regression_v4_panel_manifest.json',{'version':VERSION,'panels':out,'raw_source':str(raw)})
    return out

def _metric(candidate, package, spec, outcome, mode):
    h='event_time' if spec=='event' else 'horizon'; c=candidate.rename(columns={h:'h'}); p=package.rename(columns={h:'h'})
    m=c.merge(p[['h','estimate','std_error']].rename(columns={'estimate':'package_estimate'}),on='h',how='inner')
    d=m.estimate-m.package_estimate
    post=m.loc[m.h>=0]
    return {'source_mode':mode,'spec':spec,'outcome':outcome,'n_horizons':len(m),
      'correlation':float(m.estimate.corr(m.package_estimate)),'rmse':float(np.sqrt(np.mean(d*d))),
      'max_abs_difference':float(np.max(np.abs(d))),
      'post_treatment_sign_agreement':float(np.mean(np.sign(post.estimate)==np.sign(post.package_estimate)))}

def run(cfg, modes=None, specs=None, outcomes=None, overwrite=False):
    modes=modes or list(MODES); specs=specs or list(SPECS); outcomes=outcomes or list(OUTCOMES)
    pp=paths(cfg); coeff=root(cfg)/'coefficients'; coeff.mkdir(parents=True,exist_ok=True); records=[]; fits=[]
    for mode in modes:
      frame=read_table(pp[mode])
      for spec in specs:
        prepared=_prepare_event_study('imports',frame) if spec=='event' else _prepare_dynamic('imports',frame,package_logs=False)
        for outcome in outcomes:
          dest=coeff/mode/spec/f'{outcome}.parquet'; dest.parent.mkdir(parents=True,exist_ok=True)
          if dest.exists() and not overwrite: fit=read_table(dest)
          else:
            res=_run_event_study_one(cfg,'imports',outcome,prepared,mode,str(pp[mode])) if spec=='event' else _run_dynamic_one(cfg,'imports',outcome,prepared,mode,str(pp[mode]))
            fit=res.frame; write_parquet(fit,dest,overwrite=True)
          records.append(fit); fits.append({'fit_id':f'{mode}|{spec}|{outcome}','path':str(dest),'nobs':int(fit.nobs.iloc[0]),'horizons':int(len(fit))})
    allc=pd.concat(records,ignore_index=True); write_parquet(allc,root(cfg)/'pooled_policy_v4_coefficients.parquet',overwrite=True)
    pkgroot=cfg.verification_dir/'trade_regressions'/'package_benchmark_v5'; metrics=[]
    for mode in modes:
      for spec in specs:
       for outcome in outcomes:
        cand=allc[(allc.source_mode==mode)&(allc.spec==spec)&(allc.outcome==outcome)]
        ppfile=pkgroot/f'package_full_{spec}_coefficients.parquet'; pkg=read_table(ppfile); pkg=pkg[(pkg.spec==spec)&(pkg.outcome==outcome)]
        metrics.append(_metric(cand,pkg,spec,outcome,mode))
    met=pd.DataFrame(metrics); write_parquet(met,root(cfg)/'pooled_policy_v4_comparisons.parquet',overwrite=True); met.to_csv(root(cfg)/'pooled_policy_v4_comparisons.csv',index=False)
    write_metadata_json(root(cfg)/'pooled_policy_v4_regression_manifest.json',{'version':VERSION,'fits':fits,'status':'complete' if len(fits)==24 else 'partial'})
    _plot(cfg,allc)
    return {'fits':fits,'metrics':metrics}

def _plot(cfg,coef):
    figroot=root(cfg)/'figures'; figroot.mkdir(parents=True,exist_ok=True)
    pdfref=cfg.verification_dir/'trade_regressions'/'package_benchmark_v5'/'reference'/'package_pdf_reference.parquet'
    ref=read_table(pdfref) if pdfref.exists() else pd.DataFrame()
    for spec in SPECS:
      h='event_time' if spec=='event' else 'horizon'
      fig,axs=plt.subplots(2,2,figsize=(12,8),sharex=True)
      for ax,outcome in zip(axs.ravel(),OUTCOMES):
       pkg=read_table(cfg.verification_dir/'trade_regressions'/'package_benchmark_v5'/f'package_full_{spec}_coefficients.parquet'); pkg=pkg[(pkg.spec==spec)&(pkg.outcome==outcome)]
       if not pkg.empty: ax.plot(pkg[h],pkg.estimate,color='black',label='Package full policy anchor')
       if not ref.empty:
        rr=ref[(ref.spec==spec)&(ref.outcome==outcome)]; ax.scatter(rr.horizon,rr.reference_value,color='grey',s=12,label='Paper PDF reference')
       for mode,color,label in [('package_full_policy_anchor','#1f77b4','Package policy, same raw sample'),('independent_paper_full_policy','#2ca02c','Independent policy, paper clock'),('independent_legal_full_policy','#ff7f0e','Independent policy, legal clock')]:
        x=coef[(coef.source_mode==mode)&(coef.spec==spec)&(coef.outcome==outcome)]
        if not x.empty: ax.plot(x[h],x.estimate,color=color,marker='o',ms=2,label=label)
       ax.axhline(0,color='k',lw=.5); ax.set_title(outcome); ax.grid(alpha=.2)
      axs[0,0].legend(fontsize=7); fig.suptitle(f'Historical policy substitution: {spec}'); fig.tight_layout(); fig.savefig(figroot/f'pooled_policy_v4_{spec}.png',dpi=160); fig.savefig(figroot/f'pooled_policy_v4_{spec}.pdf'); plt.close(fig)

def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--build-panels',action='store_true'); ap.add_argument('--run',action='store_true'); ap.add_argument('--overwrite',action='store_true'); ap.add_argument('--mode',choices=[*MODES,'all'],default='all'); ap.add_argument('--spec',choices=[*SPECS,'all'],default='all'); ap.add_argument('--outcome',choices=[*OUTCOMES,'all'],default='all'); args=ap.parse_args(); cfg=PipelineConfig.default()
 if args.build_panels: print(json.dumps(build_panels(cfg,overwrite=args.overwrite),indent=2))
 if args.run:
  modes=None if args.mode=='all' else [args.mode]; specs=None if args.spec=='all' else [args.spec]; outcomes=None if args.outcome=='all' else [args.outcome]
  print(json.dumps(run(cfg,modes=modes,specs=specs,outcomes=outcomes,overwrite=args.overwrite),indent=2,default=str))
 return 0
if __name__=='__main__': raise SystemExit(main())
