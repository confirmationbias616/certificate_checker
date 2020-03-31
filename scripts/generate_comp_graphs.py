import pandas as pd
import numpy as np
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from utils import create_connection
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import matplotlib
matplotlib.use('Agg')


with create_connection() as conn:
    df = pd.read_sql("""
        SELECT
            SUBSTR(pub_date,0,5)||'-'||SUBSTR(pub_date,6,2) as yearmonth,
            source,
            COUNT(*) as count
        FROM (SELECT DISTINCT title, owner, contractor, engineer, cert_type, source, pub_date FROM web_certificates)
        WHERE pub_date > '2018-01-01'
        GROUP BY source, yearmonth
        ORDER BY pub_date
    """, conn)

df_agg = df.pivot(index='yearmonth', columns='source', values='count').fillna(0)
df_agg = pd.DataFrame(df_agg.to_records())

for source in ['dcn', 'ocn', 'l2b']:
    df_agg[f'{source}_p'] = df_agg.apply(lambda row: row[f'{source}'] * 100 / sum([row['dcn'], row['ocn'], row['l2b']]), axis=1)

plt.figure(figsize=[15,10])
plt.rcParams.update({'font.size': 22})
plt.bar(df_agg.yearmonth, df_agg.dcn, bottom=df_agg.ocn+df_agg.l2b, align='center', color='gray', alpha=1, label='Daily Commercial News')
plt.bar(df_agg.yearmonth, df_agg.ocn, bottom=df_agg.l2b, align='center', color=(112/255, 94/255, 134/255, 1), label='Ontario Construction News')
plt.bar(df_agg.yearmonth, df_agg.l2b, align='center', color=(138/225, 175/225, 190/225, 1), label='Link2Build')
ax = plt.axes()
x_axis = ax.axes.get_xaxis()
x_label = x_axis.get_label()
x_label.set_visible(False)
for spine in ax.spines:
    ax.spines[spine].set_visible(False)
ax.tick_params(axis=u'both', which=u'both',length=0)
ax.yaxis.set_major_locator(MaxNLocator(integer=True))
ax.xaxis.set_major_locator(MaxNLocator(integer=True))
possibly_valid_xticks = (f'{x[0]}-{x[1]}' for x in zip(np.repeat(range(2018, 2100),2), ['06', '12']*100))
valid_xticks = [x for x in possibly_valid_xticks if x in ['2018-06', '2018-02', '2018-12', '2019-06', '2019-12', 'alex']]
plt.xticks(valid_xticks)
plt.locator_params(axis='x', nbins=20)
legend = plt.legend(frameon=1, prop={'size': 20})
frame = legend.get_frame()
frame.set_alpha(0)
plt.title("Quantity of CSP's per source\n")
plt.savefig("static/competition_per_quantity.png", transparent=True)


plt.figure(figsize=[15,10])
plt.rcParams.update({'font.size': 22})
plt.bar(df_agg.yearmonth, df_agg.dcn_p, align='center', alpha=0.2, label='Daily Commercial News', color='gray')
plt.bar(df_agg.yearmonth, df_agg.ocn_p, bottom=df_agg.dcn_p, align='center', alpha=0.5, label='Ontario Construction News', color='blue')
plt.bar(df_agg.yearmonth, df_agg.l2b_p, bottom=df_agg.dcn_p+df_agg.ocn_p, align='center', label='Link2Build', color=(112/255, 94/255, 134/255, 1))
ax = plt.axes()
x_axis = ax.axes.get_xaxis()
x_label = x_axis.get_label()
x_label.set_visible(False)
for spine in ax.spines:
    ax.spines[spine].set_visible(False)
ax.tick_params(axis=u'both', which=u'both',length=0)
ax.yaxis.set_major_locator(MaxNLocator(integer=True))
ax.xaxis.set_major_locator(MaxNLocator(integer=True))
plt.xticks(['2018-04', '2018-08', '2018-12', '2019-04', '2019-08', '2019-12', '2020-04'])
plt.yticks([])
plt.locator_params(axis='x', nbins=20)
legend = plt.legend(loc='center left', frameon=1, prop={'size': 20})
frame = legend.get_frame()
frame.set_alpha(0)
plt.title("Proportion of CSP's per source\n")
plt.savefig("static/competition_per_proportion.png", transparent=True)