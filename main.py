import pandas as pd
import json
import tqdm



shs_pd = pd.read_csv("list", delimiter='\t', names=['song', 'version', 'title', 'author', 'url', 'dl'])
da_ben_dict = json.load(open("da-tacos_metadata/da-tacos_benchmark_subset_metadata.json"))

da_ben_list = list()
for v0 in tqdm.tqdm(da_ben_dict.values()):
	for v1 in v0.values():
		da_ben_list.append(v1)
da_ben_pd = pd.DataFrame(da_ben_list)
merged1_pd = da_ben_pd.merge(shs_pd, left_on=['perf_title', 'perf_artist'], right_on=['title', 'author'])
residual1_pd = da_ben_pd[~da_ben_pd['perf_id'].isin(merged1_pd['perf_id'])]



train_pd = pd.read_csv("SHS100K-TRAIN", delimiter='\t', names=['work', 'perf'])
__import__('pdb').set_trace() 
