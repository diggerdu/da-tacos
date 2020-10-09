import pandas as pd
import json
import tqdm
import requests
import multiprocessing as mp


def get_uri(perf_id):
	res = requests.get(f"https://secondhandsongs.com/performance/{perf_id.split('_')[-1]}?format=json")
	crt_yt_uri = crt_sp_uri = ""
	external_uri_list = res.json().get('external_uri')
	if external_uri_list is None:
		external_uri_list = list()
	for ele in external_uri_list:
		if 'youtube' in ele['site']:
			crt_yt_uri = ele['uri']
		elif 'spotify' in ele['site']:
			crt_sp_uri = ele['uri']
		else:
			raise NotImplementedError

	tmp_fn = open("tmp/get_uri_tmp.csv", 'a')
	if crt_yt_uri or crt_sp_uri:
		print(f"{perf_id},{crt_yt_uri},{crt_sp_uri}", file=tmp_fn)
	tmp_fn.flush()
	tmp_fn.close()
	return crt_yt_uri, crt_sp_uri

if __name__ == "__main__":
	da_ben_dict = json.load(open("da-tacos_metadata/da-tacos_benchmark_subset_metadata.json"))
	da_ben_list = list()
	for subdict in tqdm.tqdm(da_ben_dict.values()):
		for v in subdict.values():
			da_ben_list.append(v)
	da_ben_pd = pd.DataFrame(da_ben_list)

	yt_url_list = list()
	sp_url_list = list()

	tmp_pd = pd.read_csv("tmp/get_uri_tmp.csv", names=['perf_id', 'yt', 'sp'])


	pool = mp.Pool(32)
	perf_id_list = da_ben_pd[~da_ben_pd['perf_id'].isin(tmp_pd['perf_id'])]['perf_id'].tolist()
	print(f"###perf id lens{len(perf_id_list)}####")
	res_list = pool.map(get_uri, perf_id_list)


		
