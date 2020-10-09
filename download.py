import os, re, logging, youtube_dl
import pandas as pd
import tqdm
import multiprocessing as mp
import glob


class MyLogger(object):
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        with open('download', 'a') as fp:
            fp.write('False\n')
        logging.info(msg.encode('UTF-8'))


def my_hook(d):
    if d['status'] == 'finished':
        with open('download', 'a') as fp:
            fp.write('True\n')


def download_data(path, name, uri):
    # data/set_id version_id http://...
    if not os.path.exists(path):
        os.mkdir(path)
    ydl_opts = {
        'format': 'bestaudio/best',
        'logger': MyLogger(),
        'outtmpl': os.path.join(path, name + '.%(ext)s'),  # '.%(ext)s' 音频文件扩展名
        'ignoreerrors': True,
        'progress_hooks': [my_hook],
        }
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([uri])
    return name



if __name__ == "__main__":
    url_pd = pd.read_csv("tmp/get_uri_tmp.csv", names=['perf_id', 'yt', 'sp'])
    pool = mp.Pool(64)
    for row_idx, row in url_pd.iterrows():
        pool.apply_async(download_data, ('da-tacos_benchmark_wav', row['perf_id'], row['yt']), callback=lambda res:print(res))
    pool.close()
    pool.join()

