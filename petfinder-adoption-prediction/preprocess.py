import os

import pandas as pd
import dask.dataframe as dd

data_root = '/home/hao.zhang/project/pycharm/ludwig-ray/ludwig-petfinder/petfinder-adoption-prediction/data'

df = pd.read_csv(os.path.join(data_root, 'train/train.csv'))


def get_image_path(petid):
    fname = os.path.join(data_root, f'train_images/{petid}-1.jpg')
    return fname if os.path.exists(fname) else None


df['Photo'] = df.PetID.map(lambda petid: get_image_path(petid))

df.to_csv(os.path.join(data_root, 'train_combined.csv'))
