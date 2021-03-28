import logging

import pandas as pd
import dask.dataframe as dd
from ludwig.api import LudwigModel
from ludwig.utils.data_utils import replace_file_extension

csv_path = "./data/train_combined.csv"

# dataset_to_use = replace_file_extension(csv_path, 'parquet')
# pd.read_csv(csv_path).to_parquet(
#     dataset_to_use,
#     index=False
# )

dataset_to_use = dd.read_csv(csv_path)


model = LudwigModel(
    # config='./config/large.yaml',
    config='./config/small.yaml',
    logging_level=logging.INFO,
    backend='ray')

# model = LudwigModel(
#     # config='./config/large.yaml',
#     config='./config/small.yaml',
#     logging_level=logging.INFO)

(
    train_stats,
    preprocessed_data,
    output_directory
) = model.train(dataset=dataset_to_use)
