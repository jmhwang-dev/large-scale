import os
from typing import List

import pandas as pd

from translate.utils import PostProcessConfig, get_dataset
from translate.utils.paths import INFERENCE_ARTIFACTS_DIR

def merge_results(src_paths: List[str], dst_prefix: str) -> None:
    if len(src_paths) == 1:
        return 

    gather_config = PostProcessConfig(
        src_paths=src_paths,
        dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, f'{dst_prefix}.tsv'),
        inplace=True
    )

    gather_config.save()

    df_list = []
    for src_path in src_paths:
        df, _ = get_dataset(src_path)
        df_list.append(df)
    
    merged_df = pd.concat(df_list, axis=0, ignore_index=True)
    merged_df.to_csv(gather_config.dst_path, sep='\t', index=False)
