from .base import BasePipeline
from translate.utils.config import PipelineConfig

from transformers.pipelines import pipeline

import torch
import gc
import time
import pandas as pd

class SentimentAnalyzer(BasePipeline):
    def __init__(self, config:PipelineConfig):
        self.config = config
        self.batch_size = self.config.initial_batch_size
        self.pipeline = pipeline(
            "text-classification",
            model=self.config.checkpoint,
            device=self.config.device,
            top_k=3,

            max_length=512,
            truncation=False,
            )

    def set_input(self, dataset: pd.DataFrame):
        self.dataset = dataset
        self.prompts = list(dataset['por2eng'])

    def run(self):
        self.start_index = 0
        self.end_index = 0
        dataset_size = len(self.prompts)

        while self.start_index < dataset_size:
            self.end_index = min(self.start_index + self.batch_size, dataset_size)
            chunk = self.prompts[self.start_index:self.end_index]

            try:
                start_time = time.time()
                outputs = self.pipeline(chunk, batch_size=len(chunk))
                duration = time.time() - start_time
                print(f"[{self.config.device}] Processing batch: {self.end_index}/{dataset_size} - Time: {duration:.2f}s")
                self.save_results(outputs)
                self.start_index = self.end_index
                self.adjust_batch_size(+1)

            except torch.cuda.OutOfMemoryError:
                print(f"[{self.config.device}] ⚠️ Out of Memory. Reducing batch size.")
                self.adjust_batch_size(-1)
                torch.cuda.empty_cache()
                gc.collect()

    def adjust_batch_size(self, delta: int):
        if self.config.device != 'cpu':
            new_size = max(1, self.batch_size + delta)
        else:
            # Limit maximum batch size for CPU inference
            new_size = min(1000, self.batch_size + delta)
        print(f"[{self.config.device}] Batch size {'increased' if delta > 0 else 'decreased'}: {self.batch_size} → {new_size}")
        self.batch_size = new_size

    def save_results(self, outputs):
        rows = [{item['label']: item['score'] for item in row} for row in outputs]
        results_df = pd.DataFrame(rows)
    
        current_dataset = self.dataset[self.start_index:self.end_index]
        current_dataset.reset_index(inplace=True, drop=True)
        merged_df = pd.concat([current_dataset, results_df], axis=1)
    
        try:
            existing_df = pd.read_csv(self.config.dst_path, sep='\t')
        except FileNotFoundError:
            df = merged_df
        else:
            df = pd.concat([existing_df, merged_df], ignore_index=True)

        df.to_csv(self.config.dst_path, sep='\t', index=False)

def run_sentiment(config: PipelineConfig, dataset):
    analyzeor = SentimentAnalyzer(config)
    analyzeor.set_input(dataset)
    analyzeor.run()
