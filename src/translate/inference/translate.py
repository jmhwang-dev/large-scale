from .base import BasePipeline
from translate.utils.config import TranslatePipelineConfig

from transformers.models.auto.tokenization_auto import AutoTokenizer
from transformers.pipelines import pipeline

import torch
import gc
import time
import pandas as pd


class Translator(BasePipeline):
    def __init__(self, config: TranslatePipelineConfig):        
        self.config = config
        self.batch_size = self.config.initial_batch_size

        # 모델에 기본 tokenizer가 지정되어 있지 않을수가 있으므로 명시적으로 지정
        tokenizer = AutoTokenizer.from_pretrained(self.config.checkpoint)
        tokenizer.padding_side = "left"

        self.pipeline = pipeline(
            "text-generation",
            model=self.config.checkpoint,
            tokenizer=tokenizer,
            torch_dtype=torch.bfloat16,
            device_map=self.config.device
        )

    def set_input(self, dataset: pd.DataFrame):
        self.dataset = dataset
        self.prompts = [
            [{
                "role": "user",
                "content": f"Translate the following text from {self.config.language_from} into {self.config.language_into}.\n{self.config.language_from}: {text.values[0]}\n{self.config.language_into}:"
            }]
            for _, text in self.dataset.iterrows()
        ]

    def run(self):
        self.start_index = 0
        self.end_index = 0
        dataset_size = len(self.prompts)

        while self.start_index < dataset_size:
            self.end_index = min(self.start_index + self.batch_size, dataset_size)
            chunk = self.prompts[self.start_index:self.end_index]

            try:
                start_time = time.time()
                outputs = self.pipeline(
                    chunk,
                    max_new_tokens=512,
                    do_sample=False,
                    batch_size=len(chunk)
                )

                duration = time.time() - start_time
                print(f"[{self.config.device}] Processing batch: {self.end_index}/{dataset_size} - Time: {duration:.2f}s.")

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
            new_size = min(200, self.batch_size + delta)
        print(f"[{self.config.device}] Batch size {'increased' if delta > 0 else 'decreased'}: {self.batch_size} → {new_size}")
        self.batch_size = new_size

    def save_results(self, outputs):
        results = [
            out[0]['generated_text'][-1]['content'] for out in outputs
        ]
        results = list(map(lambda x: x.strip(), results))

        results_df = pd.DataFrame(
            results,
            columns=['por2eng']
        )

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

def run_translator(config: TranslatePipelineConfig, dataset):
    translatore = Translator(config)
    translatore.set_input(dataset)
    translatore.run()
