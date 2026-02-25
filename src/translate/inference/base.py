from abc import ABC, abstractmethod
import pandas as pd

class BasePipeline(ABC):
    @abstractmethod
    def __init__(self):
        self._dataset = pd.DataFrame()
        pass

    @abstractmethod
    def set_input(self, dataset: pd.DataFrame):
        pass

    @abstractmethod
    def run(self):
        pass
    
    @property
    def dataset(self) -> pd.DataFrame:
        return self._dataset
    
    @dataset.setter
    def dataset(self, dataset):
        self._dataset = dataset
