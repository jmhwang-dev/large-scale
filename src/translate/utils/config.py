from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Type, TypeVar, List
import yaml

from translate.utils.paths import PREPROCESS_CONFIGS_DIR, INFERENCE_CONFIGS_DIR

T = TypeVar("T", bound="BaseConfig")

@dataclass(kw_only=True)
class BaseConfig(ABC):
    src_path: str
    dst_path: str
    inplace: bool = False

    @property
    @abstractmethod
    def config_save_path(self) -> Path:
        pass

    @property
    @abstractmethod
    def config_dict(self) -> dict:
        pass

    def save(self):
        if not self.inplace and Path(self.dst_path).exists():
            raise FileExistsError(f"{self.dst_path} already exists.")
        
        if self.config_save_path.exists():
            with open(self.config_save_path, 'r') as f:
                existing_config = yaml.safe_load(f)

            if existing_config == self.config_dict:
                print(f"[SKIP] Identical config already exists at {self.config_save_path}")
                return

            if not self.inplace:
                raise FileExistsError(
                    f"[ABORT] A different config already exists at {self.config_save_path}. "
                    f"Use `inplace=True` to overwrite."
                )

        with open(self.config_save_path, 'w') as f:
            yaml.dump(self.config_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print(f"[OK] Configuration saved at {self.config_save_path}")


    @classmethod
    def load(cls: Type[T], path: Union[str, Path]) -> T:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"{path} not found")

        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)
        
        return cls(**config_dict)

@dataclass(kw_only=True)
class PreprocessConfig(BaseConfig):
    _config_save_path: Path = field(init=False)
    _config_dict: dict = field(init=False)

    def __post_init__(self):
        stem = Path(self.dst_path).stem
        self._config_save_path = Path(PREPROCESS_CONFIGS_DIR) / f"{stem}.yml"
        self._config_dict = {
            "src_path": self.src_path,
            "dst_path": self.dst_path
        }

    @property
    def config_save_path(self) -> Path:
        return self._config_save_path

    @property
    def config_dict(self) -> dict:
        return self._config_dict

@dataclass(kw_only=True)
class PipelineConfig(BaseConfig):
    dataset_start_index: int
    dataset_end_index: int
    checkpoint: str
    device: str = "cpu"
    initial_batch_size: int

    _config_save_path: Path = field(init=False)
    _config_dict: dict = field(init=False)

    def __post_init__(self):
        if self.device not in ("auto", "cpu", "cuda"):
            raise ValueError("device should be one of 'auto', 'cpu', or 'cuda'.")

        stem = Path(self.dst_path).stem
        self._config_save_path = Path(INFERENCE_CONFIGS_DIR) / f"{stem}.yml"
        self._config_dict = {
            "src_path": self.src_path,
            "dataset_start_index": self.dataset_start_index,
            "dataset_end_index": self.dataset_end_index,
            "dst_path": self.dst_path,
            "checkpoint": self.checkpoint,
            "device": self.device,
            "initial_batch_size": self.initial_batch_size
        }

    @property
    def config_save_path(self) -> Path:
        return self._config_save_path

    @property
    def config_dict(self) -> dict:
        return self._config_dict

@dataclass(kw_only=True)
class TranslatePipelineConfig(PipelineConfig):
    language_from: str
    language_into: str

    def __post_init__(self):
        super().__post_init__()
        self._config_dict.update({
            "language_from": self.language_from,
            "language_into": self.language_into
        })

@dataclass(kw_only=True)
class PostProcessConfig(BaseConfig):
    src_paths: List[str]
    dst_path: str
    src_path: str = field(init=False)

    @property
    def config_save_path(self) -> Path:
        return self._config_save_path

    @property
    def config_dict(self) -> dict:
        return self._config_dict
    
    def __post_init__(self):
        stem = Path(self.dst_path).stem
        self._config_save_path = Path(INFERENCE_CONFIGS_DIR) / f"{stem}.yml"
        self._config_dict = {
            "src_paths": self.src_paths,
            "dst_path": self.dst_path,
        }
