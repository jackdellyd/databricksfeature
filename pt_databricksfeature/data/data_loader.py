from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class DataLoader:
    def load_dataset(self, input_path: Path) -> pd.DataFrame:
        ...  # TODO #
