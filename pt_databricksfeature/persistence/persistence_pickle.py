from pathlib import Path
from typing import Dict

import mlflow
from sklearn.base import BaseEstimator
from pt_databricksfeature.base import BasePersistence


class PersistencePickle(BasePersistence):
    end: True

    def save_model(self, object: BaseEstimator, output_path: Path):
        ... # TODO # 

