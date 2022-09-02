from dataclasses import dataclass
from typing import Dict
import pandas as pd
import numpy as np
from nptyping import NDArray
from sklearn.base import BaseEstimator

from pt_databricksfeature.utils import logger


@dataclass
class Estimator(BaseEstimator):
    data: pd.DataFrame
    model_name: str = "pt_databricksfeature"
    
    def predict(self, X: NDArray) -> NDArray:
        ... # TODO # 
