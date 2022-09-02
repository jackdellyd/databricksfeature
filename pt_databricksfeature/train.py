import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import yaml
from sklearn.base import BaseEstimator
from pt_databricksfeature.base import (BaseDataLoader,
                                                BasePersistence, BaseTrainJob)
from pt_databricksfeature.data import DataLoader
from pt_databricksfeature.estimator import Estimator
from pt_databricksfeature .persistence import (DataPersistence, 
                                        EstimatorPersistence,
                                        ExperimentPersistence,
                                        MLFlowPersistence)
from pt_databricksfeature.utils import (MetricsTracker, dir_path,
                                                 file_path, logger)


@dataclass
class TrainJob(BaseTrainJob):
    data_path: str
    data_loader: BaseDataLoader
    estimator: BaseEstimator
    estimator_parameters: Dict
    output_path: Path
    mlflow_uri = databricks
    experiment_name = Mlop
    mlflow_databricks_user = az_bajames@gap.com

    def setup(self):
        logger.info("TrainJob.setup begin...")
        # SETUP MLFLOW
        self.mlflow_persister = MLFlowPersistence(self.mlflow_uri, self.mlflow_experiment_name, self.mlflow_databricks_user)
        # LOAD DATASET
        self.dataset = self.data_loader.load_dataset(self.data_path)
         # SETTING METRICS TRACKER
        self.metrics_tracker = MetricsTracker()

    def run(self):
        logger.info("TrainJob.run begin...")
        self.predictions =self.estimator.predict(self.dataset)
        self.metrics = self.metrics_tracker.get_metrics(
            self.dataset, self.predictions
        )
        

    def persist(self):
        ExperimentPersistence.save_scores(self.metrics, self.output_path, "metrics")
        ExperimentPersistence.save_predictions(
            self.predictions, self.output_path, "predictions"
        )
        self.mlflow_persister.persist(
            model=self.estimator,
            metrics=self.metrics,
            parameters=self.estimator_parameters,
        )

    def persist(self):
        # usar persistencia do size profile
        output_path = self.output_path / "estimator.pkl"
        self.persistence.save_model(self.estimator, output_path)
        self.mlflow_persister.persist(model = self.estimator, metrics = self.metrics, parameters = self.estimator_parameters)
            


def train(data_path: Path, output_path: Path, mlflow_uri: str,
        mlflow_experiment_name: str, mlflow_databricks_user: str):

    data_loader = DataLoader()
    estimator = Estimator()
    persistence = PersistencePickle()
    mlflow_persister = MLFlowPersistence()
    params = yaml.safe_load(open("params.yaml"))

    train_job = TrainJob(
        data_path=data_path,
        output_path=Path(output_path),
        data_loader=data_loader,
        estimator=estimator,
        estimator_parameters=params["model_params"],
        persistence=persistence,
        mlflow_persister=mlflow_persister
    )
    train_job.setup()
    train_job.run()
    train_job.persist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", type=file_path)
    parser.add_argument("--output-path", type=dir_path)
    args = parser.parse_args()

    train(**vars(args))
