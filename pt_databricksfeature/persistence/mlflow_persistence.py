from dataclasses import dataclass
import mlflow
from typing import Dict, Optional


class MLFlowPersistence:
    def __init__(
        self,
        mlflow_uri: str,
        mlflow_experiment_name: str,
        mlflow_databricks_user: Optional[str],
    ) -> None:
        self.mlflow_uri = mlflow_uri
        self.mlflow_experiment_name = mlflow_experiment_name
        self.mlflow_databricks_user = mlflow_databricks_user
        self.__set_experiment()

    def __set_experiment(self):

        if self.mlflow_uri:
            mlflow.set_tracking_uri(self.mlflow_uri)
            if self.mlflow_uri == "databricks":
                if (
                    "az_" not in self.mlflow_databricks_user
                    and "@gap.com" not in self.mlflow_databricks_user
                ):
                    raise ValueError(
                        "The user must be an azure gap mail when using databricks uri "
                    )
                experiment_query = f"/Users/{self.mlflow_databricks_user}/{self.mlflow_experiment_name}"

                experiment = mlflow.get_experiment_by_name(experiment_query)
                if not experiment:
                    experiment_id = mlflow.create_experiment(experiment_query)
                    experiment = mlflow.get_experiment(experiment_id)
                mlflow.set_experiment(experiment.name)
            else:
                if "/" in self.mlflow_experiment_name:
                    raise ValueError(
                        "experiment_name you passed was for databricks user case"
                    )
                mlflow.set_experiment(self.mlflow_experiment_name)

    def __persist_logs(self, metrics: Dict, parameters: Dict):
        mlflow.log_params(parameters)
        mlflow.log_metrics(metrics)

    def persist(self, model, metrics: Dict, parameters: Dict, end=True):

        if mlflow.active_run():
            mlflow.end_run()

        run = mlflow.start_run()
        run_id = run.info.run_id

        mlflow.set_tag("run ID", run_id)

        self.__persist_logs(metrics, parameters)

        mlflow.sklearn.log_model(sk_model=model, artifact_path="model")
        mlflow.autolog()

        if end:
            mlflow.end_run()

    def persist_and_register(self, model_name, model, metrics: Dict, parameters: Dict):
        """model_name: repository name"""
        self.persist(model, metrics, parameters, end=False)
        run_uuid = mlflow.active_run().info.run_uuid
        mlflow.register_model("runs:/{}/{}".format(run_uuid, "model"), model_name)
        mlflow.end_run()
