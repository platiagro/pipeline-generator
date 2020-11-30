# -*- coding: utf-8 -*-
import platiagro

import pandas as pd
from werkzeug.exceptions import NotFound

from pipelines.database import db_session
from pipelines.models import Operator
from pipelines.models.utils import raise_if_experiment_does_not_exist


def get_dataset_name(experiment_id, operator_id,):
    """Retrieves a dataset name from experiment.
    Args:
        experiment_id(str): the experiment uuid
        operator_id(str): the operator uuid
    Returns:
        Dataset name
    """
    raise_if_experiment_does_not_exist(experiment_id)

    operator = Operator.query.get(operator_id)
    if operator is None:
        raise NotFound("The specified operator does not exist")

    # get dataset name
    dataset = operator.parameters.get('dataset')
    if dataset is None:
        # try to find dataset name in other operators
        operators = db_session.query(Operator) \
            .filter_by(experiment_id=experiment_id) \
            .filter(Operator.uuid != operator_id) \
            .all()
        for operator in operators:
            dataset = operator.parameters.get('dataset')
            if dataset:
                break
        if dataset is None:
            raise NotFound()
    return dataset


def get_dataset_pagination(application_csv,
                           name,
                           operator_id,
                           page,
                           page_size,
                           run_id):
    """Retrieves a dataset.
    Args:
        application_csv(bool): if is to return dataset as csv
        name(str): the dataset name
        operator_id(str): the operator uuid
        page_size(int) : record numbers
        page(int): page number
        run_id (str): the run id.
    Returns:
        Dataset
    """
    try:
        metadata = platiagro.stat_dataset(name=name, operator_id=operator_id)
        if "run_id" not in metadata:
            raise FileNotFoundError()
        dataset = platiagro.load_dataset(name=name, operator_id=operator_id, run_id=run_id)
    except FileNotFoundError as e:
        raise NotFound(str(e))

    if page_size == -1:
        if application_csv:
            return dataset.to_csv(index=False)
        dataset = dataset.to_dict(orient="split")
        del dataset["index"]
        return dataset
    else:
        dataset = dataset.to_dict(orient="split")
        del dataset["index"]
        pdataset = pagination_datasets(page=page, page_size=page_size, dataset=dataset)
        if application_csv:
            df = pd.DataFrame(columns=pdataset['columns'], data=pdataset['data'])
            return df.to_csv(index=False)
        return pdataset


def pagination_datasets(page, page_size, dataset):
    """pagination of datasets.
    Args:
        page_size(int) : record numbers
        page(int): page number
        dataset(json): data to be paged
    Returns:
        Paged dataset
    """
    try:
        count = 0
        new_datasets = []
        total_elements = len(dataset['data'])
        page = (page * page_size) - page_size
        for i in range(page, total_elements):
            new_datasets.append(dataset['data'][i])
            count += 1
            if page_size == count:
                response = {
                    'columns': dataset['columns'],
                    'data': new_datasets,
                    'total': len(dataset['data'])
                }
                return response
        if len(new_datasets) == 0:
            raise NotFound("The informed page does not contain records")
        else:
            response = {
                'columns': dataset['columns'],
                'data': new_datasets,
                'total': len(dataset['data'])
            }
            return response
    except RuntimeError:
        raise NotFound("The specified page does not exist")
