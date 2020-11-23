from json import loads, JSONDecodeError
from os import getenv
from re import compile, sub

from minio.error import NoSuchKey
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry
from werkzeug.exceptions import NotFound

from pipelines.controllers.utils import remove_ansi_escapes, search_for_pod_name
from pipelines.object_storage import BUCKET_NAME, get_object


JUPYTER_ENDPOINT = getenv("JUPYTER_ENDPOINT", "http://server.anonymous:80/notebook/anonymous/server")
URL_CONTENTS = f"{JUPYTER_ENDPOINT}/api/contents"

COOKIES = {"_xsrf": "token"}
HEADERS = {"content-type": "application/json", "X-XSRFToken": "token"}

SESSION = Session()
SESSION.cookies.update(COOKIES)
SESSION.headers.update(HEADERS)
SESSION.hooks = {
    "response": lambda r, *args, **kwargs: r.raise_for_status(),
}
RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "PUT", "OPTIONS", "DELETE"]
)
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
SESSION.mount("http://", ADAPTER)


def get_operator_logs(experiment_id: str, operator_id: str):
    """Retrive logs from a failed operator.
    Args:
        experiment_id (str): experiment id
        operator_id (str): operator id
    Raises:
        NotFound: notebook does not exist.
    Returns:
        dict: response.
    """
    from pipelines.controllers.experiment_runs import get_experiment_run

    operator_endpoint = f"experiments/{experiment_id}/operators/{operator_id}/Experiment.ipynb"
    try:
        r = SESSION.get(url=f"{URL_CONTENTS}/{operator_endpoint}").content
        notebook_content = loads(r.decode("utf-8"))["content"]
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 404:
            raise NotFound("The specified notebook does not exist")

    for cell in notebook_content["cells"]:
        try:
            metadata = cell["metadata"]["papermill"]

            if metadata["exception"] and metadata["status"] == "failed":
                for output in cell["outputs"]:
                    if output["output_type"] == "error":
                        error_log = output["traceback"]
                        traceback = remove_ansi_escapes(error_log)

                        return {"exception": output["ename"], "traceback": traceback}
        except KeyError:
            pass

    run_details = get_experiment_run(experiment_id, pretty=False)
    details = loads(run_details.pipeline_runtime.workflow_manifest)
    operator_container = search_for_pod_name(details, operator_id)

    if operator_container['status'] == 'Failed':
        return {"exception": operator_container['message'],
                "traceback": [f"Kernel has died: {operator_container['message']}"]}

    return {"message": "Notebook finished with status completed"}


def read_parameters(path):
    """Lists the parameters declared in a notebook.
    Args:
        path (str): path to the .ipynb file.
    Returns:
        list: a list of parameters (name, default, type, label, description).
    """
    if not path:
        return []

    object_name = path[len(f"minio://{BUCKET_NAME}/"):]
    try:
        experiment_notebook = loads(get_object(object_name).decode("utf-8"))
    except (NoSuchKey, JSONDecodeError):
        return []

    parameters = []
    cells = experiment_notebook.get("cells", [])
    for cell in cells:
        cell_type = cell["cell_type"]
        tags = cell["metadata"].get("tags", [])
        if cell_type == "code" and "parameters" in tags:
            source = cell["source"]

            parameters.extend(
                read_parameters_from_source(source),
            )

    return parameters


def read_parameters_from_source(source):
    """Lists the parameters declared in source code.
    Args:
        source (list): source code lines.
    Returns:
        list: a list of parameters (name, default, type, label, description).
    """
    parameters = []
    # Regex to capture a parameter declaration
    # Inspired by Google Colaboratory Forms
    # Example of a parameter declaration:
    # name = "value" #@param ["1st option", "2nd option"] {type:"string", label:"Foo Bar", description:"Foo Bar"}
    pattern = compile(r"^(\w+)\s*=\s*(.+)\s+#@param(?:(\s+\[.*\]))?(\s+\{.*\})")

    for line in source:
        match = pattern.search(line)
        if match:
            try:
                name = match.group(1)
                default = match.group(2)
                options = match.group(3)
                metadata = match.group(4)

                parameter = {"name": name}

                if default and default != 'None':
                    if default in ["True", "False"]:
                        default = default.lower()
                    parameter["default"] = loads(default)

                if options:
                    parameter["options"] = loads(options)

                # adds quotes to metadata keys
                metadata = sub(r"(\w+):", r'"\1":', metadata)
                parameter.update(loads(metadata))

                parameters.append(parameter)
            except JSONDecodeError:
                pass

    return parameters
