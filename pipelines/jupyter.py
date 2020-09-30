from json import loads
from os import getenv

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry

from werkzeug.exceptions import NotFound

from .training import get_training
from .utils import remove_ansi_escapes, search_for_pod_name

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


def get_operator_logs(training_id: str, operator_id: str):
    """Retrive logs from a failed operator.

    Args:
        training_id (str): training id
        operator_id (str): operator id

    Raises:
        NotFound: notebook does not exist.

    Returns:
        dict: response.
    """
    operator_endpoint = f"experiments/{training_id}/operators/{operator_id}/Experiment.ipynb"

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

    run_details = get_training(training_id, pretty=False)
    details = loads(run_details.pipeline_runtime.workflow_manifest)
    operator_container = search_for_pod_name(details, operator_id)

    if operator_container['status'] == 'Failed':
        return {"exception": operator_container['message'],
                "traceback": f"Kernel has died: {operator_container['message']}"}

    return {"message": "Notebook finished with status completed"}
