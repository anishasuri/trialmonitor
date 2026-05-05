Run:
samples-python % temporal workflow start \
  --type MonitorStudiesWorkflow \
  --task-queue hello-activity-task-queue \
  --workflow-id monitor-studies-demo \
  --input '"hello/trialsdata.json"'



# Trial Monitor Demo (`trial_monitor.py`)

This demo shows how Temporal can orchestrate long-running clinical trial monitoring workflows using data from `trialsdata.json`. This is for demo purposes. Original data can be fetched from Clinicaltrials.gov
API: https://clinicaltrials.gov/data-api/api.

## What This Demo Shows

- A **parent workflow** (`MonitorStudiesWorkflow`) that parses all studies from JSON.
- A **child workflow** (`MonitorStudyWorkflow`) per study.
- Activities that:
  - parse study data from a file,
  - fetch a study status,
  - find studies by indication.

## Files Used

- `hello/trial_monitor.py`
- `trialsdata.json`

## High-Level Flow

1. Worker starts and listens on task queue `hello-activity-task-queue`.
2. `MonitorStudiesWorkflow` loads studies from JSON.
3. For each study, it starts a child workflow `MonitorStudyWorkflow`.
4. Child workflow periodically checks status and sleeps between checks.

---

## Prerequisites

- Python deps installed for this repo (`uv sync` from repo root).
# Trial Monitor Demo (hello/trial_monitor.py)

This demo shows how to run a Temporal worker for clinical trial monitoring and view parent/child workflows in Temporal UI.

## What You Will See

1. A parent workflow, `MonitorStudiesWorkflow`, started with a JSON file input.
2. Child workflows, one per study, with workflow IDs in the format `monitor-<NCT_ID>`.
3. Continuous workflow activity visible in Temporal UI.

## Prerequisites

- Python 3.10+ (this repo uses uv)
- uv installed: https://docs.astral.sh/uv/
- Temporal CLI installed: https://docs.temporal.io/cli#install

## Run Trial Monitor End-to-End

Open three terminals from the repository root.

Terminal 1: install dependencies (first run only) and start Temporal dev server

```bash
uv sync
temporal server start-dev
```

Expected: Temporal server listens on `127.0.0.1:7233` and UI is available at `http://localhost:8233`.

Terminal 2: start the worker

```bash
source .venv/bin/activate
uv run hello/trial_monitor.py
```

This worker polls task queue `hello-activity-task-queue` and hosts:

- `MonitorStudiesWorkflow`
- `MonitorStudyWorkflow`
- activities used by both workflows

Terminal 3: start the parent workflow

```bash
temporal workflow start \
  --type MonitorStudiesWorkflow \
  --task-queue hello-activity-task-queue \
  --workflow-id monitor-studies-demo \
  --input '"hello/trialsdata.json"'
```

Notes:

- Run this command from the repo root so `hello/trialsdata.json` resolves correctly.
- If you use an absolute path, make sure it exists on your machine.

## Verify in Temporal UI

1. Open `http://localhost:8233`.
2. Namespace should be `default`.
3. Find workflow ID `monitor-studies-demo`.
4. Open it and inspect child workflows. You should see IDs such as `monitor-NCT00558935`.

## Useful Commands

List recent workflows:

```bash
temporal workflow list
```

Show one workflow:

```bash
temporal workflow describe --workflow-id monitor-studies-demo
```

Terminate and restart if needed:

```bash
temporal workflow terminate --workflow-id monitor-studies-demo --reason "restart"
```

## Troubleshooting

`FileNotFoundError` for trialsdata.json

- Cause: workflow input points to a path that does not exist on your machine.
- Fix: start workflow with `--input '"hello/trialsdata.json"'` from repo root.

Workflow does not appear in UI

- Ensure Temporal server is running (`temporal server start-dev`).
- Ensure worker is running (`uv run hello/trial_monitor.py`).
- Ensure task queue matches exactly: `hello-activity-task-queue`.

Worker starts but no progress

- Check worker terminal for activity/workflow errors.
- Confirm namespace is `default` in UI and CLI.

    class and can access class state.
  * [hello_activity_multiprocess](hello/hello_activity_multiprocess.py) - Execute a synchronous activity on a process
    pool.
  * [hello_activity_retry](hello/hello_activity_retry.py) - Demonstrate activity retry by failing until a certain number
    of attempts.
  * [hello_activity_threaded](hello/hello_activity_threaded.py) - Execute a synchronous activity on a thread pool.
  * [hello_async_activity_completion](hello/hello_async_activity_completion.py) - Complete an activity outside of the
    function that was called.
  * [hello_cancellation](hello/hello_cancellation.py) - Manually react to cancellation inside workflows and activities.
  * [hello_change_log_level](hello/hello_change_log_level.py) - Change the level of workflow task failure from WARN to ERROR.
  * [hello_child_workflow](hello/hello_child_workflow.py) - Execute a child workflow from a workflow.
  * [hello_continue_as_new](hello/hello_continue_as_new.py) - Use continue as new to restart a workflow.
  * [hello_cron](hello/hello_cron.py) - Execute a workflow once a minute.
  * [hello_exception](hello/hello_exception.py) - Execute an activity that raises an error out of the workflow and out
    of the program.
  * [hello_local_activity](hello/hello_local_activity.py) - Execute a local activity from a workflow.
  * [hello_mtls](hello/hello_mtls.py) - Accept URL, namespace, and certificate info as CLI args and use mTLS for
    connecting to server.
  * [hello_parallel_activity](hello/hello_parallel_activity.py) - Execute multiple activities at once.
  * [hello_query](hello/hello_query.py) - Invoke queries on a workflow.
  * [hello_search_attributes](hello/hello_search_attributes.py) - Start workflow with search attributes then change
    while running.
  * [hello_signal](hello/hello_signal.py) - Send signals to a workflow.
  * [hello update](hello/hello_update.py) - Send a request to and a response from a client to a workflow execution.
<!-- Keep this list in alphabetical order -->
* [activity_worker](activity_worker) - Use Python activities from a workflow in another language.
* [batch_sliding_window](batch_sliding_window) - Batch processing with a sliding window of child workflows.
* [bedrock](bedrock) - Orchestrate a chatbot with Amazon Bedrock.
* [cloud_export_to_parquet](cloud_export_to_parquet) - Set up schedule workflow to process exported files on an hourly basis
* [context_propagation](context_propagation) - Context propagation through workflows/activities via interceptor.
* [custom_converter](custom_converter) - Use a custom payload converter to handle custom types.
* [custom_decorator](custom_decorator) - Custom decorator to auto-heartbeat a long-running activity.
* [custom_metric](custom_metric) - Custom metric to record the workflow type in the activity schedule to start latency.
* [dsl](dsl) - DSL workflow that executes steps defined in a YAML file.
* [eager_wf_start](eager_wf_start) - Run a workflow using Eager Workflow Start
* [encryption](encryption) - Apply end-to-end encryption for all input/output.
* [env_config](env_config) - Load client configuration from TOML files with programmatic overrides.
* [gevent_async](gevent_async) - Combine gevent and Temporal.
* [hello_standalone_activity](hello_standalone_activity) - Use activities without using a workflow.
* [langchain](langchain) - Orchestrate workflows for LangChain.
* [langgraph_plugin](langgraph_plugin) - Run LangGraph workflows as durable Temporal workflows (Graph API and Functional API).
* [message_passing/introduction](message_passing/introduction/) - Introduction to queries, signals, and updates.
* [message_passing/safe_message_handlers](message_passing/safe_message_handlers/) - Safely handling updates and signals.
* [message_passing/update_with_start/lazy_initialization](message_passing/update_with_start/lazy_initialization/) - Use update-with-start to update a Shopping Cart, starting it if it does not exist.
* [Nexus Messaging](nexus_messaging): Demonstrates how send signal, update and query messages through Nexus.
  This contains two samples, one sending messages to an existing workflow and a second that creates a workflow through Nexus
  and sends messages to it.
* [open_telemetry](open_telemetry) - Trace workflows with OpenTelemetry.
* [patching](patching) - Alter workflows safely with `patch` and `deprecate_patch`.
* [polling](polling) - Recommended implementation of an activity that needs to periodically poll an external resource waiting its successful completion.
* [prometheus](prometheus) - Configure Prometheus metrics on clients/workers.
* [workflow_streams](workflow_streams) - Workflow-hosted durable event stream via `temporalio.contrib.workflow_streams`. **Experimental**
* [pydantic_converter](pydantic_converter) - Data converter for using Pydantic models.
* [schedules](schedules) - Demonstrates a Workflow Execution that occurs according to a schedule.
* [sentry](sentry) - Report errors to Sentry.
* [trio_async](trio_async) - Use asyncio Temporal in Trio-based environments.
* [updatable_timer](updatable_timer) - A timer that can be updated while sleeping.
* [worker_specific_task_queues](worker_specific_task_queues) - Use unique task queues to ensure activities run on specific workers.
* [worker_versioning](worker_versioning) - Use the Worker Versioning feature to more easily version your workflows & other code.
* [worker_multiprocessing](worker_multiprocessing) - Leverage Python multiprocessing to parallelize workflow tasks and other CPU bound operations by running multiple workers.

## Test

To run the tests:

    uv run poe test
