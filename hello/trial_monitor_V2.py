import argparse
import asyncio
import json
import logging
import platform
import ssl
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import certifi

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

API_BASE_URL = "https://clinicaltrials.gov/api/v2"
TASK_QUEUE = "trial-monitor-v2-task-queue"
DEFAULT_POLL_INTERVAL_SECONDS = 300
DEFAULT_MAX_POLLS_PER_RUN = 100


@dataclass
class StudyDescription:
    nct_id: str
    brief_title: str
    overall_status: str
    start_date: str
    completion_date: str
    conditions: list[str]
    organization: str
    brief_summary: str

    @property
    def is_terminal(self) -> bool:
        return self.overall_status.upper() in {
            "COMPLETED",
            "TERMINATED",
            "WITHDRAWN",
        }


@dataclass
class SearchStudiesInput:
    condition: str
    max_studies: int = 10


@dataclass
class MonitorStudiesInput:
    condition: str
    max_studies: int = 10
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    max_polls_per_run: int = DEFAULT_MAX_POLLS_PER_RUN


@dataclass
class MonitorStudyInput:
    nct_id: str
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    poll_count: int = 0
    max_polls_per_run: int = DEFAULT_MAX_POLLS_PER_RUN


def _build_ssl_context() -> ssl.SSLContext:
    """
    Build an SSL context that trusts the OS keychain (important on corporate
    networks with TLS inspection proxies such as Netskope/Zscaler) as well as
    the certifi bundle as a fallback.
    """
    ctx = ssl.create_default_context(cafile=certifi.where())

    if platform.system() == "Darwin":
        # Export the macOS system + root keychains and load them so that
        # corporate proxy CA certificates (e.g. Netskope) are trusted.
        try:
            keychains = [
                "/Library/Keychains/System.keychain",
                "/System/Library/Keychains/SystemRootCertificates.keychain",
            ]
            pem_chunks: list[str] = []
            for kc in keychains:
                result = subprocess.run(
                    ["security", "find-certificate", "-a", "-p", kc],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0 and result.stdout:
                    pem_chunks.append(result.stdout)

            if pem_chunks:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".pem", delete=False
                ) as tmp:
                    # Write certifi CAs first, then system CAs
                    with open(certifi.where()) as f:
                        tmp.write(f.read())
                    tmp.write("\n".join(pem_chunks))
                    tmp_path = tmp.name

                ctx = ssl.create_default_context(cafile=tmp_path)
        except Exception as exc:
            logging.warning("Could not load macOS keychain certs: %s", exc)

    return ctx


# Build once at module level so activities share the context
_SSL_CONTEXT = _build_ssl_context()


def _fetch_api_json(path: str, params: Optional[dict[str, object]] = None) -> dict:
    url = f"{API_BASE_URL}{path}"
    if params:
        filtered = {k: v for k, v in params.items() if v is not None}
        url = f"{url}?{urlencode(filtered)}"

    request = Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "trialmonitor-v2-demo/1.0",
        },
    )

    with urlopen(request, timeout=30, context=_SSL_CONTEXT) as response:
        return json.loads(response.read().decode("utf-8"))


def _study_from_api(study_json: dict) -> StudyDescription:
    protocol = study_json.get("protocolSection", {})
    ident = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    desc = protocol.get("descriptionModule", {})
    cond = protocol.get("conditionsModule", {})
    sponsors = protocol.get("sponsorCollaboratorsModule", {})

    organization = (
        ident.get("organization", {}).get("fullName")
        or sponsors.get("leadSponsor", {}).get("name")
        or "Unknown"
    )

    return StudyDescription(
        nct_id=ident.get("nctId", ""),
        brief_title=ident.get("briefTitle", "Unknown"),
        overall_status=status.get("overallStatus", "UNKNOWN"),
        start_date=status.get("startDateStruct", {}).get("date", "Unknown"),
        completion_date=status.get("completionDateStruct", {}).get("date", "Unknown"),
        conditions=cond.get("conditions", []),
        organization=organization,
        brief_summary=desc.get("briefSummary", ""),
    )


@activity.defn
def search_studies(input: SearchStudiesInput) -> list[StudyDescription]:
    """
    Search studies by condition using the ClinicalTrials.gov API.

    This keeps the demo focused on API usage instead of reading from a local JSON file.
    """
    studies: list[StudyDescription] = []
    next_page_token: Optional[str] = None

    while len(studies) < input.max_studies:
        page_size = min(100, input.max_studies - len(studies))
        params: dict[str, object] = {
            "query.cond": input.condition,
            "pageSize": page_size,
            "format": "json",
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        payload = _fetch_api_json("/studies", params)
        page_items = payload.get("studies", [])

        if not page_items:
            break

        for raw_study in page_items:
            study = _study_from_api(raw_study)
            if study.nct_id:
                studies.append(study)
                if len(studies) >= input.max_studies:
                    break

        next_page_token = payload.get("nextPageToken")
        if not next_page_token:
            break

    # Deduplicate by NCT ID while preserving order
    deduped: dict[str, StudyDescription] = {}
    for study in studies:
        deduped.setdefault(study.nct_id, study)

    return list(deduped.values())[: input.max_studies]


@activity.defn
def fetch_study_by_nct_id(nct_id: str) -> StudyDescription:
    """
    Fetch a single study by NCT ID from the ClinicalTrials.gov API.
    """
    payload = _fetch_api_json(f"/studies/{quote(nct_id)}", {"format": "json"})

    # Some endpoints return the study directly; this keeps parsing defensive.
    if "studies" in payload:
        studies = payload.get("studies", [])
        if not studies:
            raise ValueError(f"Study {nct_id} not found")
        return _study_from_api(studies[0])

    return _study_from_api(payload)


@workflow.defn
class MonitorStudyWorkflow:
    def __init__(self) -> None:
        self.latest_study: Optional[StudyDescription] = None
        self.poll_count = 0

    @workflow.query
    def get_latest_study(self) -> Optional[StudyDescription]:
        return self.latest_study

    @workflow.run
    async def run(self, input: MonitorStudyInput) -> StudyDescription:
        self.poll_count = input.poll_count

        while True:
            study = await workflow.execute_activity(
                fetch_study_by_nct_id,
                input.nct_id,
                start_to_close_timeout=timedelta(seconds=30),
            )
            self.latest_study = study

            workflow.logger.info(
                "Polled study %s with status %s",
                study.nct_id,
                study.overall_status,
            )

            if study.is_terminal:
                workflow.logger.info(
                    "Study %s reached terminal state %s",
                    study.nct_id,
                    study.overall_status,
                )
                return study

            self.poll_count += 1
            if self.poll_count >= input.max_polls_per_run:
                workflow.continue_as_new(
                    MonitorStudyInput(
                        nct_id=input.nct_id,
                        poll_interval_seconds=input.poll_interval_seconds,
                        poll_count=0,
                        max_polls_per_run=input.max_polls_per_run,
                    )
                )

            await workflow.sleep(timedelta(seconds=input.poll_interval_seconds))


@workflow.defn
class MonitorStudiesWorkflow:
    def __init__(self) -> None:
        self.started_workflow_ids: list[str] = []

    @workflow.query
    def get_started_workflow_ids(self) -> list[str]:
        return self.started_workflow_ids

    @workflow.run
    async def run(self, input: MonitorStudiesInput) -> list[str]:
        studies = await workflow.execute_activity(
            search_studies,
            SearchStudiesInput(
                condition=input.condition,
                max_studies=input.max_studies,
            ),
            start_to_close_timeout=timedelta(seconds=60),
        )

        workflow.logger.info(
            "Found %d studies for condition=%s",
            len(studies),
            input.condition,
        )

        for study in studies:
            child_workflow_id = f"trial-monitor-{study.nct_id}"

            await workflow.start_child_workflow(
                MonitorStudyWorkflow.run,
                MonitorStudyInput(
                    nct_id=study.nct_id,
                    poll_interval_seconds=input.poll_interval_seconds,
                    poll_count=0,
                    max_polls_per_run=input.max_polls_per_run,
                ),
                id=child_workflow_id,
                task_queue=TASK_QUEUE,
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
            self.started_workflow_ids.append(child_workflow_id)

        return self.started_workflow_ids


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "127.0.0.1:7233")
    client = await Client.connect(**config)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[MonitorStudyWorkflow, MonitorStudiesWorkflow],
        activities=[search_studies, fetch_study_by_nct_id],
        activity_executor=ThreadPoolExecutor(max_workers=10),
    )
    logging.info("Trial monitor worker started on task queue: %s", TASK_QUEUE)
    await worker.run()


async def start_parent_workflow(
    condition: str,
    max_studies: int,
    poll_interval_seconds: int,
    workflow_id: str,
) -> None:
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "127.0.0.1:7233")
    client = await Client.connect(**config)

    result = await client.execute_workflow(
        MonitorStudiesWorkflow.run,
        MonitorStudiesInput(
            condition=condition,
            max_studies=max_studies,
            poll_interval_seconds=poll_interval_seconds,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    print("Started child workflows:")
    for workflow_id in result:
        print(f" - {workflow_id}")


async def start_single_study_workflow(
    nct_id: str,
    poll_interval_seconds: int,
) -> None:
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "127.0.0.1:7233")
    client = await Client.connect(**config)

    handle = await client.start_workflow(
        MonitorStudyWorkflow.run,
        MonitorStudyInput(
            nct_id=nct_id,
            poll_interval_seconds=poll_interval_seconds,
        ),
        id=f"trial-monitor-{nct_id}",
        task_queue=TASK_QUEUE,
    )

    print(f"Started workflow {handle.id} for {nct_id}")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="ClinicalTrials.gov API + Temporal trial monitor v2"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Run Temporal worker")

    parent_parser = subparsers.add_parser(
        "start-parent",
        help="Search ClinicalTrials.gov and fan out one workflow per NCT ID",
    )
    parent_parser.add_argument("--condition", required=True)
    parent_parser.add_argument("--max-studies", type=int, default=5)
    parent_parser.add_argument("--poll-interval-seconds", type=int, default=300)
    parent_parser.add_argument(
        "--workflow-id",
        default="trial-monitor-parent",
    )

    one_parser = subparsers.add_parser(
        "start-one",
        help="Start monitoring a single NCT ID",
    )
    one_parser.add_argument("--nct-id", required=True)
    one_parser.add_argument("--poll-interval-seconds", type=int, default=300)

    args = parser.parse_args()

    if args.command == "worker":
        await run_worker()
        return

    if args.command == "start-parent":
        await start_parent_workflow(
            condition=args.condition,
            max_studies=args.max_studies,
            poll_interval_seconds=args.poll_interval_seconds,
            workflow_id=args.workflow_id,
        )
        return

    if args.command == "start-one":
        await start_single_study_workflow(
            nct_id=args.nct_id,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    asyncio.run(main())
