import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import List
import json
from pathlib import Path

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker


# Load study data once at module level
def _load_studies_data():
    """Load the clinical trials data from JSON file."""
    data_path = Path(__file__).with_name("trialsdata.json")
    with data_path.open("r", encoding="utf-8") as f:
        return json.load(f)["studies"]


STUDIES_DATA = _load_studies_data()



@dataclass
class StudyDescription:
    """Represents a clinical trial study."""
    nct_id: str
    brief_title: str
    overall_status: str
    start_date: str
    completion_date: str
    conditions: List[str]
    organization: str
    brief_summary: str

    @classmethod
    def from_json(cls, study_json: dict) -> "StudyDescription":
        """Parse a study from the JSON data structure."""
        proto = study_json["protocolSection"]
        ident = proto["identificationModule"]
        status = proto["statusModule"]
        desc = proto["descriptionModule"]
        cond = proto["conditionsModule"]

        return cls(
            nct_id=ident["nctId"],
            brief_title=ident["briefTitle"],
            overall_status=status["overallStatus"],
            start_date=status.get("startDateStruct", {}).get("date", "Unknown"),
            completion_date=status.get("completionDateStruct", {}).get("date", "Unknown"),
            conditions=cond.get("conditions", []),
            organization=ident.get("organization", {}).get("fullName", "Unknown"),
            brief_summary=desc.get("briefSummary", ""))

    @ property
    def is_completed(self) -> bool:
        """Check if the study has completed."""
        return self.overall_status in ["COMPLETED", "TERMINATED", "WITHDRAWN"]



@activity.defn
def parse_studies_from_file(filename: str) -> List[StudyDescription]:
    with open(filename, "r", encoding="utf-8") as f:
        raw = json.load(f)
    studies = raw.get("studies", [])
    return [StudyDescription.from_json(s) for s in studies]


@activity.defn
def get_study_status(nct_id: str) -> StudyDescription:
    """Fetch the current status of a clinical trial study."""
    # Find the study in our data
    for study_json in STUDIES_DATA:
        if study_json["protocolSection"]["identificationModule"]["nctId"] == nct_id:
            return StudyDescription.from_json(study_json)

    raise ValueError(f"Study {nct_id} not found")



@activity.defn
def find_studies_by_indication(indication: str) -> List[StudyDescription]:
    """Find all studies that match the given indication/condition."""
    matching_studies = []

    for study_json in STUDIES_DATA:
        proto = study_json["protocolSection"]
        conditions = proto.get("conditionsModule", {}).get("conditions", [])

        # Case-insensitive partial match on conditions
        if any(indication.lower() in cond.lower() for cond in conditions):
            matching_studies.append(StudyDescription.from_json(study_json))

    return matching_studies



@workflow.defn
class MonitorStudyWorkflow:
    def __init__(self) -> None:
        self.status = "None"

    @workflow.query
    async def get_study_status(self) -> StudyDescription:
        return self.status

    @workflow.run
    async def run(self, nct_id: str):
        # Parse the study data once and store it in the workflow

        while 1:
            status = await workflow.execute_activity(
                get_study_status,
                nct_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            self.status = status
            sleep_duration = workflow.random().randint(20, 30)
            await workflow.sleep(sleep_duration)


            #if status.is_completed:
            #    return


            # FIXME: continue-as-new

        workflow.logger.info("Running workflow with parameter %s" % name)
        return await workflow.execute_activity(
            compose_greeting,
            ComposeGreetingInput("Hello", name),
            start_to_close_timeout=timedelta(seconds=10),
        )

@activity.defn
def get_study_status(study_id: str) -> str:
    # ...
    status = ["completed", "recruiting", "not yet recruiting", "terminated", "withdrawn"]
    numberArrary = [1,2,3,4,5,6,7,8,9,10]
    return f"{random.choice(status)}"


@workflow.defn
class MonitorStudiesWorkflow:
    all_studies: List[StudyDescription]

    # update the status of the studies
    @workflow.update
    async def update_studies(self, updated_studies: List[StudyDescription]):
        self.all_studies = updated_studies
        return len(self.all_studies)

    @workflow.run
    async def run(self, filename: str):

        # parse the file once and store it in the workflow
        self.all_studies = await workflow.execute_activity(
            parse_studies_from_file,
            filename,
            start_to_close_timeout=timedelta(seconds=30),
        )

        # loop over the studies and start workflows for each one
        for study in self.all_studies:
            await workflow.execute_child_workflow(MonitorStudyWorkflow, study.nct_id,
                                          id=f"monitor-{study.nct_id}",
                                          task_queue="hello-activity-task-queue",
                                          )

        while 1:
            studies = await workflow.execute_activity(
                find_studies_by_indication,
                indication,
                start_to_close_timeout=timedelta(seconds=10),
            )

            for study in studies:
                workflow.start_workflow(MonitorStudyWorkflow, study.nct_id)

            await workflow.sleep("1 day")

            # FIXME: continue-as-new

        workflow.logger.info("Running workflow with parameter %s" % name)
        return await workflow.execute_activity(
            compose_greeting,
            ComposeGreetingInput("Hello", name),
            start_to_close_timeout=timedelta(seconds=10),
        )


async def main():
    # Uncomment the lines below to see logging output
    # import logging
    # logging.basicConfig(level=logging.INFO)

    # Load configuration
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "127.0.0.1:7233")

    # Start client
    client = await Client.connect(**config)

    # Run a worker for the workflow
    worker = Worker(
        client,
        task_queue="hello-activity-task-queue",
        workflows=[MonitorStudyWorkflow, MonitorStudiesWorkflow],
        activities=[get_study_status, find_studies_by_indication, parse_studies_from_file],
        activity_executor=ThreadPoolExecutor(5),
    )
    await worker.run()
        #workflow.logger.info("Trial monitor worker started")

        # While the worker is running, use the client to run the workflow and
        # print out its result. Note, in many production setups, the client
        # would be in a completely separate process from the worker.
        #result = await client.execute_workflow(
        #    MonitorStudyWorkflow.run,
        #    "STUDIES_DATA",
        #    id="hello-activity-workflow-id",
        #    task_queue="hello-activity-task-queue",
        #)
        #print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
