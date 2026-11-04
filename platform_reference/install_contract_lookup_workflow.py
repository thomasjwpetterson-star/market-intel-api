"""Install/undo only the lookup stage on the existing candidate workflow.

Snapshot the current workflow and role first. A rollback removes our two states
and exact task permission, preserving subsequent unrelated workflow changes.
"""
import argparse
import hashlib
import json
from pathlib import Path

import boto3

from .contract_lookup_workflow import add_lookup_stage, remove_lookup_stage

STATE_MACHINE = "arn:aws:states:us-east-1:868631722720:stateMachine:mimir-etl-refresh"
ROLE = "mimir-etl-shadow-EtlStateMachineRole-DMNn6DYfh7J2"
POLICY = "RunEtlTasks"


def activate(task, journal, *, rollback=False):
    if not task.startswith("arn:aws:ecs:us-east-1:868631722720:task-definition/mimir-contract-lookup:"):
        raise ValueError("Unexpected lookup task family/account/region")
    session = boto3.Session(region_name="us-east-1")
    sf, iam, ecs = (session.client(name) for name in ("stepfunctions", "iam", "ecs"))
    definition = ecs.describe_task_definition(taskDefinition=task)["taskDefinition"]
    container = definition["containerDefinitions"]
    if (definition["cpu"] != "2048" or definition["memory"] != "4096" or
            definition.get("ephemeralStorage", {}).get("sizeInGiB") != 50 or
            len(container) != 1 or container[0].get("memory") != 3072):
        raise ValueError("Lookup worker exceeds the approved resource envelope")
    current = sf.describe_state_machine(stateMachineArn=STATE_MACHINE)
    before = json.loads(current["definition"])
    after = (remove_lookup_stage(before, task) if rollback else add_lookup_stage(before, task))
    validation = sf.validate_state_machine_definition(definition=json.dumps(after), type="STANDARD", severity="ERROR")
    if validation["result"] != "OK":
        raise ValueError(validation)
    policy = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    updated = json.loads(json.dumps(policy))
    statements = [s for s in updated["Statement"] if "ecs:RunTask" in
                  ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])]
    if len(statements) != 1 or not isinstance(statements[0]["Resource"], list):
        raise ValueError("Unexpected ETL task permission structure")
    resources = statements[0]["Resource"]
    if rollback:
        statements[0]["Resource"] = [arn for arn in resources if arn != task]
    elif task not in resources:
        resources.append(task)
    if journal.exists():
        raise FileExistsError("Choose a new activation journal")
    journal.parent.mkdir(parents=True, exist_ok=True)
    record = {"state_machine": STATE_MACHINE, "task_definition": task, "rollback": rollback,
              "before_definition": before, "after_definition": after,
              "before_policy": policy, "after_policy": updated, "state": "PREPARED"}
    journal.write_text(json.dumps(record, indent=2)+"\n")
    # Stop on concurrent edits instead of restoring a stale global definition.
    latest = sf.describe_state_machine(stateMachineArn=STATE_MACHINE)
    if latest["revisionId"] != current["revisionId"]:
        raise RuntimeError("Workflow changed during preparation; rebase this additive patch")
    latest_policy = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    if latest_policy != policy:
        raise RuntimeError("Task permissions changed during preparation")
    if not rollback:
        iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(updated))
    response = sf.update_state_machine(stateMachineArn=STATE_MACHINE, definition=json.dumps(after))
    if rollback:
        iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(updated))
    observed = sf.describe_state_machine(stateMachineArn=STATE_MACHINE)
    if json.loads(observed["definition"]) != after:
        raise RuntimeError("Workflow update differs from prepared patch; inspect journal")
    if iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"] != updated:
        raise RuntimeError("Task permission update was not verified")
    record.update(state="ROLLED_BACK" if rollback else "ACTIVE", revision_id=observed["revisionId"])
    journal.write_text(json.dumps(record, indent=2)+"\n")
    return {key: record[key] for key in ("state", "state_machine", "task_definition", "revision_id")}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--task-definition", required=True)
    p.add_argument("--journal", required=True, type=Path)
    p.add_argument("--rollback", action="store_true")
    args = p.parse_args()
    print(json.dumps(activate(args.task_definition, args.journal, rollback=args.rollback)), flush=True)
