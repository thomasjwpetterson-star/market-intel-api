"""Small additive ASL patch; preserve independently maintained ETL states."""
from copy import deepcopy


def add_lookup_stage(definition, task_definition):
    result = deepcopy(definition)
    states = result["States"]
    choice = states["CheckUnifiedV7Exit"]["Choices"][0]
    if (choice.get("Variable") != "$.unified_v7_result.exit_code" or
            choice.get("NumericEquals") != 0 or
            choice.get("Next") not in {"CandidateRehearsalFinished", "BuildContractLookup"}):
        raise ValueError("Final v7 transition changed; review integration")
    template = states["BuildUnifiedV7Candidate"]
    task = deepcopy(template)
    task["Parameters"]["TaskDefinition"] = task_definition
    task["Parameters"]["Overrides"]["ContainerOverrides"] = [{
        "Name": "mimir-etl", "Command.$": "States.Array('--bucket','a-and-d-intel-lake-newaccount','--run-id',$.run_id)"}]
    task["ResultPath"] = "$.contract_lookup_result"
    task["TimeoutSeconds"] = 5400
    task["Next"] = "CheckContractLookupExit"
    states["BuildContractLookup"] = task
    states["CheckContractLookupExit"] = {"Type": "Choice", "Choices": [{
        "Variable": "$.contract_lookup_result.exit_code", "NumericEquals": 0,
        "Next": "CandidateRehearsalFinished"}], "Default": "NotifyFailure"}
    choice["Next"] = "BuildContractLookup"
    return result


def remove_lookup_stage(definition, expected_task_definition):
    result = deepcopy(definition)
    states = result["States"]
    task = states.get("BuildContractLookup", {})
    if task.get("Parameters", {}).get("TaskDefinition") != expected_task_definition:
        raise ValueError("Lookup stage changed; refusing to undo another change")
    transition = states["CheckUnifiedV7Exit"]["Choices"][0]
    if transition["Next"] != "BuildContractLookup":
        raise ValueError("Lookup transition changed")
    transition["Next"] = "CandidateRehearsalFinished"
    del states["BuildContractLookup"]
    del states["CheckContractLookupExit"]
    return result
