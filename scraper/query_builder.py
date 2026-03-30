"""Build Power BI querydata payloads from visual prototype queries."""

from config import DATA_VOLUME, MAX_ROWS_PER_REQUEST


def build_query_payload(
    prototype_query: dict,
    model_id: int,
    db_name: str,
    report_id: str,
    select_count: int,
    restart_tokens: list | None = None,
) -> dict:
    projections = list(range(select_count))

    window: dict = {"Count": MAX_ROWS_PER_REQUEST}
    if restart_tokens:
        window["RestartTokens"] = restart_tokens

    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": prototype_query,
                                "Binding": {
                                    "Primary": {"Groupings": [{"Projections": projections}]},
                                    "DataReduction": {
                                        "DataVolume": DATA_VOLUME,
                                        "Primary": {"Window": window},
                                    },
                                    "Version": 1,
                                },
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": db_name,
                    "Sources": [{"ReportId": report_id}],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": model_id,
    }
