import json
from dataclasses import asdict
from query_plan import QueryPlan, FilterPredicate, Aggregate


def plan_to_json(plan: QueryPlan) -> str:
    """
    Converts a QueryPlan object into a JSON string so it can be sent to workers.

    Example QueryPlan object:
    -------------------------
    QueryPlan(
        table="users",
        select_columns=["id", "name"],
        aggregates=[],
        filter=FilterPredicate(column="age", operator=">", value=30),
        group_by=None
    )

    JSON output after plan_to_json():
    --------------------------------
    {
      "table": "users",
      "select_columns": ["id", "name"],
      "aggregates": [],
      "filter": {"column": "age", "operator": ">", "value": 30},
      "group_by": null
    }

    Note:
    asdict() automatically converts nested dataclasses (FilterPredicate, Aggregate)
    into normal dicts so they can be serialized by json.dumps().
    """
    return json.dumps(asdict(plan))



def json_to_plan(json_str: str) -> QueryPlan:
    """
    Converts a JSON string back into a fully reconstructed QueryPlan object,
    including nested dataclasses (FilterPredicate, Aggregate).

    Example JSON input:
    -------------------
    {
      "table": "orders",
      "select_columns": [],
      "aggregates": [
        {"func": "SUM", "column": "amount"},
        {"func": "COUNT", "column": "id"}
      ],
      "filter": {
        "column": "status",
        "operator": "=",
        "value": "COMPLETE"
      },
      "group_by": "customer_id"
    }

    After json_to_plan(), we get:
    -----------------------------
    QueryPlan(
        table='orders',
        select_columns=[],
        aggregates=[
            Aggregate(func='SUM', column='amount'),
            Aggregate(func='COUNT', column='id')
        ],
        filter=FilterPredicate(column='status', operator='=', value='COMPLETE'),
        group_by='customer_id'
    )

    Steps:
    1. Load JSON → dict.
    2. Reconstruct FilterPredicate if present.
    3. Recreate each Aggregate object from list of dicts.
    4. Create final QueryPlan object using unpacked dict (**data).
    """

    data = json.loads(json_str)

    # Reconstruct nested FilterPredicate if present
    if data.get('filter'):
        data['filter'] = FilterPredicate(**data['filter'])

    # Reconstruct list of Aggregate objects 
    if data.get('aggregates'):
        data['aggregates'] = [Aggregate(**agg) for agg in data['aggregates']]

    # Return a fully reconstructed QueryPlan instance
    return QueryPlan(**data)
