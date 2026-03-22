"""
pymssql Cursor.execute() accepts only tuple or dict for bound parameters, not list.
Passing a list raises ValueError and caused silent empty results in execute_query helpers.

IN (...) with bound parameters:
    SQL:  WHERE col IN (%s, %s, %s)
    Args: must be (v1, v2, v3) — same rule as any multi-placeholder query.
    A Python list [v1, v2, v3] must be converted with normalize_params() before execute.

Note: Building IN ('a','b') via string concatenation does not pass a list to execute;
      those literals are embedded in the SQL text (different pattern; watch for injection if
      values are not trusted).
"""


def normalize_params(params):
    """
    Normalize the second argument to pymssql cursor.execute(query, params).

    - list -> tuple (covers single %s, IN (%s,%s,...), OFFSET/FETCH, etc.)
    - tuple, dict -> unchanged
    - None -> None
    """
    if params is None:
        return None
    if isinstance(params, list):
        return tuple(params)
    return params
