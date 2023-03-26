"Adapted from https://github.com/sqlalchemy/sqlalchemy/wiki/Query-Plan-SQL-construct"
import contextlib
import os

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable

EXPLAIN_SQL = bool(int(os.getenv("TILED_EXPLAIN_SQL", "0") or "0"))


class _explain(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, stmt, analyze=False):
        self.statement = stmt
        self.analyze = analyze


@compiles(_explain, "postgresql")
def pg_explain(element, compiler, **kw):
    text = "EXPLAIN "
    if element.analyze:
        text += "ANALYZE "
    text += compiler.process(element.statement, **kw)

    return text


@compiles(_explain, "sqlite")
def sqlite_explain(element, compiler, **kw):
    text = "EXPLAIN QUERY PLAN "
    if element.analyze:
        text += "EXPLAIN QUERY PLAN "
    text += compiler.process(element.statement, **kw)

    return text


_query_explanation_callbacks = []


@contextlib.contextmanager
def record_explanations():
    explanations = []

    def capture(e):
        explanations.append(e)

    _query_explanation_callbacks.append(capture)
    yield explanations
    _query_explanation_callbacks.remove(capture)


class ExplainAsyncSession(AsyncSession):
    """
    Extend AsyncSession to explain and then query.

    If EXPLAIN_SQL is off and there are now callbacks, just fall back
    to normal AsyncSession. For performance reasons, we only query
    for the explanation if it will be used.

    Use 'explain' paramete (default: None) to force explain on
    or off (True, False).

    1. Explain the query.
    2. Pass the explanation to the callback(s).
    3. Execute normally.
    """

    async def execute(self, statement, *args, explain=None, **kwargs):
        if explain is None:
            explain = bool(EXPLAIN_SQL or _query_explanation_callbacks)
        if explain:
            explanation = (
                await super().execute(_explain(statement), *args, **kwargs)
            ).all()
            for callback in _query_explanation_callbacks:
                callback(explanation)
        return await super().execute(statement, *args, **kwargs)
