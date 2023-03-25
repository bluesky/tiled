"Adapted from https://github.com/sqlalchemy/sqlalchemy/wiki/Query-Plan-SQL-construct"
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable


class explain(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, stmt, analyze=False):
        self.statement = stmt
        self.analyze = analyze


@compiles(explain, "postgresql")
def pg_explain(element, compiler, **kw):
    text = "EXPLAIN "
    if element.analyze:
        text += "ANALYZE "
    text += compiler.process(element.statement, **kw)

    return text


@compiles(explain, "sqlite")
def sqlite_explain(element, compiler, **kw):
    text = "EXPLAIN QUERY PLAN "
    if element.analyze:
        text += "EXPLAIN QUERY PLAN "
    text += compiler.process(element.statement, **kw)

    return text


class ExplainAsyncSession(AsyncSession):
    """
    Extend AsyncSession to explain and then explain.

    1. Explain the query.
    2. Pass the explanation to the callback.
    3. Execute normally.

    This has has an intentionally simple callback registry.
    Because sessions are short-lived contexts, we do not need to
    deal with the complexity of callback removal.

    This is probably a way to achieve this capability using SQLAlchemy hooks,
    but the hooks involve SQLAlchemy core objects that I do not yet fully
    understand.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__explanation_callbacks = []

    def add_explanation_callback(self, callback):
        self.__explanation_callbacks.append(callback)

    async def execute(self, statement, *args, **kwargs):
        explanation = (await super().execute(explain(statement), *args, **kwargs)).all()
        for callback in self.__explanation_callbacks:
            callback(explanation)
        return await super().execute(statement, *args, **kwargs)
