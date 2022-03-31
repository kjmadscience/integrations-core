import psycopg2
from cachetools import TTLCache

try:
    import datadog_agent
except ImportError:
    from ..stubs import datadog_agent

from datadog_checks.base.utils.db.utils import (
    DBMAsyncJob,
)

REMOTE_COMMANDS = [
    {
        "command_id": 1,
        "command_type": "kill_query",
        "host": "dbm-agent-integration-postgres-12.cfxxae8cilce.us-east-1.rds.amazonaws.com",
        "postgres_pid": 5769,
    }
]


class PostgresCommandRunner(DBMAsyncJob):
    def __init__(self, check, config, shutdown_callback):
        collection_interval = 1
        super(PostgresCommandRunner, self).__init__(
            check,
            rate_limit=1 / collection_interval,
            run_sync=False,
            enabled=True,
            dbms="postgres",
            min_collection_interval=collection_interval,
            config_host=config.host,
            expected_db_exceptions=(psycopg2.errors.DatabaseError,),
            job_name="command-runner",
            shutdown_callback=shutdown_callback,
        )
        self._check = check
        self._config = config
        self._command_cache = TTLCache(
            maxsize=1000,
            ttl=600
        )

    def _get_commands(self):
        self._log.info("fetching agent commands")
        valid_commands = []
        for c in REMOTE_COMMANDS:
            if c['host'] != self._config.host:
                self._log.warn("wrong host, skipping command. expected host=%s got host=%s", self._config.host, c['host'])
                continue
            if c['command_id'] in self._command_cache:
                self._log.info("skipping already executed command_id=%s", c['command_id'])
                continue
            self._command_cache[c['command_id']] = True
            valid_commands.append(c)
        return valid_commands

    def _kill_query(self, pid):
        self._log.info("killing postgres pid. pid=%s", pid)
        with self._check._get_db(self._config.dbname).cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            self._log.info("running query SELECT datadog.pg_terminate_backend(%s)", pid)
            cursor.execute("SELECT datadog.pg_terminate_backend(%s)" % pid)
            self._log.info("successfully killed pid=%s", pid)

    def _explain_analyze(self, query_signature):
        self._log.info("running explain analyze. query_signature=%s", query_signature)

    def run_job(self):
        commands = self._get_commands()
        for command in commands:
            if command['command_type'] == 'kill_query':
                self._kill_query(command['postgres_pid'])
            elif command['command_type'] == 'explain_analyze':
                self._explain_analyze(command['query_signature'])
            else:
                self._log.error("invalid command_type: %s", command)
            self._check.count(
                "dd.postgres.command_run.run",
                1,
                tags=self._tags + ["command_type:{}".format(command['command_type'])] + self._check._get_debug_tags(),
                hostname=self._check.resolved_hostname,
            )
