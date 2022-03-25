# (C) Datadog, Inc. 2022-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import json
import logging
import time

import mock
import pytest

try:
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack

from datadog_checks.teradata.check import TeradataCheck

from .common import CHECK_NAME, SERVICE_CHECK_CONNECT, SERVICE_CHECK_QUERY

EXPECTED_TAGS = ["teradata_server:localhost", "teradata_port:1025", "td_env:dev"]


@pytest.mark.parametrize(
    "test_instance, expected_tags, conn_params",
    [
        pytest.param(
            {
                'server': 'localhost',
                'username': 'datadog',
                'password': 'dd_teradata',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": None,
                "logdata": None,
                "user": "datadog",
                "password": "dd_teradata",
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use default options",
        ),
        pytest.param(
            {
                'server': 'td-internal',
                'port': 1125,
                'username': 'dd',
                'password': 'td_datadog',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:td-internal', 'teradata_port:1125'],
            {
                "host": "td-internal",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1125",
                "logmech": None,
                "logdata": None,
                "user": "dd",
                "password": "td_datadog",
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use custom server, db port, and driver path",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'username': 'dd',
                'password': 'td_datadog',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": None,
                "logdata": None,
                "user": "dd",
                "password": "td_datadog",
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use default TLS options",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'https_port': 543,
                'ssl_mode': 'REQUIRE',
                'username': 'dd',
                'password': 'td_datadog',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": None,
                "logdata": None,
                "user": "dd",
                "password": "td_datadog",
                "https_port": "543",
                "sslmode": "REQUIRE",
                "sslprotocol": "TLSv1.2",
            },
            id="Use custom TLS options",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'auth_mechanism': 'JWT',
                'auth_data': 'token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4g'
                'RG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": "JWT",
                "logdata": "token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9"
                "lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
                "user": None,
                "password": None,
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use JWT auth option",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'auth_mechanism': 'KRB5',
                'auth_data': 'dd@localhost@@td_datadog',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": "KRB5",
                "logdata": "dd@localhost@@td_datadog",
                "user": None,
                "password": None,
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use KRB5 auth option",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'auth_mechanism': 'LDAP',
                'auth_data': 'dd@@td_datadog',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": "LDAP",
                "logdata": "dd@@td_datadog",
                "user": None,
                "password": None,
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use LDAP auth option",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'auth_mechanism': 'TDNEGO',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": "TDNEGO",
                "logdata": None,
                "user": None,
                "password": None,
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use TDNEGO auth option",
        ),
        pytest.param(
            {
                'server': 'localhost',
                'username': 'datadog',
                'password': 'dd_teradata',
                'auth_mechanism': 'TD2',
                'database': 'AdventureWorksDW',
            },
            ['teradata_server:localhost', 'teradata_port:1025'],
            {
                "host": "localhost",
                "account": None,
                "database": "AdventureWorksDW",
                "dbs_port": "1025",
                "logmech": "TD2",
                "logdata": None,
                "user": "datadog",
                "password": "dd_teradata",
                "https_port": "443",
                "sslmode": "PREFER",
                "sslprotocol": "TLSv1.2",
            },
            id="Use TD2 auth option (default)",
        ),
    ],
)
def test__connect(test_instance, dd_run_check, aggregator, expected_tags, conn_params):
    """
    Test the _connect method
    """
    check = TeradataCheck(CHECK_NAME, {}, [test_instance])
    conn = mock.MagicMock()
    cursor = conn.cursor()
    cursor.rowcount = float('+inf')

    teradatasql = mock.MagicMock()
    teradatasql.connect.return_value = conn

    mocks = [
        ('datadog_checks.teradata.check.teradatasql', teradatasql),
        ('datadog_checks.teradata.check.TERADATASQL_IMPORT_ERROR', None),
    ]

    with ExitStack() as stack:
        for mock_call in mocks:
            stack.enter_context(mock.patch(*mock_call))
        dd_run_check(check)
        assert check._connection == conn

    teradatasql.connect.assert_called_with(json.dumps(conn_params))
    aggregator.assert_service_check(SERVICE_CHECK_CONNECT, check.OK, tags=expected_tags)
    aggregator.assert_service_check(SERVICE_CHECK_QUERY, check.OK, tags=expected_tags)


def test_import_error(dd_run_check, aggregator, instance, caplog):
    caplog.clear()
    caplog.set_level(logging.ERROR)

    check = TeradataCheck(CHECK_NAME, {}, [instance])
    teradatasql = mock.MagicMock()

    mock_import_error = ImportError()

    mocks = [
        ('datadog_checks.teradata.check.teradatasql', teradatasql),
        ('datadog_checks.teradata.check.TERADATASQL_IMPORT_ERROR', mock_import_error),
    ]

    with ExitStack() as stack:
        for mock_call in mocks:
            stack.enter_context(mock.patch(*mock_call))
        with pytest.raises(Exception, match="raise ImportError\\(TERADATASQL_IMPORT_ERROR\\)"):
            dd_run_check(check)

            assert check._connection is None

            aggregator.assert_service_check(SERVICE_CHECK_CONNECT, check.CRITICAL, tags=EXPECTED_TAGS)
            aggregator.assert_service_check(SERVICE_CHECK_QUERY, check.CRITICAL, tags=EXPECTED_TAGS)

            assert (
                "Teradata SQL Driver module is unavailable. Please double check your installation and refer to the "
                "Datadog documentation for more information." in caplog.text
            )


def test_connection_errors(dd_run_check, aggregator, instance, caplog):
    caplog.clear()
    caplog.set_level(logging.ERROR)
    check = TeradataCheck(CHECK_NAME, {}, [instance])

    conn = mock.Mock(side_effect=Exception("teradatasql.OperationalError"))
    teradatasql = mock.MagicMock()
    teradatasql.connect.side_effect = conn

    mocks = [
        ('datadog_checks.teradata.check.teradatasql', teradatasql),
        ('datadog_checks.teradata.check.TERADATASQL_IMPORT_ERROR', None),
    ]

    with ExitStack() as stack:
        for mock_call in mocks:
            stack.enter_context(mock.patch(*mock_call))
        with pytest.raises(Exception, match="Exception: teradatasql.OperationalError"):
            dd_run_check(check)

            assert check._connection_errors > 0
            assert check._query_errors < 0

            aggregator.assert_service_check(SERVICE_CHECK_CONNECT, check.CRITICAL, tags=EXPECTED_TAGS)
            aggregator.assert_service_check(SERVICE_CHECK_QUERY, check.CRITICAL, tags=EXPECTED_TAGS)


def test_query_errors(dd_run_check, aggregator, instance):
    check = TeradataCheck(CHECK_NAME, {}, [instance])
    query_error = mock.Mock(side_effect=Exception("teradatasql.Error"))
    check._query_manager.executor = mock.MagicMock()
    check._query_manager.executor.side_effect = query_error

    teradatasql = mock.MagicMock()

    mocks = [
        ('datadog_checks.teradata.check.teradatasql', teradatasql),
        ('datadog_checks.teradata.check.TERADATASQL_IMPORT_ERROR', None),
    ]

    with ExitStack() as stack:
        for mock_call in mocks:
            stack.enter_context(mock.patch(*mock_call))
        dd_run_check(check)

    assert check._connection_errors < 1
    assert check._query_errors > 0

    aggregator.assert_service_check(SERVICE_CHECK_CONNECT, check.OK, tags=EXPECTED_TAGS)
    aggregator.assert_service_check(SERVICE_CHECK_QUERY, check.CRITICAL, tags=EXPECTED_TAGS)


def test_no_rows_returned(dd_run_check, aggregator, instance, caplog):
    caplog.clear()
    caplog.set_level(logging.WARNING)
    check = TeradataCheck(CHECK_NAME, {}, [instance])
    conn = mock.MagicMock()
    cursor = conn.cursor()
    cursor.rowcount = 0

    teradatasql = mock.MagicMock()
    teradatasql.connect.return_value = conn

    mocks = [
        ('datadog_checks.teradata.check.teradatasql', teradatasql),
        ('datadog_checks.teradata.check.TERADATASQL_IMPORT_ERROR', None),
    ]

    with ExitStack() as stack:
        for mock_call in mocks:
            stack.enter_context(mock.patch(*mock_call))
        dd_run_check(check)

    assert check._connection_errors < 1
    assert check._query_errors > 0
    assert "Failed to fetch records from query:" in caplog.text
    aggregator.assert_service_check(SERVICE_CHECK_CONNECT, check.OK, tags=EXPECTED_TAGS)
    aggregator.assert_service_check(SERVICE_CHECK_QUERY, check.CRITICAL, tags=EXPECTED_TAGS)


current_time = int(time.time())


@pytest.mark.parametrize(
    "row, expected",
    [
        pytest.param([current_time, 200.5], [current_time, 200.5], id="Valid timestamp"),
        pytest.param(
            [1648093966, 193.0],
            "Resource Usage stats are invalid. Row timestamp is more than 1h in the past. "
            "Is `SPMA` Resource Usage Logging enabled?",
            id="Old timestamp",
        ),
        pytest.param(
            [current_time + 800, 300.3],
            "Row timestamp is more than 10 min in the future. Try checking system time settings.",
            id="Future timestamp",
        ),
        pytest.param(
            ["Not a timestamp", 500],
            "Timestamp `Not a timestamp` is invalid. Skipping row.",
            id="Unable to validate timestamp",
        ),
    ],
)
def test_validate_timestamp(caplog, instance, row, expected):
    check = TeradataCheck(CHECK_NAME, {}, [instance])

    query = "SELECT TOP 1 TheTimestamp, FileLockBlocks FROM DBC.ResSpmaView ORDER BY TheTimestamp DESC;"

    try:
        check._validate_timestamp(row, query)
        assert expected == row
    except Exception:
        assert expected in caplog.text
