#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import pytest

pytestmark = pytest.mark.integration


def test_source_lifecycle(source):
    assert not source.was_created
    source.create()
    source.state = source._get_state_from_file(source.configuration_path)
    assert source.was_created
    assert not source.get_diff_with_remote_resource()
    source.raw_configuration["configuration"]["pokemon_name"] = "snorlax"
    source.configuration = source._deserialize_raw_configuration()
    assert 'changed from "ditto" to "snorlax"' in source.get_diff_with_remote_resource()
    source.update()
    assert not source.get_diff_with_remote_resource()
    assert source.catalog["streams"][0]["config"]["alias_name"] == "pokemon"


def test_destination_lifecycle(destination):
    assert not destination.was_created
    destination.create()
    destination.state = destination._get_state_from_file(destination.configuration_path)
    assert destination.was_created
    assert not destination.get_diff_with_remote_resource()
    destination.raw_configuration["configuration"]["host"] = "foo"
    destination.configuration = destination._deserialize_raw_configuration()
    assert 'changed from "localhost" to "foo"' in destination.get_diff_with_remote_resource()
    destination.update()
    assert not destination.get_diff_with_remote_resource()


def test_connection_lifecycle(source, destination, connection):
    assert source.was_created
    assert destination.was_created
    assert not connection.was_created
    connection.create()
    connection.state = connection._get_state_from_file(connection.configuration_path)
    assert connection.was_created
    assert not connection.get_diff_with_remote_resource()
    connection.raw_configuration["configuration"]["status"] = "inactive"
    connection.configuration = connection._deserialize_raw_configuration()
    assert 'changed from "active" to "inactive"' in connection.get_diff_with_remote_resource()
    connection.update()
    assert not connection.get_diff_with_remote_resource()
