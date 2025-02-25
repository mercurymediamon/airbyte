#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from unittest.mock import mock_open, patch

import pytest
from airbyte_api_client import ApiException
from airbyte_api_client.model.airbyte_catalog import AirbyteCatalog
from airbyte_api_client.model.connection_schedule import ConnectionSchedule
from airbyte_api_client.model.connection_status import ConnectionStatus
from airbyte_api_client.model.namespace_definition_type import NamespaceDefinitionType
from airbyte_api_client.model.resource_requirements import ResourceRequirements
from octavia_cli.apply import resources, yaml_loaders


class TestResourceState:
    def test_init(self, mocker):
        mocker.patch.object(resources, "os")
        state = resources.ResourceState("config_path", "resource_id", 123, "config_hash")
        assert state.configuration_path == "config_path"
        assert state.resource_id == "resource_id"
        assert state.generation_timestamp == 123
        assert state.configuration_hash == "config_hash"
        assert state.path == resources.os.path.join.return_value
        resources.os.path.dirname.assert_called_with("config_path")
        resources.os.path.join.assert_called_with(resources.os.path.dirname.return_value, "state.yaml")

    @pytest.fixture
    def state(self):
        return resources.ResourceState("config_path", "resource_id", 123, "config_hash")

    def test_as_dict(self, state):
        assert state.as_dict() == {
            "configuration_path": state.configuration_path,
            "resource_id": state.resource_id,
            "generation_timestamp": state.generation_timestamp,
            "configuration_hash": state.configuration_hash,
        }

    def test_save(self, mocker, state):
        mocker.patch.object(resources, "yaml")
        mocker.patch.object(state, "as_dict")

        expected_content = state.as_dict.return_value
        with patch("builtins.open", mock_open()) as mock_file:
            state._save()
        mock_file.assert_called_with(state.path, "w")
        resources.yaml.dump.assert_called_with(expected_content, mock_file.return_value)

    def test_create(self, mocker):
        mocker.patch.object(resources.time, "time", mocker.Mock(return_value=0))
        mocker.patch.object(resources.ResourceState, "_save")
        state = resources.ResourceState.create("config_path", "my_hash", "resource_id")
        assert isinstance(state, resources.ResourceState)
        resources.ResourceState._save.assert_called_once()
        assert state.configuration_path == "config_path"
        assert state.resource_id == "resource_id"
        assert state.generation_timestamp == 0
        assert state.configuration_hash == "my_hash"

    def test_from_file(self, mocker):
        mocker.patch.object(resources, "yaml")
        resources.yaml.safe_load.return_value = {
            "configuration_path": "config_path",
            "resource_id": "resource_id",
            "generation_timestamp": 0,
            "configuration_hash": "my_hash",
        }
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            state = resources.ResourceState.from_file("state.yaml")
        resources.yaml.safe_load.assert_called_with(mock_file.return_value)
        assert isinstance(state, resources.ResourceState)
        assert state.configuration_path == "config_path"
        assert state.resource_id == "resource_id"
        assert state.generation_timestamp == 0
        assert state.configuration_hash == "my_hash"


@pytest.fixture
def local_configuration():
    return {
        "exotic_attribute": "foo",
        "configuration": {"foo": "bar"},
        "resource_name": "bar",
        "definition_id": "bar",
        "definition_image": "fooo",
        "definition_version": "barrr",
    }


class TestBaseResource:
    @pytest.fixture
    def patch_base_class(self, mocker):
        # Mock abstract methods to enable instantiating abstract class
        mocker.patch.object(resources.BaseResource, "__abstractmethods__", set())
        mocker.patch.object(resources.BaseResource, "create_function_name", "create_resource")
        mocker.patch.object(resources.BaseResource, "resource_id_field", "resource_id")
        mocker.patch.object(resources.BaseResource, "ResourceIdRequestBody")
        mocker.patch.object(resources.BaseResource, "update_function_name", "update_resource")
        mocker.patch.object(resources.BaseResource, "get_function_name", "get_resource")
        mocker.patch.object(resources.BaseResource, "resource_type", "universal_resource")
        mocker.patch.object(resources.BaseResource, "api")

    def test_init_no_remote_resource(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(resources.BaseResource, "_get_state_from_file", mocker.Mock(return_value=None))
        mocker.patch.object(resources, "hash_config")
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.APPLY_PRIORITY == 0
        assert resource.workspace_id == "workspace_id"
        assert resource.raw_configuration == local_configuration
        assert resource.configuration_path == "bar.yaml"
        assert resource.api_instance == resource.api.return_value
        resource.api.assert_called_with(mock_api_client)
        assert resource.state == resource._get_state_from_file.return_value
        assert resource.remote_resource is None
        assert resource.was_created is False
        assert resource.local_file_changed is True
        assert resource.resource_id is None

    def test_init_with_remote_resource_not_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource, "_get_state_from_file", mocker.Mock(return_value=mocker.Mock(configuration_hash="my_hash"))
        )
        mocker.patch.object(resources.BaseResource, "_get_remote_resource", mocker.Mock(return_value={"resource_id": "my_resource_id"}))

        mocker.patch.object(resources, "hash_config", mocker.Mock(return_value="my_hash"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is False
        assert resource.resource_id == resource.state.resource_id

    def test_init_with_remote_resource_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource,
            "_get_state_from_file",
            mocker.Mock(return_value=mocker.Mock(configuration_hash="my_state_hash")),
        )
        mocker.patch.object(resources.BaseResource, "_get_remote_resource", mocker.Mock(return_value={"resource_id": "my_resource_id"}))
        mocker.patch.object(resources, "hash_config", mocker.Mock(return_value="my_new_hash"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is True
        assert resource.resource_id == resource.state.resource_id

    @pytest.fixture
    def resource(self, patch_base_class, mock_api_client, local_configuration):
        return resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")

    def test_get_remote_resource(self, resource, mocker):
        mocker.patch.object(resource, "_get_fn")
        remote_resource = resource._get_remote_resource()
        assert remote_resource == resource._get_fn.return_value
        resource._get_fn.assert_called_with(resource.api_instance, resource.get_payload)

    @pytest.mark.parametrize(
        "state_path_is_file",
        [True, False],
    )
    def test_get_state_from_file(self, mocker, resource, state_path_is_file):
        mocker.patch.object(resources, "os")
        mock_expected_state_path = mocker.Mock(is_file=mocker.Mock(return_value=state_path_is_file))
        mocker.patch.object(resources, "Path", mocker.Mock(return_value=mock_expected_state_path))
        mocker.patch.object(resources, "ResourceState")
        state = resource._get_state_from_file(resource.configuration_path)
        resources.os.path.dirname.assert_called_with(resource.configuration_path)
        resources.os.path.join.assert_called_with(resources.os.path.dirname.return_value, "state.yaml")
        resources.Path.assert_called_with(resources.os.path.join.return_value)
        if state_path_is_file:
            resources.ResourceState.from_file.assert_called_with(mock_expected_state_path)
            assert state == resources.ResourceState.from_file.return_value
        else:
            assert state is None

    @pytest.mark.parametrize(
        "resource_id",
        [None, "foo"],
    )
    def test_resource_id_request_body(self, mocker, resource_id, resource):
        mocker.patch.object(resources.BaseResource, "resource_id", resource_id)
        if resource_id is None:
            with pytest.raises(resources.NonExistingResourceError):
                resource.resource_id_request_body
                resource.ResourceIdRequestBody.assert_not_called()
        else:
            assert resource.resource_id_request_body == resource.ResourceIdRequestBody.return_value
            resource.ResourceIdRequestBody.assert_called_with(resource_id)

    @pytest.mark.parametrize(
        "was_created",
        [True, False],
    )
    def test_get_diff_with_remote_resource(self, patch_base_class, mocker, mock_api_client, local_configuration, was_created):
        mocker.patch.object(resources.BaseResource, "_get_remote_comparable_configuration")
        mocker.patch.object(resources.BaseResource, "was_created", was_created)
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        mocker.patch.object(resources, "compute_diff")
        if was_created:
            diff = resource.get_diff_with_remote_resource()
            resources.compute_diff.assert_called_with(resource._get_remote_comparable_configuration.return_value, resource.configuration)
            assert diff == resources.compute_diff.return_value.pretty.return_value
        else:
            with pytest.raises(resources.NonExistingResourceError):
                resource.get_diff_with_remote_resource()

    def test_create_or_update(self, mocker, resource):
        expected_results = {resource.resource_id_field: "resource_id"}
        operation_fn = mocker.Mock(return_value=expected_results)
        mocker.patch.object(resources, "ResourceState")
        payload = "foo"
        result, state = resource._create_or_update(operation_fn, payload)
        assert result == expected_results
        assert state == resources.ResourceState.create.return_value
        resources.ResourceState.create.assert_called_with(resource.configuration_path, resource.configuration_hash, "resource_id")

    @pytest.mark.parametrize(
        "response_status,expected_error",
        [(404, ApiException), (422, resources.InvalidConfigurationError)],
    )
    def test_create_or_update_error(self, mocker, resource, response_status, expected_error):
        operation_fn = mocker.Mock(side_effect=ApiException(status=response_status))
        mocker.patch.object(resources, "ResourceState")
        with pytest.raises(expected_error):
            resource._create_or_update(operation_fn, "foo")

    def test_create(self, mocker, resource):
        mocker.patch.object(resource, "_create_or_update")
        assert resource.create() == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._create_fn, resource.create_payload)

    def test_update(self, mocker, resource):
        mocker.patch.object(resource, "_create_or_update")
        assert resource.update() == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._update_fn, resource.update_payload)

    @pytest.mark.parametrize(
        "configuration, invalid_keys, expect_error",
        [
            ({"valid_key": "foo", "invalidKey": "bar"}, {"invalidKey"}, True),
            ({"valid_key": "foo", "invalidKey": "bar", "secondInvalidKey": "bar"}, {"invalidKey", "secondInvalidKey"}, True),
            ({"valid_key": "foo", "validKey": "bar"}, {"invalidKey"}, False),
        ],
    )
    def test__check_for_invalid_configuration_keys(self, configuration, invalid_keys, expect_error):
        if not expect_error:
            result = resources.BaseResource._check_for_invalid_configuration_keys(configuration, invalid_keys, "You have some invalid keys")
            assert result is None
        else:
            with pytest.raises(resources.InvalidConfigurationError, match="You have some invalid keys: ") as error_info:
                resources.BaseResource._check_for_invalid_configuration_keys(configuration, invalid_keys, "You have some invalid keys")
            assert all([invalid_key in str(error_info) for invalid_key in invalid_keys])


class TestSourceAndDestination:
    @pytest.fixture
    def patch_source_and_destination(self, mocker):
        mocker.patch.object(resources.SourceAndDestination, "__abstractmethods__", set())
        mocker.patch.object(resources.SourceAndDestination, "api")
        mocker.patch.object(resources.SourceAndDestination, "create_function_name", "create")
        mocker.patch.object(resources.SourceAndDestination, "update_function_name", "update")
        mocker.patch.object(resources.SourceAndDestination, "get_function_name", "get")
        mocker.patch.object(resources.SourceAndDestination, "_get_state_from_file", mocker.Mock(return_value=None))
        mocker.patch.object(resources, "hash_config")

    def test_init(self, patch_source_and_destination, mocker, mock_api_client, local_configuration):
        assert resources.SourceAndDestination.__base__ == resources.BaseResource
        resource = resources.SourceAndDestination(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.definition_id == local_configuration["definition_id"]
        assert resource.definition_image == local_configuration["definition_image"]
        assert resource.definition_version == local_configuration["definition_version"]

    def test_get_remote_comparable_configuration(self, patch_source_and_destination, mocker, mock_api_client, local_configuration):
        mocker.patch.object(resources.Source, "remote_resource")
        resource = resources.Source(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource._get_remote_comparable_configuration() == resource.remote_resource.connection_configuration


class TestSource:
    @pytest.mark.parametrize(
        "state",
        [None, resources.ResourceState("config_path", "resource_id", 123, "abc")],
    )
    def test_init(self, mocker, mock_api_client, local_configuration, state):
        assert resources.Source.__base__ == resources.SourceAndDestination
        mocker.patch.object(resources.Source, "resource_id", "foo")
        source = resources.Source(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        mocker.patch.object(source, "state", state)
        assert source.api == resources.source_api.SourceApi
        assert source.create_function_name == "create_source"
        assert source.resource_id_field == "source_id"
        assert source.update_function_name == "update_source"
        assert source.resource_type == "source"
        assert source.APPLY_PRIORITY == 0
        assert source.create_payload == resources.SourceCreate(
            source.definition_id, source.configuration, source.workspace_id, source.resource_name
        )
        assert source.update_payload == resources.SourceUpdate(
            source_id=source.resource_id, connection_configuration=source.configuration, name=source.resource_name
        )
        if state is None:
            assert source.get_payload is None
        else:
            assert source.get_payload == resources.SourceIdRequestBody(state.resource_id)

    @pytest.mark.parametrize(
        "resource_id",
        [None, "foo"],
    )
    def test_source_discover_schema_request_body(self, mocker, mock_api_client, resource_id, local_configuration):
        mocker.patch.object(resources, "SourceDiscoverSchemaRequestBody")
        mocker.patch.object(resources.Source, "resource_id", resource_id)
        source = resources.Source(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        if resource_id is None:
            with pytest.raises(resources.NonExistingResourceError):
                source.source_discover_schema_request_body
                resources.SourceDiscoverSchemaRequestBody.assert_not_called()
        else:
            assert source.source_discover_schema_request_body == resources.SourceDiscoverSchemaRequestBody.return_value
            resources.SourceDiscoverSchemaRequestBody.assert_called_with(source.resource_id)

    def test_catalog(self, mocker, mock_api_client, local_configuration):
        mocker.patch.object(resources.Source, "source_discover_schema_request_body")
        source = resources.Source(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        source.api_instance = mocker.Mock()
        catalog = source.catalog
        assert catalog == source.api_instance.discover_schema_for_source.return_value.catalog
        source.api_instance.discover_schema_for_source.assert_called_with(source.source_discover_schema_request_body)


class TestDestination:
    @pytest.mark.parametrize(
        "state",
        [None, resources.ResourceState("config_path", "resource_id", 123, "abc")],
    )
    def test_init(self, mocker, mock_api_client, local_configuration, state):
        assert resources.Destination.__base__ == resources.SourceAndDestination
        mocker.patch.object(resources.Destination, "resource_id", "foo")
        destination = resources.Destination(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        mocker.patch.object(destination, "state", state)
        assert destination.api == resources.destination_api.DestinationApi
        assert destination.create_function_name == "create_destination"
        assert destination.resource_id_field == "destination_id"
        assert destination.update_function_name == "update_destination"
        assert destination.resource_type == "destination"
        assert destination.APPLY_PRIORITY == 0
        assert destination.create_payload == resources.DestinationCreate(
            destination.workspace_id, destination.resource_name, destination.definition_id, destination.configuration
        )
        assert destination.update_payload == resources.DestinationUpdate(
            destination_id=destination.resource_id, connection_configuration=destination.configuration, name=destination.resource_name
        )
        if state is None:
            assert destination.get_payload is None
        else:
            assert destination.get_payload == resources.DestinationIdRequestBody(state.resource_id)


class TestConnection:
    @pytest.fixture
    def connection_configuration(self):
        return {
            "definition_type": "connection",
            "resource_name": "my_connection",
            "source_id": "my_source",
            "destination_id": "my_destination",
            "configuration": {
                "namespace_definition": "customformat",
                "namespace_format": "foo",
                "prefix": "foo",
                "sync_catalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "name_example",
                                "json_schema": {},
                                "supported_sync_modes": ["incremental"],
                                "source_defined_cursor": True,
                                "default_cursor_field": ["default_cursor_field"],
                                "source_defined_primary_key": [["string_example"]],
                                "namespace": "namespace_example",
                            },
                            "config": {
                                "sync_mode": "incremental",
                                "cursor_field": ["cursor_field_example"],
                                "destination_sync_mode": "append_dedup",
                                "primary_key": [["string_example"]],
                                "alias_name": "alias_name_example",
                                "selected": True,
                            },
                        }
                    ]
                },
                "schedule": {"units": 1, "time_unit": "days"},
                "status": "active",
                "resource_requirements": {"cpu_request": "foo", "cpu_limit": "foo", "memory_request": "foo", "memory_limit": "foo"},
            },
        }

    @pytest.fixture
    def legacy_connection_configurations(self):
        return [
            {
                "definition_type": "connection",
                "resource_name": "my_connection",
                "source_id": "my_source",
                "destination_id": "my_destination",
                "configuration": {
                    "namespaceDefinition": "customformat",
                    "namespaceFormat": "foo",
                    "prefix": "foo",
                    "syncCatalog": {
                        "streams": [
                            {
                                "stream": {
                                    "name": "name_example",
                                    "json_schema": {},
                                    "supported_sync_modes": ["incremental"],
                                    "source_defined_cursor": True,
                                    "default_cursor_field": ["default_cursor_field"],
                                    "source_defined_primary_key": [["string_example"]],
                                    "namespace": "namespace_example",
                                },
                                "config": {
                                    "sync_mode": "incremental",
                                    "cursor_field": ["cursor_field_example"],
                                    "destination_sync_mode": "append_dedup",
                                    "primary_key": [["string_example"]],
                                    "alias_name": "alias_name_example",
                                    "selected": True,
                                },
                            }
                        ]
                    },
                    "schedule": {"units": 1, "time_unit": "days"},
                    "status": "active",
                    "resourceRequirements": {"cpu_request": "foo", "cpu_limit": "foo", "memory_request": "foo", "memory_limit": "foo"},
                },
            },
            {
                "definition_type": "connection",
                "resource_name": "my_connection",
                "source_id": "my_source",
                "destination_id": "my_destination",
                "configuration": {
                    "namespace_definition": "customformat",
                    "namespace_format": "foo",
                    "prefix": "foo",
                    "sync_catalog": {
                        "streams": [
                            {
                                "stream": {
                                    "name": "name_example",
                                    "jsonSchema": {},
                                    "supportedSyncModes": ["incremental"],
                                    "sourceDefinedCursor": True,
                                    "defaultCursorField": ["default_cursor_field"],
                                    "sourceDefinedPrimary_key": [["string_example"]],
                                    "namespace": "namespace_example",
                                },
                                "config": {
                                    "syncMode": "incremental",
                                    "cursorField": ["cursor_field_example"],
                                    "destinationSyncMode": "append_dedup",
                                    "primaryKey": [["string_example"]],
                                    "aliasName": "alias_name_example",
                                    "selected": True,
                                },
                            }
                        ]
                    },
                    "schedule": {"units": 1, "time_unit": "days"},
                    "status": "active",
                    "resource_requirements": {"cpu_request": "foo", "cpu_limit": "foo", "memory_request": "foo", "memory_limit": "foo"},
                },
            },
        ]

    @pytest.mark.parametrize(
        "state",
        [None, resources.ResourceState("config_path", "resource_id", 123, "abc")],
    )
    def test_init(self, mocker, mock_api_client, state, connection_configuration):
        assert resources.Connection.__base__ == resources.BaseResource
        mocker.patch.object(resources.Connection, "resource_id", "foo")
        connection = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        mocker.patch.object(connection, "state", state)
        assert connection.api == resources.connection_api.ConnectionApi
        assert connection.create_function_name == "create_connection"
        assert connection.resource_id_field == "connection_id"
        assert connection.update_function_name == "update_connection"
        assert connection.resource_type == "connection"
        assert connection.APPLY_PRIORITY == 1

        assert connection.create_payload == resources.ConnectionCreate(
            name=connection.resource_name,
            source_id=connection.source_id,
            destination_id=connection.destination_id,
            **connection.configuration,
        )
        assert connection.update_payload == resources.ConnectionUpdate(connection_id=connection.resource_id, **connection.configuration)
        if state is None:
            assert connection.get_payload is None
        else:
            assert connection.get_payload == resources.ConnectionIdRequestBody(state.resource_id)

    def test_get_remote_comparable_configuration(self, mocker, mock_api_client, connection_configuration):
        mocker.patch.object(
            resources.Connection,
            "remote_resource",
            mocker.Mock(
                to_dict=mocker.Mock(
                    return_value={
                        "name": "foo",
                        "source_id": "bar",
                        "destination_id": "fooo",
                        "connection_id": "baar",
                        "operation_ids": "foooo",
                        "foo": "bar",
                    }
                )
            ),
        )
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        assert resource._get_remote_comparable_configuration() == {"foo": "bar"}
        resource.remote_resource.to_dict.assert_called_once()

    def test_create(self, mocker, mock_api_client, connection_configuration):
        mocker.patch.object(resources.Connection, "_create_or_update")
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        create_result = resource.create()
        assert create_result == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._create_fn, resource.create_payload)

    def test_update(self, mocker, mock_api_client, connection_configuration):
        mocker.patch.object(resources.Connection, "_create_or_update")
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        resource.state = mocker.Mock(resource_id="foo")
        update_result = resource.update()
        assert update_result == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._update_fn, resource.update_payload)

    def test__deserialize_raw_configuration(self, mock_api_client, connection_configuration):
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        configuration = resource._deserialize_raw_configuration()
        assert isinstance(configuration["sync_catalog"], AirbyteCatalog)
        assert configuration["namespace_definition"] == NamespaceDefinitionType(
            connection_configuration["configuration"]["namespace_definition"]
        )
        assert configuration["schedule"] == ConnectionSchedule(**connection_configuration["configuration"]["schedule"])
        assert configuration["resource_requirements"] == ResourceRequirements(
            **connection_configuration["configuration"]["resource_requirements"]
        )
        assert configuration["status"] == ConnectionStatus(connection_configuration["configuration"]["status"])
        assert list(configuration.keys()) == [
            "namespace_definition",
            "namespace_format",
            "prefix",
            "sync_catalog",
            "schedule",
            "status",
            "resource_requirements",
        ]

    def test__create_configured_catalog(self, mock_api_client, connection_configuration):
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        created_catalog = resource._create_configured_catalog(connection_configuration["configuration"]["sync_catalog"])
        stream, config = (
            connection_configuration["configuration"]["sync_catalog"]["streams"][0]["stream"],
            connection_configuration["configuration"]["sync_catalog"]["streams"][0]["config"],
        )

        assert len(created_catalog.streams) == len(connection_configuration["configuration"]["sync_catalog"]["streams"])
        assert created_catalog.streams[0].stream.name == stream["name"]
        assert created_catalog.streams[0].stream.json_schema == stream["json_schema"]
        assert created_catalog.streams[0].stream.supported_sync_modes == stream["supported_sync_modes"]
        assert created_catalog.streams[0].stream.source_defined_cursor == stream["source_defined_cursor"]
        assert created_catalog.streams[0].stream.namespace == stream["namespace"]
        assert created_catalog.streams[0].stream.source_defined_primary_key == stream["source_defined_primary_key"]
        assert created_catalog.streams[0].stream.default_cursor_field == stream["default_cursor_field"]

        assert created_catalog.streams[0].config.sync_mode == config["sync_mode"]
        assert created_catalog.streams[0].config.cursor_field == config["cursor_field"]
        assert created_catalog.streams[0].config.destination_sync_mode == config["destination_sync_mode"]
        assert created_catalog.streams[0].config.primary_key == config["primary_key"]
        assert created_catalog.streams[0].config.alias_name == config["alias_name"]
        assert created_catalog.streams[0].config.selected == config["selected"]

    def test__check_for_legacy_connection_configuration_keys(
        self, mock_api_client, connection_configuration, legacy_connection_configurations
    ):
        resource = resources.Connection(mock_api_client, "workspace_id", connection_configuration, "bar.yaml")
        assert resource._check_for_legacy_connection_configuration_keys(connection_configuration["configuration"]) is None
        for legacy_configuration in legacy_connection_configurations:
            with pytest.raises(resources.InvalidConfigurationError):
                resource._check_for_legacy_connection_configuration_keys(legacy_configuration["configuration"])


@pytest.mark.parametrize(
    "local_configuration,resource_to_mock,expected_error",
    [
        ({"definition_type": "source"}, "Source", None),
        ({"definition_type": "destination"}, "Destination", None),
        ({"definition_type": "connection"}, "Connection", None),
        ({"definition_type": "not_existing"}, None, NotImplementedError),
    ],
)
def test_factory(mocker, mock_api_client, local_configuration, resource_to_mock, expected_error):
    mocker.patch.object(resources, "yaml")
    if resource_to_mock is not None:
        mocker.patch.object(resources, resource_to_mock)
    resources.yaml.load.return_value = local_configuration
    with patch("builtins.open", mock_open(read_data="data")) as mock_file:
        if not expected_error:
            resource = resources.factory(mock_api_client, "workspace_id", "my_config.yaml")
            resources.yaml.load.assert_called_with(mock_file.return_value, yaml_loaders.EnvVarLoader)
            resource == getattr(resources, resource_to_mock).return_value
            mock_file.assert_called_with("my_config.yaml", "r")
        else:
            with pytest.raises(expected_error):
                resources.factory(mock_api_client, "workspace_id", "my_config.yaml")
                mock_file.assert_called_with("my_config.yaml", "r")
