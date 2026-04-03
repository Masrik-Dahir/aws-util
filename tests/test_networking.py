"""Tests for aws_util.networking -- 100% line coverage."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import aws_util.networking as mod
from aws_util.networking import VPCConnectivityResult, vpc_connectivity_manager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "Op",
    )


def _mock_ec2(**overrides: object) -> MagicMock:
    """Return a MagicMock that behaves like an EC2 client."""
    ec2 = MagicMock()
    for k, v in overrides.items():
        setattr(ec2, k, v)
    return ec2


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestVPCConnectivityResult:
    def test_create(self) -> None:
        r = VPCConnectivityResult(
            connectivity_type="peering",
            connection_id="pcx-123",
            status="active",
            routes_created=["rt-1 -> 10.0.0.0/16 via pcx-123"],
            security_group_rules_added=["sg-1: tcp/443 from 10.0.0.0/16"],
            dns_records_created=[],
        )
        assert r.connectivity_type == "peering"
        assert r.connection_id == "pcx-123"
        assert r.status == "active"

    def test_frozen(self) -> None:
        r = VPCConnectivityResult(
            connectivity_type="peering",
            connection_id="pcx-123",
            status="active",
            routes_created=[],
            security_group_rules_added=[],
            dns_records_created=[],
        )
        with pytest.raises(Exception):
            r.status = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _add_routes
# ---------------------------------------------------------------------------


class TestAddRoutes:
    def test_success(self) -> None:
        ec2 = _mock_ec2()
        result = mod._add_routes(
            ec2, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
        )
        assert len(result) == 1
        assert "rt-1" in result[0]
        assert "pcx-1" in result[0]

    def test_already_exists(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_route.side_effect = _client_error("RouteAlreadyExists")
        result = mod._add_routes(
            ec2, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
        )
        assert "already exists" in result[0]

    def test_other_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_route.side_effect = _client_error("SomeOtherError")
        with pytest.raises(RuntimeError, match="Failed to create route"):
            mod._add_routes(
                ec2, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
            )


# ---------------------------------------------------------------------------
# _add_security_group_rules
# ---------------------------------------------------------------------------


class TestAddSecurityGroupRules:
    def test_success(self) -> None:
        ec2 = _mock_ec2()
        result = mod._add_security_group_rules(
            ec2, ["sg-1"], "10.0.0.0/16", [443, 80]
        )
        assert len(result) == 2
        assert "sg-1: tcp/443" in result[0]
        assert "sg-1: tcp/80" in result[1]

    def test_duplicate(self) -> None:
        ec2 = _mock_ec2()
        ec2.authorize_security_group_ingress.side_effect = _client_error(
            "InvalidPermission.Duplicate"
        )
        result = mod._add_security_group_rules(
            ec2, ["sg-1"], "10.0.0.0/16", [443]
        )
        assert "already exists" in result[0]

    def test_other_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.authorize_security_group_ingress.side_effect = _client_error(
            "SomeOtherError"
        )
        with pytest.raises(RuntimeError, match="Failed to add SG rule"):
            mod._add_security_group_rules(
                ec2, ["sg-1"], "10.0.0.0/16", [443]
            )


# ---------------------------------------------------------------------------
# _create_dns_record
# ---------------------------------------------------------------------------


class TestCreateDnsRecord:
    def test_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_r53 = MagicMock()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock_r53)
        result = mod._create_dns_record(
            "Z123", "svc.example.com", "pcx-1", None
        )
        assert result == ["svc.example.com -> pcx-1"]
        mock_r53.change_resource_record_sets.assert_called_once()

    def test_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_r53 = MagicMock()
        mock_r53.change_resource_record_sets.side_effect = _client_error(
            "NoSuchHostedZone"
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock_r53)
        with pytest.raises(RuntimeError, match="Failed to create DNS record"):
            mod._create_dns_record("Z123", "svc.example.com", "pcx-1", None)


# ---------------------------------------------------------------------------
# _wait_for_peering
# ---------------------------------------------------------------------------


class TestWaitForPeering:
    def test_active_immediately(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "active"}}
            ]
        }
        result = mod._wait_for_peering(ec2, "pcx-1")
        assert result == "active"

    def test_terminal_state(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "failed", "Message": "boom"}}
            ]
        }
        with pytest.raises(RuntimeError, match="terminal state"):
            mod._wait_for_peering(ec2, "pcx-1")

    def test_not_found(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": []
        }
        with pytest.raises(RuntimeError, match="not found"):
            mod._wait_for_peering(ec2, "pcx-1")

    def test_describe_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_vpc_peering_connections.side_effect = _client_error(
            "AccessDenied"
        )
        with pytest.raises(RuntimeError, match="Failed to describe peering"):
            mod._wait_for_peering(ec2, "pcx-1")

    def test_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "pending-acceptance"}}
            ]
        }
        # Make time.monotonic advance past deadline
        calls = iter([0.0, 0.0, 200.0])
        monkeypatch.setattr(mod.time, "monotonic", lambda: next(calls))
        monkeypatch.setattr(mod.time, "sleep", lambda _: None)
        with pytest.raises(TimeoutError, match="did not become active"):
            mod._wait_for_peering(ec2, "pcx-1", timeout=120.0)


# ---------------------------------------------------------------------------
# _setup_peering
# ---------------------------------------------------------------------------


class TestSetupPeering:
    def test_success_minimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-1"
            }
        }
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "active"}}
            ]
        }
        conn_id, status, routes, sg_rules = mod._setup_peering(
            ec2, "vpc-1", "vpc-2", [], [], "10.0.0.0/16", "10.1.0.0/16", [], []
        )
        assert conn_id == "pcx-1"
        assert status == "active"
        assert routes == []
        assert sg_rules == []

    def test_with_routes_and_sg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-2"
            }
        }
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "active"}}
            ]
        }
        conn_id, status, routes, sg_rules = mod._setup_peering(
            ec2,
            "vpc-1",
            "vpc-2",
            ["rt-1"],
            ["rt-2"],
            "10.0.0.0/16",
            "10.1.0.0/16",
            ["sg-1"],
            [443],
        )
        assert conn_id == "pcx-2"
        assert len(routes) == 2
        assert len(sg_rules) == 2

    def test_create_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.side_effect = _client_error(
            "VpcLimitExceeded"
        )
        with pytest.raises(RuntimeError, match="Failed to create VPC peering"):
            mod._setup_peering(
                ec2, "vpc-1", "vpc-2", [], [], "", "", [], []
            )

    def test_accept_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-1"
            }
        }
        ec2.accept_vpc_peering_connection.side_effect = _client_error(
            "Forbidden"
        )
        with pytest.raises(RuntimeError, match="Failed to accept VPC peering"):
            mod._setup_peering(
                ec2, "vpc-1", "vpc-2", [], [], "", "", [], []
            )


# ---------------------------------------------------------------------------
# _get_vpc_subnets
# ---------------------------------------------------------------------------


class TestGetVpcSubnets:
    def test_success(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
                {"SubnetId": "sub-2", "AvailabilityZone": "us-east-1b"},
                {"SubnetId": "sub-3", "AvailabilityZone": "us-east-1a"},
            ]
        }
        result = mod._get_vpc_subnets(ec2, "vpc-1")
        assert result == ["sub-1", "sub-2"]

    def test_error_returns_empty(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_subnets.side_effect = _client_error("AccessDenied")
        result = mod._get_vpc_subnets(ec2, "vpc-1")
        assert result == []


# ---------------------------------------------------------------------------
# _find_existing_tgw_attachment
# ---------------------------------------------------------------------------


class TestFindExistingTgwAttachment:
    def test_success(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"TransitGatewayAttachmentId": "att-1"}
            ]
        }
        result = mod._find_existing_tgw_attachment(ec2, "tgw-1", "vpc-1")
        assert result == "att-1"

    def test_not_found(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": []
        }
        with pytest.raises(RuntimeError, match="No existing TGW attachment"):
            mod._find_existing_tgw_attachment(ec2, "tgw-1", "vpc-1")

    def test_api_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.side_effect = _client_error(
            "AccessDenied"
        )
        with pytest.raises(RuntimeError, match="Failed to find existing"):
            mod._find_existing_tgw_attachment(ec2, "tgw-1", "vpc-1")


# ---------------------------------------------------------------------------
# _describe_tgw_attachment_status
# ---------------------------------------------------------------------------


class TestDescribeTgwAttachmentStatus:
    def test_success(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"State": "available"}
            ]
        }
        result = mod._describe_tgw_attachment_status(ec2, "att-1")
        assert result == "available"

    def test_empty(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": []
        }
        result = mod._describe_tgw_attachment_status(ec2, "att-1")
        assert result == "unknown"

    def test_error_returns_unknown(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateway_vpc_attachments.side_effect = _client_error(
            "AccessDenied"
        )
        result = mod._describe_tgw_attachment_status(ec2, "att-1")
        assert result == "unknown"


# ---------------------------------------------------------------------------
# _setup_transit_gateway
# ---------------------------------------------------------------------------


class TestSetupTransitGateway:
    def _ec2_for_tgw(self) -> MagicMock:
        """Return a mock EC2 client configured for TGW tests."""
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
            ]
        }
        ec2.create_transit_gateway_vpc_attachment.side_effect = [
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-req"
                }
            },
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-acc"
                }
            },
        ]
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"State": "available"}
            ]
        }
        return ec2

    def test_success_minimal(self) -> None:
        ec2 = self._ec2_for_tgw()
        conn_id, status, routes, sg_rules = mod._setup_transit_gateway(
            ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-req"
        assert status == "available"
        assert routes == []
        assert sg_rules == []

    def test_with_routes_and_sg(self) -> None:
        ec2 = self._ec2_for_tgw()
        conn_id, status, routes, sg_rules = mod._setup_transit_gateway(
            ec2,
            "tgw-1",
            "vpc-1",
            "vpc-2",
            ["rt-1"],
            ["rt-2"],
            "10.0.0.0/16",
            "10.1.0.0/16",
            ["sg-1"],
            [443],
            None,
        )
        assert len(routes) == 2
        assert len(sg_rules) == 2

    def test_describe_tgw_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.side_effect = _client_error(
            "AccessDenied"
        )
        with pytest.raises(RuntimeError, match="Failed to describe transit"):
            mod._setup_transit_gateway(
                ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
            )

    def test_tgw_not_found(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": []
        }
        with pytest.raises(RuntimeError, match="not found"):
            mod._setup_transit_gateway(
                ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
            )

    def test_requestor_attach_duplicate(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
            ]
        }
        ec2.create_transit_gateway_vpc_attachment.side_effect = [
            _client_error("DuplicateTransitGatewayAttachment"),
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-acc"
                }
            },
        ]
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"TransitGatewayAttachmentId": "att-existing", "State": "available"}
            ]
        }
        conn_id, status, routes, sg_rules = mod._setup_transit_gateway(
            ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-existing"

    def test_acceptor_attach_duplicate(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
            ]
        }
        ec2.create_transit_gateway_vpc_attachment.side_effect = [
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-req"
                }
            },
            _client_error("DuplicateTransitGatewayAttachment"),
        ]
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"TransitGatewayAttachmentId": "att-existing", "State": "available"}
            ]
        }
        conn_id, status, routes, sg_rules = mod._setup_transit_gateway(
            ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-req"

    def test_requestor_attach_other_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {"Subnets": []}
        ec2.create_transit_gateway_vpc_attachment.side_effect = _client_error(
            "InvalidParameterValue"
        )
        with pytest.raises(RuntimeError, match="Failed to attach"):
            mod._setup_transit_gateway(
                ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
            )

    def test_acceptor_attach_other_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {"Subnets": []}
        ec2.create_transit_gateway_vpc_attachment.side_effect = [
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-req"
                }
            },
            _client_error("InvalidParameterValue"),
        ]
        with pytest.raises(RuntimeError, match="Failed to attach"):
            mod._setup_transit_gateway(
                ec2, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
            )


# ---------------------------------------------------------------------------
# _setup_privatelink
# ---------------------------------------------------------------------------


class TestSetupPrivatelink:
    def _ec2_for_pl(self, svc_name: object = "com.svc") -> MagicMock:
        ec2 = _mock_ec2()
        ec2.create_vpc_endpoint_service_configuration.return_value = {
            "ServiceConfiguration": {
                "ServiceId": "svc-1",
                "ServiceName": svc_name,
            }
        }
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
            ]
        }
        ec2.create_vpc_endpoint.return_value = {
            "VpcEndpoint": {
                "VpcEndpointId": "vpce-1",
                "State": "available",
            }
        }
        return ec2

    def test_success_with_string_svc_name(self) -> None:
        ec2 = self._ec2_for_pl(svc_name="com.svc")
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"
        assert status == "available"
        assert routes == []
        assert sg_rules == []

    def test_with_sg_and_ports(self) -> None:
        ec2 = self._ec2_for_pl()
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", None, "vpc-1", "vpc-2",
            ["sg-1"], [443], "10.0.0.0/16", None
        )
        assert len(sg_rules) == 1

    def test_service_name_list(self) -> None:
        ec2 = self._ec2_for_pl(svc_name=["svc-a", "svc-b"])
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"

    def test_service_name_empty_list_with_fallback(self) -> None:
        ec2 = self._ec2_for_pl(svc_name=[])
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", "fallback-svc", "vpc-1", "vpc-2",
            [], [], "", None
        )
        assert ep_id == "vpce-1"

    def test_service_name_none_no_fallback(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_endpoint_service_configuration.return_value = {
            "ServiceConfiguration": {
                "ServiceId": "svc-1",
                "ServiceName": None,
            }
        }
        ec2.describe_subnets.return_value = {"Subnets": []}
        ec2.create_vpc_endpoint.return_value = {
            "VpcEndpoint": {
                "VpcEndpointId": "vpce-1",
                "State": "pending",
            }
        }
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"

    def test_no_subnets_no_sg(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_endpoint_service_configuration.return_value = {
            "ServiceConfiguration": {
                "ServiceId": "svc-1",
                "ServiceName": "com.svc",
            }
        }
        ec2.describe_subnets.return_value = {"Subnets": []}
        ec2.create_vpc_endpoint.return_value = {
            "VpcEndpoint": {
                "VpcEndpointId": "vpce-2",
                "State": "available",
            }
        }
        ep_id, status, routes, sg_rules = mod._setup_privatelink(
            ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-2"

    def test_create_svc_config_error(self) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_endpoint_service_configuration.side_effect = _client_error(
            "InvalidParameter"
        )
        with pytest.raises(RuntimeError, match="Failed to create VPC endpoint service"):
            mod._setup_privatelink(
                ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
            )

    def test_create_endpoint_error(self) -> None:
        ec2 = self._ec2_for_pl()
        ec2.create_vpc_endpoint.side_effect = _client_error("VpcEndpointFailed")
        with pytest.raises(RuntimeError, match="Failed to create VPC interface"):
            mod._setup_privatelink(
                ec2, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
            )


# ---------------------------------------------------------------------------
# vpc_connectivity_manager — main entry point
# ---------------------------------------------------------------------------


class TestVpcConnectivityManager:
    def test_invalid_type(self) -> None:
        with pytest.raises(ValueError, match="connectivity_type must be"):
            vpc_connectivity_manager(
                connectivity_type="invalid",
                requestor_vpc_id="vpc-1",
            )

    def test_peering_missing_acceptor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(ValueError, match="acceptor_vpc_id is required"):
            vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="",
            )

    def test_peering_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-1"
            }
        }
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "active"}}
            ]
        }
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        result = vpc_connectivity_manager(
            connectivity_type="peering",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
        )
        assert isinstance(result, VPCConnectivityResult)
        assert result.connectivity_type == "peering"
        assert result.connection_id == "pcx-1"

    def test_peering_with_dns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-1"
            }
        }
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "active"}}
            ]
        }
        mock_r53 = MagicMock()

        def mock_get_client(service: str, *a: object, **kw: object) -> MagicMock:
            if service == "route53":
                return mock_r53
            return ec2

        monkeypatch.setattr(mod, "get_client", mock_get_client)
        result = vpc_connectivity_manager(
            connectivity_type="peering",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
            hosted_zone_id="Z123",
            dns_record_name="svc.example.com",
        )
        assert len(result.dns_records_created) == 1

    def test_transit_gateway_missing_tgw_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(ValueError, match="transit_gateway_id is required"):
            vpc_connectivity_manager(
                connectivity_type="transit_gateway",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    def test_transit_gateway_missing_acceptor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(ValueError, match="acceptor_vpc_id is required"):
            vpc_connectivity_manager(
                connectivity_type="transit_gateway",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="",
                transit_gateway_id="tgw-1",
            )

    def test_transit_gateway_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        ec2.describe_transit_gateways.return_value = {
            "TransitGateways": [{"TransitGatewayId": "tgw-1"}]
        }
        ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
            ]
        }
        ec2.create_transit_gateway_vpc_attachment.side_effect = [
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-req"
                }
            },
            {
                "TransitGatewayVpcAttachment": {
                    "TransitGatewayAttachmentId": "att-acc"
                }
            },
        ]
        ec2.describe_transit_gateway_vpc_attachments.return_value = {
            "TransitGatewayVpcAttachments": [
                {"State": "available"}
            ]
        }
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        result = vpc_connectivity_manager(
            connectivity_type="transit_gateway",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
            transit_gateway_id="tgw-1",
        )
        assert result.connectivity_type == "transit_gateway"
        assert result.connection_id == "att-req"

    def test_privatelink_missing_nlb(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(ValueError, match="nlb_arn is required"):
            vpc_connectivity_manager(
                connectivity_type="privatelink",
                requestor_vpc_id="vpc-1",
            )

    def test_privatelink_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_endpoint_service_configuration.return_value = {
            "ServiceConfiguration": {
                "ServiceId": "svc-1",
                "ServiceName": "com.svc",
            }
        }
        ec2.describe_subnets.return_value = {"Subnets": []}
        ec2.create_vpc_endpoint.return_value = {
            "VpcEndpoint": {
                "VpcEndpointId": "vpce-1",
                "State": "available",
            }
        }
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        result = vpc_connectivity_manager(
            connectivity_type="privatelink",
            requestor_vpc_id="vpc-1",
            nlb_arn="arn:nlb",
        )
        assert result.connectivity_type == "privatelink"
        assert result.connection_id == "vpce-1"

    def test_generic_exception_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: (_ for _ in ()).throw(TypeError("boom")),
        )
        with pytest.raises(RuntimeError, match="vpc_connectivity_manager failed"):
            vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    def test_runtime_error_passthrough(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.side_effect = _client_error(
            "VpcLimitExceeded"
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(RuntimeError, match="Failed to create VPC peering"):
            vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    def test_timeout_error_passthrough(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _mock_ec2()
        ec2.create_vpc_peering_connection.return_value = {
            "VpcPeeringConnection": {
                "VpcPeeringConnectionId": "pcx-1"
            }
        }
        ec2.describe_vpc_peering_connections.return_value = {
            "VpcPeeringConnections": [
                {"Status": {"Code": "pending-acceptance"}}
            ]
        }
        calls = iter([0.0, 0.0, 200.0])
        monkeypatch.setattr(mod.time, "monotonic", lambda: next(calls))
        monkeypatch.setattr(mod.time, "sleep", lambda _: None)
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ec2)
        with pytest.raises(TimeoutError):
            vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )
