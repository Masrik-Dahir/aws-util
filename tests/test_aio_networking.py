"""Tests for aws_util.aio.networking -- 100% line coverage."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio import networking as mod
from aws_util.networking import VPCConnectivityResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(**overrides: object) -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# ---------------------------------------------------------------------------
# _add_routes
# ---------------------------------------------------------------------------


class TestAddRoutes:
    async def test_success(self) -> None:
        client = _make_client(return_value={})
        result = await mod._add_routes(
            client, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
        )
        assert len(result) == 1
        assert "rt-1" in result[0]
        assert "pcx-1" in result[0]

    async def test_already_exists(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("RouteAlreadyExists")
        )
        result = await mod._add_routes(
            client, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
        )
        assert "already exists" in result[0]

    async def test_other_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("SomeOtherError")
        )
        with pytest.raises(RuntimeError, match="Failed to create route"):
            await mod._add_routes(
                client, ["rt-1"], "10.0.0.0/16", "VpcPeeringConnectionId", "pcx-1"
            )


# ---------------------------------------------------------------------------
# _add_security_group_rules
# ---------------------------------------------------------------------------


class TestAddSecurityGroupRules:
    async def test_success(self) -> None:
        client = _make_client(return_value={})
        result = await mod._add_security_group_rules(
            client, ["sg-1"], "10.0.0.0/16", [443, 80]
        )
        assert len(result) == 2
        assert "sg-1: tcp/443" in result[0]
        assert "sg-1: tcp/80" in result[1]

    async def test_duplicate(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("InvalidPermission.Duplicate")
        )
        result = await mod._add_security_group_rules(
            client, ["sg-1"], "10.0.0.0/16", [443]
        )
        assert "already exists" in result[0]

    async def test_other_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("SomeOtherError")
        )
        with pytest.raises(RuntimeError, match="Failed to add SG rule"):
            await mod._add_security_group_rules(
                client, ["sg-1"], "10.0.0.0/16", [443]
            )


# ---------------------------------------------------------------------------
# _create_dns_record
# ---------------------------------------------------------------------------


class TestCreateDnsRecord:
    async def test_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_r53 = _make_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_r53
        )
        result = await mod._create_dns_record(
            "Z123", "svc.example.com", "pcx-1", None
        )
        assert result == ["svc.example.com -> pcx-1"]

    async def test_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_r53 = _make_client(
            side_effect=RuntimeError("NoSuchHostedZone")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_r53
        )
        with pytest.raises(RuntimeError, match="Failed to create DNS record"):
            await mod._create_dns_record(
                "Z123", "svc.example.com", "pcx-1", None
            )


# ---------------------------------------------------------------------------
# _wait_for_peering
# ---------------------------------------------------------------------------


class TestWaitForPeering:
    async def test_active_immediately(self) -> None:
        client = _make_client(
            return_value={
                "VpcPeeringConnections": [
                    {"Status": {"Code": "active"}}
                ]
            }
        )
        result = await mod._wait_for_peering(client, "pcx-1")
        assert result == "active"

    async def test_terminal_state(self) -> None:
        client = _make_client(
            return_value={
                "VpcPeeringConnections": [
                    {"Status": {"Code": "failed", "Message": "boom"}}
                ]
            }
        )
        with pytest.raises(RuntimeError, match="terminal state"):
            await mod._wait_for_peering(client, "pcx-1")

    async def test_not_found(self) -> None:
        client = _make_client(
            return_value={"VpcPeeringConnections": []}
        )
        with pytest.raises(RuntimeError, match="not found"):
            await mod._wait_for_peering(client, "pcx-1")

    async def test_describe_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("AccessDenied")
        )
        with pytest.raises(RuntimeError, match="Failed to describe peering"):
            await mod._wait_for_peering(client, "pcx-1")

    async def test_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = _make_client(
            return_value={
                "VpcPeeringConnections": [
                    {"Status": {"Code": "pending-acceptance"}}
                ]
            }
        )
        cnt = 0

        def fake_mono():
            nonlocal cnt
            cnt += 1
            return 0.0 if cnt <= 2 else 200.0

        monkeypatch.setattr(mod.time, "monotonic", fake_mono)
        monkeypatch.setattr(
            mod.asyncio, "sleep", AsyncMock()
        )
        with pytest.raises(TimeoutError, match="did not become active"):
            await mod._wait_for_peering(client, "pcx-1", timeout=120.0)


# ---------------------------------------------------------------------------
# _setup_peering
# ---------------------------------------------------------------------------


class TestSetupPeering:
    async def test_success_minimal(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # create peering
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-1"
                    }
                },
                # accept peering
                {},
                # describe peering (wait)
                {
                    "VpcPeeringConnections": [
                        {"Status": {"Code": "active"}}
                    ]
                },
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_peering(
            client, "vpc-1", "vpc-2", [], [], "10.0.0.0/16", "10.1.0.0/16", [], []
        )
        assert conn_id == "pcx-1"
        assert status == "active"
        assert routes == []
        assert sg_rules == []

    async def test_with_routes_and_sg(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # create peering
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-2"
                    }
                },
                # accept peering
                {},
                # describe peering (wait)
                {
                    "VpcPeeringConnections": [
                        {"Status": {"Code": "active"}}
                    ]
                },
                # _add_routes for requestor (CreateRoute)
                {},
                # _add_routes for acceptor (CreateRoute)
                {},
                # _add_sg_rules acceptor cidr sg-1:443
                {},
                # _add_sg_rules requestor cidr sg-1:443
                {},
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_peering(
            client,
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

    async def test_create_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("VpcLimitExceeded")
        )
        with pytest.raises(RuntimeError, match="Failed to create VPC peering"):
            await mod._setup_peering(
                client, "vpc-1", "vpc-2", [], [], "", "", [], []
            )

    async def test_accept_error(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-1"
                    }
                },
                RuntimeError("Forbidden"),
            ]
        )
        with pytest.raises(RuntimeError, match="Failed to accept VPC peering"):
            await mod._setup_peering(
                client, "vpc-1", "vpc-2", [], [], "", "", [], []
            )


# ---------------------------------------------------------------------------
# _get_vpc_subnets
# ---------------------------------------------------------------------------


class TestGetVpcSubnets:
    async def test_success(self) -> None:
        client = _make_client(
            return_value={
                "Subnets": [
                    {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
                    {"SubnetId": "sub-2", "AvailabilityZone": "us-east-1b"},
                    {"SubnetId": "sub-3", "AvailabilityZone": "us-east-1a"},
                ]
            }
        )
        result = await mod._get_vpc_subnets(client, "vpc-1")
        assert result == ["sub-1", "sub-2"]

    async def test_error_returns_empty(self) -> None:
        client = _make_client(side_effect=RuntimeError("AccessDenied"))
        result = await mod._get_vpc_subnets(client, "vpc-1")
        assert result == []


# ---------------------------------------------------------------------------
# _find_existing_tgw_attachment
# ---------------------------------------------------------------------------


class TestFindExistingTgwAttachment:
    async def test_success(self) -> None:
        client = _make_client(
            return_value={
                "TransitGatewayVpcAttachments": [
                    {"TransitGatewayAttachmentId": "att-1"}
                ]
            }
        )
        result = await mod._find_existing_tgw_attachment(
            client, "tgw-1", "vpc-1"
        )
        assert result == "att-1"

    async def test_not_found(self) -> None:
        client = _make_client(
            return_value={"TransitGatewayVpcAttachments": []}
        )
        with pytest.raises(RuntimeError, match="No existing TGW attachment"):
            await mod._find_existing_tgw_attachment(
                client, "tgw-1", "vpc-1"
            )

    async def test_api_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("AccessDenied")
        )
        with pytest.raises(RuntimeError, match="Failed to find existing"):
            await mod._find_existing_tgw_attachment(
                client, "tgw-1", "vpc-1"
            )


# ---------------------------------------------------------------------------
# _describe_tgw_attachment_status
# ---------------------------------------------------------------------------


class TestDescribeTgwAttachmentStatus:
    async def test_success(self) -> None:
        client = _make_client(
            return_value={
                "TransitGatewayVpcAttachments": [
                    {"State": "available"}
                ]
            }
        )
        result = await mod._describe_tgw_attachment_status(
            client, "att-1"
        )
        assert result == "available"

    async def test_empty(self) -> None:
        client = _make_client(
            return_value={"TransitGatewayVpcAttachments": []}
        )
        result = await mod._describe_tgw_attachment_status(
            client, "att-1"
        )
        assert result == "unknown"

    async def test_error_returns_unknown(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("AccessDenied")
        )
        result = await mod._describe_tgw_attachment_status(
            client, "att-1"
        )
        assert result == "unknown"


# ---------------------------------------------------------------------------
# _setup_transit_gateway
# ---------------------------------------------------------------------------


class TestSetupTransitGateway:
    async def test_success_minimal(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets for requestor
                {
                    "Subnets": [
                        {"SubnetId": "sub-1", "AvailabilityZone": "us-east-1a"},
                    ]
                },
                # create TGW attachment requestor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-req"
                    }
                },
                # describe subnets for acceptor
                {
                    "Subnets": [
                        {"SubnetId": "sub-2", "AvailabilityZone": "us-east-1a"},
                    ]
                },
                # create TGW attachment acceptor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-acc"
                    }
                },
                # describe attachment status
                {
                    "TransitGatewayVpcAttachments": [
                        {"State": "available"}
                    ]
                },
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_transit_gateway(
            client, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-req"
        assert status == "available"

    async def test_with_routes_and_sg(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets requestor
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                # create TGW attach requestor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-req"
                    }
                },
                # describe subnets acceptor
                {"Subnets": [{"SubnetId": "sub-2", "AvailabilityZone": "a"}]},
                # create TGW attach acceptor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-acc"
                    }
                },
                # add route requestor
                {},
                # add route acceptor
                {},
                # add sg rules (acceptor cidr)
                {},
                # add sg rules (requestor cidr)
                {},
                # describe attachment status
                {"TransitGatewayVpcAttachments": [{"State": "available"}]},
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_transit_gateway(
            client,
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

    async def test_describe_tgw_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("AccessDenied")
        )
        with pytest.raises(RuntimeError, match="Failed to describe transit"):
            await mod._setup_transit_gateway(
                client, "tgw-1", "vpc-1", "vpc-2",
                [], [], "", "", [], [], None
            )

    async def test_tgw_not_found(self) -> None:
        client = _make_client(
            return_value={"TransitGateways": []}
        )
        with pytest.raises(RuntimeError, match="not found"):
            await mod._setup_transit_gateway(
                client, "tgw-1", "vpc-1", "vpc-2",
                [], [], "", "", [], [], None
            )

    async def test_requestor_attach_duplicate(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets requestor
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                # create TGW attach requestor -- duplicate
                RuntimeError("DuplicateTransitGatewayAttachment"),
                # find existing attachment
                {
                    "TransitGatewayVpcAttachments": [
                        {"TransitGatewayAttachmentId": "att-existing"}
                    ]
                },
                # describe subnets acceptor
                {"Subnets": [{"SubnetId": "sub-2", "AvailabilityZone": "a"}]},
                # create TGW attach acceptor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-acc"
                    }
                },
                # describe attachment status
                {"TransitGatewayVpcAttachments": [{"State": "available"}]},
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_transit_gateway(
            client, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-existing"

    async def test_acceptor_attach_duplicate(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets requestor
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                # create TGW attach requestor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-req"
                    }
                },
                # describe subnets acceptor
                {"Subnets": [{"SubnetId": "sub-2", "AvailabilityZone": "a"}]},
                # create TGW attach acceptor -- duplicate
                RuntimeError("DuplicateTransitGatewayAttachment"),
                # find existing attachment
                {
                    "TransitGatewayVpcAttachments": [
                        {"TransitGatewayAttachmentId": "att-existing"}
                    ]
                },
                # describe attachment status
                {"TransitGatewayVpcAttachments": [{"State": "available"}]},
            ]
        )
        conn_id, status, routes, sg_rules = await mod._setup_transit_gateway(
            client, "tgw-1", "vpc-1", "vpc-2", [], [], "", "", [], [], None
        )
        assert conn_id == "att-req"

    async def test_requestor_attach_other_error(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets
                {"Subnets": []},
                # create attach -- other error
                RuntimeError("InvalidParameterValue"),
            ]
        )
        with pytest.raises(RuntimeError, match="Failed to attach"):
            await mod._setup_transit_gateway(
                client, "tgw-1", "vpc-1", "vpc-2",
                [], [], "", "", [], [], None
            )

    async def test_acceptor_attach_other_error(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets requestor
                {"Subnets": []},
                # create attach requestor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-req"
                    }
                },
                # describe subnets acceptor
                {"Subnets": []},
                # create attach acceptor -- other error
                RuntimeError("InvalidParameterValue"),
            ]
        )
        with pytest.raises(RuntimeError, match="Failed to attach"):
            await mod._setup_transit_gateway(
                client, "tgw-1", "vpc-1", "vpc-2",
                [], [], "", "", [], [], None
            )


# ---------------------------------------------------------------------------
# _setup_privatelink
# ---------------------------------------------------------------------------


class TestSetupPrivatelink:
    async def test_success_string_svc_name(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # create svc config
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": "com.svc",
                    }
                },
                # describe subnets
                {
                    "Subnets": [
                        {"SubnetId": "sub-1", "AvailabilityZone": "a"},
                    ]
                },
                # create endpoint
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "available",
                    }
                },
            ]
        )
        ep_id, status, routes, sg_rules = await mod._setup_privatelink(
            client, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"
        assert status == "available"
        assert routes == []
        assert sg_rules == []

    async def test_with_sg_and_ports(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                # create svc config
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": "com.svc",
                    }
                },
                # describe subnets
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                # create endpoint
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "available",
                    }
                },
                # add sg rule
                {},
            ]
        )
        ep_id, status, routes, sg_rules = await mod._setup_privatelink(
            client, "arn:nlb", None, "vpc-1", "vpc-2",
            ["sg-1"], [443], "10.0.0.0/16", None
        )
        assert len(sg_rules) == 1

    async def test_service_name_list(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": ["svc-a", "svc-b"],
                    }
                },
                {"Subnets": []},
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "available",
                    }
                },
            ]
        )
        ep_id, status, routes, sg_rules = await mod._setup_privatelink(
            client, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"

    async def test_service_name_empty_list_fallback(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": [],
                    }
                },
                {"Subnets": []},
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "available",
                    }
                },
            ]
        )
        ep_id, status, routes, sg_rules = await mod._setup_privatelink(
            client, "arn:nlb", "fallback-svc", "vpc-1", "vpc-2",
            [], [], "", None
        )
        assert ep_id == "vpce-1"

    async def test_service_name_none_no_fallback(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": None,
                    }
                },
                {"Subnets": []},
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "pending",
                    }
                },
            ]
        )
        ep_id, status, routes, sg_rules = await mod._setup_privatelink(
            client, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
        )
        assert ep_id == "vpce-1"

    async def test_create_svc_config_error(self) -> None:
        client = _make_client(
            side_effect=RuntimeError("InvalidParameter")
        )
        with pytest.raises(RuntimeError, match="Failed to create VPC endpoint service"):
            await mod._setup_privatelink(
                client, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
            )

    async def test_create_endpoint_error(self) -> None:
        client = AsyncMock()
        client.call = AsyncMock(
            side_effect=[
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": "com.svc",
                    }
                },
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                RuntimeError("VpcEndpointFailed"),
            ]
        )
        with pytest.raises(RuntimeError, match="Failed to create VPC interface"):
            await mod._setup_privatelink(
                client, "arn:nlb", None, "vpc-1", "vpc-2", [], [], "", None
            )


# ---------------------------------------------------------------------------
# vpc_connectivity_manager
# ---------------------------------------------------------------------------


class TestVpcConnectivityManager:
    async def test_invalid_type(self) -> None:
        with pytest.raises(ValueError, match="connectivity_type must be"):
            await mod.vpc_connectivity_manager(
                connectivity_type="invalid",
                requestor_vpc_id="vpc-1",
            )

    async def test_peering_missing_acceptor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = _make_client()
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(ValueError, match="acceptor_vpc_id is required"):
            await mod.vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="",
            )

    async def test_peering_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = AsyncMock()
        mock_ec2.call = AsyncMock(
            side_effect=[
                # create peering
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-1"
                    }
                },
                # accept peering
                {},
                # describe peering (wait)
                {
                    "VpcPeeringConnections": [
                        {"Status": {"Code": "active"}}
                    ]
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        result = await mod.vpc_connectivity_manager(
            connectivity_type="peering",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
        )
        assert isinstance(result, VPCConnectivityResult)
        assert result.connectivity_type == "peering"
        assert result.connection_id == "pcx-1"

    async def test_peering_with_dns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = AsyncMock()
        mock_ec2.call = AsyncMock(
            side_effect=[
                # create peering
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-1"
                    }
                },
                # accept peering
                {},
                # describe peering (wait)
                {
                    "VpcPeeringConnections": [
                        {"Status": {"Code": "active"}}
                    ]
                },
            ]
        )
        mock_r53 = _make_client(return_value={})

        def mock_async_client(
            service: str, *a: object, **kw: object
        ) -> AsyncMock:
            if service == "route53":
                return mock_r53
            return mock_ec2

        monkeypatch.setattr(mod, "async_client", mock_async_client)
        result = await mod.vpc_connectivity_manager(
            connectivity_type="peering",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
            hosted_zone_id="Z123",
            dns_record_name="svc.example.com",
        )
        assert len(result.dns_records_created) == 1

    async def test_transit_gateway_missing_tgw_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = _make_client()
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(ValueError, match="transit_gateway_id is required"):
            await mod.vpc_connectivity_manager(
                connectivity_type="transit_gateway",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    async def test_transit_gateway_missing_acceptor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = _make_client()
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(ValueError, match="acceptor_vpc_id is required"):
            await mod.vpc_connectivity_manager(
                connectivity_type="transit_gateway",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="",
                transit_gateway_id="tgw-1",
            )

    async def test_transit_gateway_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = AsyncMock()
        mock_ec2.call = AsyncMock(
            side_effect=[
                # describe TGW
                {"TransitGateways": [{"TransitGatewayId": "tgw-1"}]},
                # describe subnets requestor
                {"Subnets": [{"SubnetId": "sub-1", "AvailabilityZone": "a"}]},
                # create TGW attach requestor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-req"
                    }
                },
                # describe subnets acceptor
                {"Subnets": [{"SubnetId": "sub-2", "AvailabilityZone": "a"}]},
                # create TGW attach acceptor
                {
                    "TransitGatewayVpcAttachment": {
                        "TransitGatewayAttachmentId": "att-acc"
                    }
                },
                # describe attachment status
                {"TransitGatewayVpcAttachments": [{"State": "available"}]},
            ]
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        result = await mod.vpc_connectivity_manager(
            connectivity_type="transit_gateway",
            requestor_vpc_id="vpc-1",
            acceptor_vpc_id="vpc-2",
            transit_gateway_id="tgw-1",
        )
        assert result.connectivity_type == "transit_gateway"
        assert result.connection_id == "att-req"

    async def test_privatelink_missing_nlb(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = _make_client()
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(ValueError, match="nlb_arn is required"):
            await mod.vpc_connectivity_manager(
                connectivity_type="privatelink",
                requestor_vpc_id="vpc-1",
            )

    async def test_privatelink_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = AsyncMock()
        mock_ec2.call = AsyncMock(
            side_effect=[
                # create svc config
                {
                    "ServiceConfiguration": {
                        "ServiceId": "svc-1",
                        "ServiceName": "com.svc",
                    }
                },
                # describe subnets
                {"Subnets": []},
                # create endpoint
                {
                    "VpcEndpoint": {
                        "VpcEndpointId": "vpce-1",
                        "State": "available",
                    }
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        result = await mod.vpc_connectivity_manager(
            connectivity_type="privatelink",
            requestor_vpc_id="vpc-1",
            nlb_arn="arn:nlb",
        )
        assert result.connectivity_type == "privatelink"
        assert result.connection_id == "vpce-1"

    async def test_generic_exception_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def bad_client(*a: object, **kw: object) -> None:
            raise TypeError("boom")

        monkeypatch.setattr(mod, "async_client", bad_client)
        with pytest.raises(RuntimeError, match="vpc_connectivity_manager failed"):
            await mod.vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    async def test_runtime_error_passthrough(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = _make_client(
            side_effect=RuntimeError("VpcLimitExceeded")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(
            RuntimeError, match="Failed to create VPC peering"
        ):
            await mod.vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )

    async def test_timeout_error_passthrough(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ec2 = AsyncMock()
        mock_ec2.call = AsyncMock(
            side_effect=[
                # create peering
                {
                    "VpcPeeringConnection": {
                        "VpcPeeringConnectionId": "pcx-1"
                    }
                },
                # accept peering
                {},
                # describe peering (wait) -- pending
                {
                    "VpcPeeringConnections": [
                        {"Status": {"Code": "pending-acceptance"}}
                    ]
                },
            ]
        )
        cnt2 = 0

        def fake_mono2():
            nonlocal cnt2
            cnt2 += 1
            return 0.0 if cnt2 <= 1 else 200.0

        monkeypatch.setattr(mod.time, "monotonic", fake_mono2)
        monkeypatch.setattr(mod.asyncio, "sleep", AsyncMock())
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ec2
        )
        with pytest.raises(TimeoutError):
            await mod.vpc_connectivity_manager(
                connectivity_type="peering",
                requestor_vpc_id="vpc-1",
                acceptor_vpc_id="vpc-2",
            )
