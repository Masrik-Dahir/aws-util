"""Tests for aws_util.__init__ public API surface."""
from __future__ import annotations

import aws_util


def test_retrieve_importable():
    assert callable(aws_util.retrieve)


def test_clear_ssm_cache_importable():
    assert callable(aws_util.clear_ssm_cache)


def test_clear_secret_cache_importable():
    assert callable(aws_util.clear_secret_cache)


def test_clear_all_caches_importable():
    assert callable(aws_util.clear_all_caches)


def test_get_parameter_importable():
    assert callable(aws_util.get_parameter)


def test_get_secret_importable():
    assert callable(aws_util.get_secret)


def test_load_app_config_importable():
    assert callable(aws_util.load_app_config)


def test_get_db_credentials_importable():
    assert callable(aws_util.get_db_credentials)


def test_send_alert_importable():
    assert callable(aws_util.send_alert)


def test_notify_on_exception_importable():
    assert callable(aws_util.notify_on_exception)


def test_all_exports_listed():
    for name in aws_util.__all__:
        assert hasattr(aws_util, name), f"{name} in __all__ but not importable"
