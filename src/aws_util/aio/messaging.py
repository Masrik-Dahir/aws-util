"""Async wrappers for :mod:`aws_util.messaging`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.messaging import (
    ChannelConfig,
    ChannelResult,
    DigestEvent,
    DigestFlushResult,
    EventDeduplicationResult,
    FifoMessageResult,
    FilterPolicyResult,
    MultiChannelNotifierResult,
    batch_notification_digester as _sync_batch_notification_digester,
    event_deduplicator as _sync_event_deduplicator,
    multi_channel_notifier as _sync_multi_channel_notifier,
    sns_filter_policy_manager as _sync_sns_filter_policy_manager,
    sqs_fifo_sequencer as _sync_sqs_fifo_sequencer,
)

__all__ = [
    "ChannelConfig",
    "ChannelResult",
    "MultiChannelNotifierResult",
    "EventDeduplicationResult",
    "FilterPolicyResult",
    "FifoMessageResult",
    "DigestEvent",
    "DigestFlushResult",
    "multi_channel_notifier",
    "event_deduplicator",
    "sns_filter_policy_manager",
    "sqs_fifo_sequencer",
    "batch_notification_digester",
]

multi_channel_notifier = async_wrap(_sync_multi_channel_notifier)
event_deduplicator = async_wrap(_sync_event_deduplicator)
sns_filter_policy_manager = async_wrap(_sync_sns_filter_policy_manager)
sqs_fifo_sequencer = async_wrap(_sync_sqs_fifo_sequencer)
batch_notification_digester = async_wrap(_sync_batch_notification_digester)
