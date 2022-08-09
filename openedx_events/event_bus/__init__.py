"""Classes and utility functions for the event bus."""

import warnings
from abc import ABC, abstractmethod
from typing import NoReturn, Optional

from django.conf import settings
from django.utils.module_loading import import_string

from openedx_events.tooling import OpenEdxPublicSignal


def try_load(setting_name: str, expected_class: type, default):
    """
    Load an instance of ``expected_class`` as indicated by ``setting_name``.

    The setting points to a loader or constructor that will fetch or create an
    instance of the expected class. If the configuration is missing or invalid,
    or the loader throws an exception or returns the wrong type, the default is
    returned instead.

    Arguments:
        setting_name: Name of a Django setting containing a dotted module path, indicating a loader
        expected_class: The loader must produce an instance of this class object (or a subclass)
        default: Object to return if any part of the lookup or loading fails
    """
    constructor_path = getattr(settings, setting_name, None)
    if constructor_path is None:
        # No warning is called for if event-bus is not configured-for at *all*
        return default

    try:
        constructor = import_string(constructor_path)
        instance = constructor()
        if isinstance(instance, expected_class):
            return instance
        else:
            warnings.warn(f"{constructor_path} returned unexpected type {type(instance)}", UserWarning)
            return default
    except BaseException as e:
        warnings.warn(f"Failed to load {expected_class} from setting '{setting_name}': {e!r}", UserWarning)
        return default


# == Producer ==


class EventBusProducer(ABC):
    """
    Parent class for event bus producer implementations.
    """

    def send(
            self, *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
    ) -> None:
        """
        Send a signal event to the event bus under the specified topic.

        Arguments:
            signal: The original OpenEdxPublicSignal the event was sent to
            topic: The event bus topic for the event (without environmental prefix)
            event_key_field: Path to the event data field to use as the event key (period-delimited
              string naming the dictionary keys to descend)
            event_data: The event data (kwargs) sent to the signal
        """


class NoEventBusProducer(EventBusProducer):
    """
    Stub implementation to "load" when no implementation is properly configured.
    """

    def send(
            self, *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
    ) -> None:
        """Do nothing."""


def make_producer() -> EventBusProducer:
    """
    Create the producer implementation, as configured.

    If misconfigured, returns a fake implementation that can be called but does nothing.
    """
    return try_load('EVENT_BUS_PRODUCER', EventBusProducer, NoEventBusProducer())


PRODUCER = make_producer()


# == Consumer ==


class EventBusConsumer(ABC):
    """
    Parent class for event bus consumer implementations.
    """

    @abstractmethod
    def consume_indefinitely(self, *, topic: str, group_id: str):
        """
        Consume events from a topic in an infinite loop.

        Events will be converted into calls to Django signals.

        Arguments:
            topic: The event bus topic to consume from (without environmental prefix)
            group_id: The consumer group to participate in.
        """


class NoEventBusConsumer(EventBusConsumer):
    """
    Stub implementation to "load" when no implementation is properly configured.
    """

    def consume_indefinitely(self, *, topic: str, group_id: str):
        """Do nothing."""


def make_consumer() -> EventBusConsumer:
    """
    Create the consumer implementation, as configured.

    If misconfigured, returns a fake implementation that can be called but does nothing.
    """
    return try_load('EVENT_BUS_CONSUMER', EventBusConsumer, NoEventBusConsumer())


CONSUMER = make_consumer()
