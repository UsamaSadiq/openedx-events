"""Classes and utility functions for the event bus."""

from abc import ABC, abstractmethod
from typing import NoReturn, Optional

from django.conf import settings
from django.utils.module_loading import import_string


class EventBus(ABC):
    """
    Parent class for event bus implementations.
    """

    @abstractmethod
    def send(
            *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
            sync: bool = False,
    ) -> None:
        """
        Send a signal event to the event bus under the specified topic.

        Arguments:
            signal: The original OpenEdxPublicSignal the event was sent to
            topic: The event bus topic for the event
            event_key_field: Path to the event data field to use as the event key (period-delimited
              string naming the dictionary keys to descend)
            event_data: The event data (kwargs) sent to the signal
            sync: Whether to wait indefinitely for event to be received by the message bus (probably
              only want to use this for testing)
        """
        pass

    @abstractmethod
    def consume_indefinitely(*, topic: str, group_id: str) -> NoReturn:
        """
        Consume events from a topic in an infinite loop.

        Events will be converted into calls to Django signals.

        Arguments:
            topic: The event bus topic to consume from.
            group_id: The consumer group to participate in.
        """
        pass


class NoEventBus(EventBus):
    """
    Stub implementation to "load" when no implementation is properly configured.
    """

    def send(
            *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
            sync: bool = False,
    ) -> None:
        pass

    def consume_indefinitely(*, topic: str, group_id: str) -> NoReturn:
        pass


def _load_event_bus() -> EventBus:
    """
    Load the currently configured event bus implementation.

    If configuration is invalid, will return a NoEventBus instance that does nothing.
    """
    try:
        eb = settings.EVENT_BUS
        loader = import_string(eb['loader'])
        impl = loader(eb)
        if isinstance(impl, EventBus):
            return impl
        else:
            warnings.warn(f"Loader {loader} a {type(impl)}, not an EventBus", UserWarning)
            return NoEventBus()
    except BaseException as e:
        warnings.warn(f"Failed to load event bus implementation: {e!r}", UserWarning)
        return NoEventBus()

# The configured event bus implementation, or a stub instance that does nothing.
EVENT_BUS = _load_event_bus()
