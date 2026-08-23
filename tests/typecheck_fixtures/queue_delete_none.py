"""Deliberately failing fixture: explicit None is not a valid delete target."""

from simplebroker import Queue


def delete_optional_id(queue: Queue) -> bool:
    return queue.delete(message_id=None)
