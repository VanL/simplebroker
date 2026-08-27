"""Static negative fixture: live generators do not accept reverse order."""

from simplebroker import Queue


def invalid_generator_orders(queue: Queue) -> None:
    queue.read_generator(order="newest")
    queue.peek_generator(order="newest")
    queue.move_generator("destination", order="newest")
