#!/usr/bin/env python3
"""
Performance benchmark for SimpleBroker.

This script measures the performance of various SimpleBroker operations
to verify the "1000+ messages/second" throughput claim.

Usage:
    python benchmark.py [--iterations N] [--message-sizes S1,S2,S3]

Example:
    python benchmark.py --iterations 10000 --message-sizes 100,1024,10240
"""

import argparse
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simplebroker import Queue


@contextmanager
def timer(name: str):  # type: ignore[no-untyped-def]
    """Context manager for timing operations."""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"{name}: {elapsed:.3f} seconds")


def benchmark_write(queue_name: str, iterations: int, message_size: int) -> float:
    """Benchmark write operations."""
    message = "x" * message_size

    with Queue(queue_name) as q:
        start = time.perf_counter()
        for _i in range(iterations):
            q.write(message)
        elapsed = time.perf_counter() - start

    messages_per_second = iterations / elapsed
    print(f"Write benchmark ({message_size} bytes):")
    print(f"  - {iterations} messages in {elapsed:.3f} seconds")
    print(f"  - {messages_per_second:.0f} messages/second")
    print(f"  - {elapsed / iterations * 1000:.3f} ms per message")
    print()

    return messages_per_second


def benchmark_read(queue_name: str, iterations: int) -> float:
    """Benchmark read operations."""
    start = time.perf_counter()
    messages_read = 0

    with Queue(queue_name) as q:
        for _i in range(iterations):
            msg = q.read()
            if msg:
                messages_read += 1
            else:
                break

    elapsed = time.perf_counter() - start
    messages_per_second = messages_read / elapsed if elapsed > 0 else 0

    print("Read benchmark:")
    print(f"  - {messages_read} messages in {elapsed:.3f} seconds")
    print(f"  - {messages_per_second:.0f} messages/second")
    print(
        f"  - {elapsed / messages_read * 1000:.3f} ms per message"
        if messages_read > 0
        else "  - No messages read"
    )
    print()

    return messages_per_second


def benchmark_peek(queue_name: str, iterations: int, message_size: int) -> float:
    """Benchmark peek operations."""
    # First, write some messages
    message = "x" * message_size
    with Queue(queue_name) as q:
        for _i in range(min(100, iterations)):  # Write up to 100 messages
            q.write(message)

    # Now benchmark peek
    start = time.perf_counter()
    messages_peeked = 0

    with Queue(queue_name) as q:
        for _i in range(iterations):
            msg = q.peek()
            if msg:
                messages_peeked += 1
            else:
                break

    elapsed = time.perf_counter() - start
    messages_per_second = messages_peeked / elapsed if elapsed > 0 else 0

    print(f"Peek benchmark ({message_size} bytes):")
    print(f"  - {messages_peeked} peeks in {elapsed:.3f} seconds")
    print(f"  - {messages_per_second:.0f} peeks/second")
    print(
        f"  - {elapsed / messages_peeked * 1000:.3f} ms per peek"
        if messages_peeked > 0
        else "  - No messages peeked"
    )
    print()

    # Clean up
    with Queue(queue_name) as q:
        q.delete()

    return messages_per_second


def benchmark_move(
    source_queue: str, dest_queue: str, iterations: int, message_size: int
) -> float:
    """Benchmark move operations."""
    # First, write messages to source queue
    message = "x" * message_size
    with Queue(source_queue) as q:
        for _i in range(iterations):
            q.write(message)

    # Now benchmark move
    start = time.perf_counter()

    with Queue(source_queue) as src:
        moved = src.move(dest_queue, all_messages=True)

    elapsed = time.perf_counter() - start
    messages_per_second = moved / elapsed if elapsed > 0 else 0

    print(f"Move benchmark ({message_size} bytes):")
    print(f"  - {moved} messages moved in {elapsed:.3f} seconds")
    print(f"  - {messages_per_second:.0f} messages/second")
    print(
        f"  - {elapsed / moved * 1000:.3f} ms per message"
        if moved > 0
        else "  - No messages moved"
    )
    print()

    # Clean up
    with Queue(dest_queue) as q:
        q.delete()

    return messages_per_second


def benchmark_roundtrip(queue_name: str, iterations: int, message_size: int) -> float:
    """Benchmark write+read roundtrip."""
    message = "x" * message_size

    start = time.perf_counter()

    with Queue(queue_name) as q:
        for _i in range(iterations):
            q.write(message)
            msg = q.read()
            if not msg:
                print("ERROR: Failed to read message")
                break

    elapsed = time.perf_counter() - start
    roundtrips_per_second = iterations / elapsed if elapsed > 0 else 0

    print(f"Write+Read roundtrip benchmark ({message_size} bytes):")
    print(f"  - {iterations} roundtrips in {elapsed:.3f} seconds")
    print(f"  - {roundtrips_per_second:.0f} roundtrips/second")
    print(f"  - {elapsed / iterations * 1000:.3f} ms per roundtrip")
    print()

    return roundtrips_per_second


def run_benchmarks(iterations: int, message_sizes: List[int]) -> None:
    """Run all benchmarks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to temp directory
        os.chdir(tmpdir)

        print("=" * 60)
        print("SimpleBroker Performance Benchmark")
        print(f"Iterations: {iterations}")
        print(f"Message sizes: {message_sizes} bytes")
        print("=" * 60)
        print()

        results = {}

        for size in message_sizes:
            print(f"\n### Message size: {size} bytes ###\n")

            # Write benchmark
            write_speed = benchmark_write("bench_write", iterations, size)
            results[f"write_{size}"] = write_speed

            # Read benchmark
            read_speed = benchmark_read("bench_write", iterations)
            results[f"read_{size}"] = read_speed

            # Peek benchmark
            peek_speed = benchmark_peek("bench_peek", iterations, size)
            results[f"peek_{size}"] = peek_speed

            # Move benchmark
            move_speed = benchmark_move("bench_src", "bench_dst", iterations, size)
            results[f"move_{size}"] = move_speed

            # Roundtrip benchmark
            roundtrip_speed = benchmark_roundtrip("bench_roundtrip", iterations, size)
            results[f"roundtrip_{size}"] = roundtrip_speed

        # Summary
        print("\n" + "=" * 60)
        print("PERFORMANCE SUMMARY")
        print("=" * 60)

        for size in message_sizes:
            print(f"\nMessage size: {size} bytes")
            print(f"  Write:     {results[f'write_{size}']:>7.0f} msg/sec")
            print(f"  Read:      {results[f'read_{size}']:>7.0f} msg/sec")
            print(f"  Peek:      {results[f'peek_{size}']:>7.0f} msg/sec")
            print(f"  Move:      {results[f'move_{size}']:>7.0f} msg/sec")
            print(f"  Roundtrip: {results[f'roundtrip_{size}']:>7.0f} msg/sec")

        # Verify the 1000+ messages/second claim
        print("\n" + "=" * 60)
        print("PERFORMANCE VERIFICATION")
        print("=" * 60)

        # Check if any operation achieves 1000+ messages/second
        verified = False
        for operation, speed in results.items():
            if speed >= 1000:
                verified = True
                print(f"✓ {operation}: {speed:.0f} msg/sec (>= 1000)")

        if verified:
            print("\n✓ SimpleBroker achieves 1000+ messages/second throughput")
        else:
            print("\n✗ Performance below 1000 messages/second in this environment")
            print("  Note: Performance varies by hardware, disk speed, and system load")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Benchmark SimpleBroker performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick benchmark with defaults
  python benchmark.py

  # High-volume test
  python benchmark.py --iterations 10000

  # Test different message sizes
  python benchmark.py --message-sizes 10,100,1024,10240

  # Comprehensive test
  python benchmark.py --iterations 5000 --message-sizes 100,1024,10240
""",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=1000,
        help="Number of messages to test (default: 1000)",
    )

    parser.add_argument(
        "--message-sizes",
        type=str,
        default="100,1024",
        help="Comma-separated list of message sizes in bytes (default: 100,1024)",
    )

    args = parser.parse_args()

    # Parse message sizes
    try:
        message_sizes = [int(s.strip()) for s in args.message_sizes.split(",")]
    except ValueError:
        print("Error: Invalid message sizes. Use comma-separated integers.")
        sys.exit(1)

    # Run benchmarks
    run_benchmarks(args.iterations, message_sizes)


if __name__ == "__main__":
    main()
