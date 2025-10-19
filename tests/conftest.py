from datetime import UTC, datetime
import hashlib
from pathlib import Path
import time
from typing import Any, AsyncGenerator, Callable, cast

import pytest
import pytest_asyncio
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
from core.database import FileDatabase
from core.models import FileRecord


@pytest_asyncio.fixture(scope="function")
async def test_db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest_asyncio.fixture(scope="function")
async def database(test_db_path: Path) -> AsyncGenerator[FileDatabase, Any]:
    """Provide a connected database instance."""
    db = FileDatabase(test_db_path)
    await db.connect()
    yield db
    await db.close()


@pytest_asyncio.fixture(scope="function")
async def sample_file_record() -> FileRecord:
    """Provide a sample file record for testing."""
    return FileRecord(
        file_path=Path("original/doge.png"),
        file_hash=hashlib.sha256(b"original/doge.png").hexdigest(),
        file_size=12345,
        created_at=datetime.now(UTC),
    )

class BenchmarkResult:
    """Container for benchmark results with formatted output."""
    
    def __init__(
        self,
        name: str,
        min_time: float,
        max_time: float,
        mean_time: float,
        median_time: float,
        std_dev: float,
        iterations: int,
        times: list[float],
    ):
        self.name = name
        self.min_time = min_time
        self.max_time = max_time
        self.mean_time = mean_time
        self.median_time = median_time
        self.std_dev = std_dev
        self.iterations = iterations
        self.times = times
        
        self.ops_per_sec = 1 / mean_time if mean_time > 0 else 0
        
        sorted_times = sorted(times)
        n = len(sorted_times)
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        self.q1 = sorted_times[q1_idx]
        self.q3 = sorted_times[q3_idx]
        self.iqr = self.q3 - self.q1
        
        lower_bound = self.q1 - 1.5 * self.iqr
        upper_bound = self.q3 + 1.5 * self.iqr
        self.outliers = sum(1 for t in times if t < lower_bound or t > upper_bound)
    
    def __str__(self) -> str:
        """Format benchmark results for display."""
        return (
            f"\n{'='*70}\n"
            f"Benchmark: {self.name}\n"
            f"{'-'*70}\n"
            f"Iterations:  {self.iterations}\n"
            f"Min Time:    {self.min_time*1000:.2f} ms\n"
            f"Max Time:    {self.max_time*1000:.2f} ms\n"
            f"Mean Time:   {self.mean_time*1000:.2f} ms\n"
            f"Median Time: {self.median_time*1000:.2f} ms\n"
            f"Std Dev:     {self.std_dev*1000:.2f} ms\n"
            f"{'='*70}"
        )


_benchmark_results: list[BenchmarkResult] = []

def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus: int, config: Config):
    """Hook to add benchmark summary to pytest output."""
    if not _benchmark_results:
        return
    
    terminalreporter.section("Benchmark Results")
    terminalreporter.write_line("")
    terminalreporter.write_line("=" * 150)
    
    header = (
        f"{'Name':<40} {'Min (ms)':>10} {'Max (ms)':>10} {'Mean (ms)':>11} "
        f"{'StdDev (ms)':>12} {'Median (ms)':>12} {'IQR (ms)':>10} "
        f"{'Outliers':>10} {'OPS':>12} {'Rounds':>8}"
    )
    terminalreporter.write_line(header)
    terminalreporter.write_line("-" * 150)
    
    for result in sorted(_benchmark_results, key=lambda x: x.mean_time):
        # Color code based on performance
        if result.mean_time < 0.001:  # < 1ms
            color = "green"
        elif result.mean_time < 0.01:  # < 10ms
            color = "blue"
        elif result.mean_time < 0.1:  # < 100ms
            color = "yellow"
        else:
            color = "red"
        
        row = (
            f"{result.name:<40} "
            f"{result.min_time * 1000:>10.2f} "
            f"{result.max_time * 1000:>10.2f} "
            f"{result.mean_time * 1000:>11.2f} "
            f"{result.std_dev * 1000:>12.2f} "
            f"{result.median_time * 1000:>12.2f} "
            f"{result.iqr * 1000:>10.2f} "
            f"{result.outliers:>10} "
            f"{result.ops_per_sec:>12.2f} "
            f"{result.iterations:>8}"
        )
        terminalreporter.write_line(row, **{color: True})
    
    terminalreporter.write_line("=" * 150)
    terminalreporter.write_line("")
    terminalreporter.write_line("Legend:")
    terminalreporter.write_line("  IQR: Interquartile Range (Q3 - Q1)")
    terminalreporter.write_line("  Outliers: Values outside 1.5 * IQR from Q1/Q3")
    terminalreporter.write_line("  OPS: Operations Per Second (1 / Mean)")
    terminalreporter.write_line("  Rounds: Number of iterations")
    terminalreporter.write_line("")
    
@pytest.fixture
def async_benchmark(request: pytest.FixtureRequest):
    """Fixture for benchmarking async functions with detailed metrics."""
    
    async def _benchmark(
        func: Callable [..., Any],
        *args: Any,
        iterations: int = 5,
        warmup: int = 1,
        **kwargs: Any,
    ) -> BenchmarkResult:
        """
        Benchmark an async function.
        
        :param func: Async function to benchmark.
        :param iterations: Number of times to run the function.
        :param warmup: Number of warmup runs (not counted).
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        :return: BenchmarkResult with statistics.
        """
        # Warmup runs
        for _ in range(warmup):
            await func(*args, **kwargs)
        
        # Benchmark runs
        times: list[float] = []
        for _ in range(iterations):
            start = time.perf_counter()
            await func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        
        times_sorted = sorted(times)
        min_time = times_sorted[0]
        max_time = times_sorted[-1]
        mean_time = sum(times) / len(times)
        median_time = times_sorted[len(times) // 2]
        
        variance = sum((t - mean_time) ** 2 for t in times) / len(times)
        std_dev = variance ** 0.5
        
        test_name = cast(str, request.node.name) # type: ignore
        
        result = BenchmarkResult(
            name=test_name,
            min_time=min_time,
            max_time=max_time,
            mean_time=mean_time,
            median_time=median_time,
            std_dev=std_dev,
            iterations=iterations,
            times=times,
        )
        
        _benchmark_results.append(result)

        
        return result
    
    return _benchmark