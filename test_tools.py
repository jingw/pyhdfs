import os
import subprocess
from pathlib import Path


def test_black() -> None:
    subprocess.check_call(["black", "--check", os.path.dirname(__file__)])


def test_flake8() -> None:
    subprocess.check_call(["flake8"], cwd=os.path.dirname(__file__))


def test_isort() -> None:
    subprocess.check_call(
        ["isort", "--check-only", "--diff", os.path.dirname(__file__)]
    )


def test_mypy() -> None:
    subprocess.check_call(["mypy", os.path.dirname(__file__)])


def test_pyupgrade() -> None:
    # also see python-version in .github/workflows/ci.yml
    subprocess.check_call(
        ["pyupgrade", "--py38-plus"] + list(Path(__file__).parent.glob("**/*.py"))
    )
