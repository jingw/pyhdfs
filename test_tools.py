import os
import subprocess


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
