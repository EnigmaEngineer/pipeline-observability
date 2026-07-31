"""The only test entrypoint. CI calls this and nothing else.

A module that reports zero cases fails the run. That rule is here because the usual way a
test suite lies is not a wrong assertion, it is a file the runner imported and never
executed. A green tick over nothing is worse than a red one.
"""

import importlib
import sys

MODULES = [
    "tests.test_model",
    "tests.test_schema",
    "tests.test_generate",
    "tests.test_pipeline",
]


def main():
    failed = []
    cases = 0
    for name in MODULES:
        mod = importlib.import_module(name)
        checks = mod.run()
        checks.report()
        cases += checks.total
        if checks.failures:
            failed.append(name)
        if checks.total == 0:
            failed.append(f"{name} (asserted nothing)")

    print(f"\n{len(MODULES)} modules, {cases} cases")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
