"""A three-function test helper, because pytest is not a dependency here.

The reason it exists rather than pytest: this repo's CI runs plain scripts, and a test
file written for one runner and executed by another can exit 0 having asserted nothing.
Making the runner and the assertion helper the same thing removes that gap.

A module with zero cases is treated as a failure by run_all. Silence is not a pass.
"""


class Checks:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failures = []

    def ok(self, cond, label):
        if cond:
            self.passed += 1
        else:
            self.failures.append(label)
        return bool(cond)

    def eq(self, got, want, label):
        return self.ok(got == want, f"{label}: got {got!r}, want {want!r}")

    def raises(self, exc_type, fn, label):
        try:
            fn()
        except exc_type:
            self.passed += 1
            return True
        except Exception as e:  # wrong exception is a different failure than none
            self.failures.append(f"{label}: raised {type(e).__name__} not {exc_type.__name__}")
            return False
        self.failures.append(f"{label}: nothing raised")
        return False

    def raises_message(self, exc_type, fragment, fn, label):
        """Assert the exception and the text of it.

        Added 2026-08-03 after the log space test on 08-02 passed against code with the
        guard deleted, because `math.log(0)` raises `ValueError` all by itself. Checking
        the type alone proves the language did something. Checking the message proves the
        guard did. Any test of a guard that raises a builtin exception type belongs here
        rather than in `raises`.
        """
        try:
            fn()
        except exc_type as e:
            if fragment in str(e):
                self.passed += 1
                return True
            self.failures.append(
                f"{label}: raised {exc_type.__name__} without {fragment!r}: {e}")
            return False
        except Exception as e:
            self.failures.append(
                f"{label}: raised {type(e).__name__} not {exc_type.__name__}")
            return False
        self.failures.append(f"{label}: nothing raised")
        return False

    @property
    def total(self):
        return self.passed + len(self.failures)

    def report(self):
        for f in self.failures:
            print(f"  FAIL {self.name}: {f}")
        print(f"{self.name:<22} {self.passed}/{self.total} passed")
        return 0 if not self.failures else 1
