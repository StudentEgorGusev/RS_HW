import pathlib
import pytest

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()

class PassedCounter:
    def __init__(self):
        self.passed = 0

    def pytest_report_teststatus(self, report, config):
        if report.when == 'call' and report.passed:
            self.passed += 1

    def reset(self):
        self.passed = 0

score = 0
counter = PassedCounter()

pytest.main(['-vs', SCRIPT_DIR / 'test_server.py'], plugins=[counter])
if counter.passed == 4:
    score += 6
counter.reset()
print()

pytest.main(['-vs', SCRIPT_DIR / 'test_client.py'], plugins=[counter])
if counter.passed == 3:
    score += 4

print(f"\nSCORE: {score}")