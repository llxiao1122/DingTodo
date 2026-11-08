import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTRY_SCRIPT = PROJECT_ROOT / "event_worker.py"


class ProductionEntryImportTests(unittest.TestCase):
    def test_run_path_help_imports_from_unrelated_cwd(self):
        wrapper = (
            "import runpy, sys; "
            "sys.dont_write_bytecode = True; "
            "entry = sys.argv[1]; "
            "sys.argv = [entry, '--help']; "
            "runpy.run_path(entry, run_name='__main__')"
        )
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        unrelated_cwd = Path(tempfile.gettempdir())

        result = subprocess.run(
            [sys.executable, "-I", "-c", wrapper, str(ENTRY_SCRIPT)],
            cwd=unrelated_cwd,
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )

        self.assertNotEqual(unrelated_cwd.resolve(), PROJECT_ROOT)
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        self.assertIn("usage:", result.stdout.lower())


if __name__ == "__main__":
    unittest.main()
