"""Run the pinned SQLFluff from the project venv with a stable dbt working directory."""
import os
from pathlib import Path
import subprocess
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    executable = root / '.venv-dbt' / ('Scripts/sqlfluff.exe' if os.name == 'nt' else 'bin/sqlfluff')
    if not executable.is_file():
        raise SystemExit('Install requirements-dbt.txt in .venv-dbt first; see dbt/README.md')
    paths = [str(Path(path).resolve().relative_to(root / 'dbt')) for path in sys.argv[1:]]
    result = subprocess.run(
        [str(executable), 'lint', '--config', '.sqlfluff', *(paths or ['models', 'tests'])],
        cwd=root / 'dbt', check=False,
    )
    raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
