"""
main.py
=======
Runs the full Embodied Carbon Observatory pipeline in order.

Order:
1. ec3_ingest.py     — plants + EPD versions from EC3 API
2. egrid_ingest.py   — grid carbon from EPA eGRID
3. compute_attribution.py — GWP attribution computation

Usage:
    python main.py                    # full pipeline
    python main.py --step ec3         # EC3 only
    python main.py --step egrid       # eGRID only
    python main.py --step attribution # attribution only
    python main.py --category concrete # one category
    python main.py --dry-run          # validate without writing

Requirements:
    pip install -r requirements.txt
"""

import os
import sys
import logging
import argparse
import subprocess
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
log = logging.getLogger(__name__)


def run_step(script: str, extra_args: list = None) -> bool:
    """Run a pipeline step as a subprocess. Returns True if successful."""
    cmd = [sys.executable, script] + (extra_args or [])
    log.info(f"\n{'=' * 60}")
    log.info(f"Running: {' '.join(cmd)}")
    log.info(f"{'=' * 60}")

    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))

    if result.returncode != 0:
        log.error(f"Step failed: {script} (exit code {result.returncode})")
        return False

    log.info(f"Step complete: {script}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run the Embodied Carbon Observatory data pipeline"
    )
    parser.add_argument(
        "--step",
        choices=["ec3", "egrid", "attribution", "all"],
        default="all",
        help="Which step to run (default: all)"
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Material category filter (e.g. concrete, steel, timber)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing to database"
    )
    args = parser.parse_args()

    # Build shared args
    extra = []
    if args.category:
        extra += ["--category", args.category]
    if args.dry_run:
        extra += ["--dry-run"]

    start = datetime.now()
    log.info("=" * 60)
    log.info("EMBODIED CARBON OBSERVATORY — FULL PIPELINE")
    log.info(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Step: {args.step}")
    log.info(f"Category: {args.category or 'all'}")
    log.info(f"Dry run: {args.dry_run}")
    log.info("=" * 60)

    steps = {
        "ec3":         ("ec3_ingest.py", extra),
        "egrid":       ("egrid_ingest.py", extra),
        "attribution": ("compute_attribution.py", extra),
    }

    if args.step == "all":
        run_order = ["ec3", "egrid", "attribution"]
    else:
        run_order = [args.step]

    success = True
    for step_name in run_order:
        script, step_extra = steps[step_name]
        if not run_step(script, step_extra):
            log.error(f"Pipeline failed at step: {step_name}")
            success = False
            break

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"\nPipeline {'complete' if success else 'FAILED'} in {elapsed:.0f}s")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
