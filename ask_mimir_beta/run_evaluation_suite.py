"""Run Ask Mimir's broad deterministic suite and optional live-model sample."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def _run(label: str, command: list[str]) -> bool:
    print(f"\n=== {label} ===", flush=True)
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return completed.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/ask-mimir-evaluation"),
    )
    parser.add_argument("--include-live", action="store_true")
    parser.add_argument("--base-url", default="http://127.0.0.1:10100")
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument("--subject", default="representative-live-evaluation")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    python = sys.executable
    checks = [
        _run("Routing and entity resolution", [python, "evaluate_workflow_routing.py"]),
        _run(
            "Evidence availability",
            [
                python,
                "evaluate_evidence_offline.py",
                "--output",
                str(args.output_dir / "evidence-report.json"),
            ],
        ),
        _run(
            "Multi-turn scope journeys",
            [
                python,
                "evaluate_question_journeys.py",
                "--output",
                str(args.output_dir / "journey-report.json"),
                "--summary-output",
                str(args.output_dir / "journey-summary.md"),
                "--quiet",
            ],
        ),
    ]

    if args.include_live:
        checks.append(
            _run(
                "Representative live-model sample",
                [
                    python,
                    "evaluate_lab.py",
                    "--base-url",
                    args.base_url,
                    "--cases-file",
                    "representative_live_eval_cases.json",
                    "--tier",
                    args.tier,
                    "--subject",
                    args.subject,
                    "--output",
                    str(args.output_dir / "live-model-report.json"),
                ],
            )
        )

    passed = all(checks)
    print(
        f"\nAsk Mimir evaluation suite: {'PASS' if passed else 'FAIL'}; "
        f"live model sample: {'included' if args.include_live else 'not requested'}; "
        f"reports: {args.output_dir}"
    )
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
