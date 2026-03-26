"""
Slack notifier — sends rich Block Kit DQ alert messages to a Slack webhook.

Config example (YAML, under alerts.slack):
    webhook_url_env: "SLACK_WEBHOOK_URL"   # env var holding the webhook URL
    channel: "#data-quality"               # informational only (webhook ignores it)
    mention_on_failure: "@data-team"       # prepended to failure messages
    enabled: true
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_SEVERITY_EMOJI = {
    "pass": "✅",
    "info": "🟢",
    "warning": "🟡",
    "error": "🟠",
    "critical": "🔴",
}
_SEVERITY_COLOR = {
    "pass": "#36A64F",
    "info": "#36A64F",
    "warning": "#FFCC00",
    "error": "#FF6600",
    "critical": "#FF0000",
}


class SlackNotifier:
    """
    Sends DQ run reports and simple alerts to Slack using Block Kit formatting.

    Usage:
        notifier = SlackNotifier(config["alerts"]["slack"])
        notifier.send_dq_report(
            table_name="fact_orders",
            pipeline_name="orders_etl",
            results=[...],        # list of validator result dicts
            overall_passed=False,
            run_id="abc-123",
            environment="prod",
        )
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.webhook_url: str | None = (
            os.environ.get(
                config.get("webhook_url_env", "SLACK_WEBHOOK_URL"),
                config.get("webhook_url", ""),
            )
            or None
        )
        self.channel: str = config.get("channel", "#data-quality")
        self.mention: str = config.get("mention_on_failure", "")
        self.enabled: bool = bool(config.get("enabled", True))

    # ── Public ─────────────────────────────────────────────────────────────────

    def send_dq_report(
        self,
        table_name: str,
        pipeline_name: str,
        results: list[dict[str, Any]],
        overall_passed: bool,
        run_id: str = "",
        environment: str = "unknown",
    ) -> bool:
        """Send a full DQ run report. Returns True on successful delivery."""
        if not self._pre_flight():
            return not self.enabled  # disabled → True (not an error); no URL → False

        severity = "pass" if overall_passed else "error"
        emoji = _SEVERITY_EMOJI[severity]
        color = _SEVERITY_COLOR[severity]
        status = "PASSED" if overall_passed else "FAILED"
        failed = [r for r in results if not r.get("passed", True)]

        blocks: list[dict] = [
            self._header(f"{emoji} DQ {status}: {table_name}"),
            self._context(pipeline_name, environment, run_id),
            self._divider(),
        ]

        if overall_passed:
            blocks.append(
                self._section(
                    f"All {len(results)} validator(s) passed. :white_check_mark:"
                )
            )
        else:
            if self.mention:
                blocks.append(
                    self._section(f"{self.mention} — DQ failure requires attention.")
                )
            for failure in failed:
                blocks.extend(self._failure_blocks(failure))

        blocks += [self._divider(), self._summary(results)]

        return self._post({"attachments": [{"color": color, "blocks": blocks}]})

    def send_simple_alert(self, text: str, severity: str = "error") -> bool:
        """Send a plain-text alert (pipeline-level errors, etc.)."""
        if not self._pre_flight():
            return not self.enabled
        emoji = _SEVERITY_EMOJI.get(severity, "⚠️")
        return self._post({"text": f"{emoji} {text}"})

    # ── Block builders ─────────────────────────────────────────────────────────

    @staticmethod
    def _header(text: str) -> dict:
        return {
            "type": "header",
            "text": {"type": "plain_text", "text": text[:150], "emoji": True},
        }

    @staticmethod
    def _section(text: str) -> dict:
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    @staticmethod
    def _divider() -> dict:
        return {"type": "divider"}

    @staticmethod
    def _context(pipeline: str, env: str, run_id: str) -> dict:
        elements = [
            {"type": "mrkdwn", "text": f"*Pipeline:* {pipeline}"},
            {"type": "mrkdwn", "text": f"*Env:* `{env}`"},
        ]
        if run_id:
            elements.append({"type": "mrkdwn", "text": f"*Run ID:* `{run_id}`"})
        return {"type": "context", "elements": elements}

    @staticmethod
    def _failure_blocks(result: dict[str, Any]) -> list[dict]:
        validator = result.get("validator", "Unknown")
        table = result.get("table_name", "")
        failures = result.get("failures", [])
        message = result.get("message", "")

        header = f"*{validator}*" + (f" — `{table}`" if table else "")
        if message:
            header += f"\n_{message}_"

        blocks: list[dict] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}}
        ]

        if failures:
            lines = []
            for f in failures[:10]:
                col = f.get("column", "")
                msg = f.get("message", "")
                lines.append(f"• `{col}`: {msg}" if col else f"• {msg}")
            if len(failures) > 10:
                lines.append(f"_...and {len(failures) - 10} more_")
            blocks.append(SlackNotifier._section("\n".join(lines)))

        return blocks

    @staticmethod
    def _summary(results: list[dict[str, Any]]) -> dict:
        passed = sum(1 for r in results if r.get("passed", True))
        failed = len(results) - passed
        text = f"*Summary:* {passed}/{len(results)} validator(s) passed"
        if failed:
            text += f" — *{failed} failed* :x:"
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    # ── HTTP ───────────────────────────────────────────────────────────────────

    def _pre_flight(self) -> bool:
        if not self.enabled:
            logger.info("Slack notifier disabled — skipping.")
            return False
        if not self.webhook_url:
            logger.warning(
                "SLACK_WEBHOOK_URL not configured — cannot send Slack notification."
            )
            return False
        return True

    def _post(self, payload: dict) -> bool:
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=10) as resp:  # nosec B310
                if resp.status != 200:
                    logger.error("Slack returned HTTP %d", resp.status)
                    return False
            logger.info("Slack notification sent.")
            return True
        except URLError as exc:
            logger.error("Failed to send Slack notification: %s", exc)
            return False
