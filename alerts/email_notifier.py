"""
Email notifier — sends HTML DQ alert emails via SMTP with TLS.

Config example (YAML, under alerts.email):
    smtp_host_env: "SMTP_HOST"
    smtp_port: 587
    smtp_user_env: "SMTP_USER"
    smtp_password_env: "SMTP_PASSWORD"
    from_address: "dq-alerts@company.com"
    to_addresses:
      - "data-team@company.com"
    send_on_pass: false          # only email on failure (default)
    enabled: true
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

logger = logging.getLogger(__name__)

# ── HTML template ──────────────────────────────────────────────────────────────

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <style>
    body{{font-family:Arial,sans-serif;font-size:14px;color:#333;margin:0;padding:20px;background:#fafafa;}}
    .card{{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.1);overflow:hidden;max-width:800px;margin:auto;}}
    .header{{background:{header_color};color:#fff;padding:18px 24px;}}
    .header h1{{margin:0;font-size:20px;}}
    .meta{{padding:10px 24px;background:#f5f5f5;border-left:4px solid {header_color};font-size:12px;color:#555;}}
    .meta span{{margin-right:20px;}}
    .body{{padding:20px 24px;}}
    h2{{font-size:15px;border-bottom:1px solid #eee;padding-bottom:6px;margin-top:24px;}}
    table{{width:100%;border-collapse:collapse;}}
    th{{background:#f0f0f0;text-align:left;padding:7px 10px;font-size:12px;color:#666;}}
    td{{padding:7px 10px;border-bottom:1px solid #f5f5f5;font-size:13px;vertical-align:top;}}
    .pass{{color:#27ae60;font-weight:bold;}}
    .fail{{color:#e74c3c;font-weight:bold;}}
    code{{background:#f0f0f0;padding:1px 4px;border-radius:3px;font-family:monospace;font-size:12px;}}
    .footer{{padding:16px 24px;font-size:11px;color:#aaa;text-align:center;border-top:1px solid #eee;}}
  </style>
</head>
<body>
<div class="card">
  <div class="header"><h1>{title}</h1></div>
  <div class="meta">
    <span><strong>Pipeline:</strong> {pipeline_name}</span>
    <span><strong>Table:</strong> {table_name}</span>
    <span><strong>Environment:</strong> {environment}</span>
    {run_id_span}
  </div>
  <div class="body">
    <h2>Summary</h2>
    <table>
      <tr><th>Validator</th><th>Status</th><th>Message</th></tr>
      {summary_rows}
    </table>
    {failure_section}
  </div>
  <div class="footer">Data Quality Monitoring Framework &mdash; {timestamp}</div>
</div>
</body>
</html>
"""

_SUMMARY_ROW = (
    "<tr><td>{validator}</td>"
    '<td class="{css}">{status}</td>'
    "<td>{message}</td></tr>"
)

_FAILURE_SECTION = "<h2>Failure Details</h2>{tables}"

_FAILURE_TABLE = (
    "<p><strong>{validator}</strong></p><table><tr>{headers}</tr>{rows}</table>"
)


class EmailNotifier:
    """
    Sends HTML DQ reports via SMTP.

    Usage:
        notifier = EmailNotifier(config["alerts"]["email"])
        notifier.send_dq_report(
            table_name="fact_orders",
            pipeline_name="orders_etl",
            results=[...],
            overall_passed=False,
        )
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.smtp_host: str = os.environ.get(
            config.get("smtp_host_env", "SMTP_HOST"),
            config.get("smtp_host", "localhost"),
        )
        self.smtp_port: int = int(config.get("smtp_port", 587))
        self.smtp_user: str = os.environ.get(
            config.get("smtp_user_env", "SMTP_USER"),
            config.get("smtp_user", ""),
        )
        self.smtp_password: str = os.environ.get(
            config.get("smtp_password_env", "SMTP_PASSWORD"),
            config.get("smtp_password", ""),
        )
        self.from_address: str = config.get("from_address", "dq-alerts@company.com")
        self.to_addresses: list[str] = config.get("to_addresses", [])
        self.send_on_pass: bool = bool(config.get("send_on_pass", False))
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
        """Send a DQ run report. Returns True on successful delivery."""
        if not self.enabled:
            logger.info("Email notifier disabled — skipping.")
            return True
        if not self.to_addresses:
            logger.warning("No to_addresses configured — cannot send email.")
            return False
        if overall_passed and not self.send_on_pass:
            logger.info("DQ passed — skipping email (send_on_pass=false).")
            return True

        status = "PASSED" if overall_passed else "FAILED"
        header_color = "#27ae60" if overall_passed else "#e74c3c"
        title = f"DQ {status}: {table_name}"

        summary_rows = "\n".join(
            _SUMMARY_ROW.format(
                validator=r.get("validator", ""),
                css="pass" if r.get("passed") else "fail",
                status="PASS" if r.get("passed") else "FAIL",
                message=self._esc(r.get("message", "")),
            )
            for r in results
        )

        failed = [r for r in results if not r.get("passed", True)]
        failure_section = ""
        if failed:
            tables = "\n".join(self._build_failure_table(r) for r in failed)
            failure_section = _FAILURE_SECTION.format(tables=tables)

        run_id_span = (
            f"<span><strong>Run ID:</strong> <code>{run_id}</code></span>"
            if run_id
            else ""
        )

        html = _HTML.format(
            title=title,
            header_color=header_color,
            pipeline_name=self._esc(pipeline_name),
            table_name=self._esc(table_name),
            environment=self._esc(environment),
            run_id_span=run_id_span,
            summary_rows=summary_rows,
            failure_section=failure_section,
            timestamp=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        )

        return self._send(subject=title, html=html)

    # ── Private ────────────────────────────────────────────────────────────────

    def _build_failure_table(self, result: dict[str, Any]) -> str:
        validator = self._esc(result.get("validator", "Unknown"))
        failures = result.get("failures", [])
        if not failures:
            return f"<p><strong>{validator}</strong>: {self._esc(result.get('message', ''))}</p>"
        headers = list(failures[0].keys())
        header_cells = "".join(f"<th>{h}</th>" for h in headers)
        rows = "".join(
            "<tr>"
            + "".join(f"<td>{self._esc(str(f.get(h, '')))}</td>" for h in headers)
            + "</tr>"
            for f in failures[:25]
        )
        return _FAILURE_TABLE.format(
            validator=validator, headers=header_cells, rows=rows
        )

    @staticmethod
    def _esc(text: str) -> str:
        return (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    def _send(self, subject: str, html: str) -> bool:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.from_address
        msg["To"] = ", ".join(self.to_addresses)
        msg.attach(MIMEText(html, "html"))
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_address, self.to_addresses, msg.as_string())
            logger.info("Email sent to %s", self.to_addresses)
            return True
        except smtplib.SMTPException as exc:
            logger.error("Failed to send email: %s", exc)
            return False
