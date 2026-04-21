"""
report_renderer.py
Chorus AI Systems — Data Analytics Manager (DAM)

Two self-contained HTML render functions:
  render_pdf_html()       — WeasyPrint-optimised executive summary (PDF target)
  render_dashboard_html() — Dark-theme static site (GitHub Pages target)

Design system:
  Typefaces : Playfair Display (display serif) · Inter (body) · JetBrains Mono (data)
  Palette   : gold #c9a84c · ink #0d1117 · navy #070d1a
  Status    : green #059669 · amber #d97706 · red #dc2626
  Grid      : 40px body margin, Elam-style consistent gutters
"""

from __future__ import annotations

import statistics
from datetime import datetime
from typing import Optional

from core.schemas import (
    ClaimType, FactListEntry, KPIDomain,
    Stage5Input, ThresholdStatus,
)
from core.historical_kpis import HISTORICAL_BENCHMARKS


# ── Shared constants ───────────────────────────────────────────────────────────

DOMAIN_LABELS = {
    "fulfillment":           "Fulfillment",
    "carrier_performance":   "Carrier Performance",
    "cost":                  "Cost & Efficiency",
    "operational_integrity": "Operational Integrity",
}

DOMAINS = ["fulfillment", "carrier_performance", "cost", "operational_integrity"]

GOOGLE_FONTS = (
    "https://fonts.googleapis.com/css2?"
    "family=Playfair+Display:wght@700;900"
    "&family=Inter:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;1,400"
    "&family=JetBrains+Mono:wght@400;500;600"
    "&display=swap"
)


# ── Shared formatters ──────────────────────────────────────────────────────────

def _fmt_val(f: FactListEntry) -> str:
    v = f.final_value
    if f.kpi_name in ("On-Time Ship Rate", "On-Time Delivery Rate",
                       "Unshipped Orders Rate", "Shipment Match Rate", "Carrier Mix"):
        return f"{v:.1%}"
    if f.kpi_name in ("Order to Ship Time", "Transit Time", "Label Lag"):
        return f"{v:.1f}h"
    if f.kpi_name in ("Shipping Cost per Order", "Cost by Carrier"):
        return f"${v:.2f}"
    return str(round(v, 3))


def _fmt_val_parts(f: FactListEntry) -> tuple[str, str]:
    """Return (numeric_string, unit) for large-display rendering."""
    v = f.final_value
    if f.kpi_name in ("On-Time Ship Rate", "On-Time Delivery Rate",
                       "Unshipped Orders Rate", "Shipment Match Rate", "Carrier Mix"):
        return f"{v * 100:.1f}", "%"
    if f.kpi_name in ("Order to Ship Time", "Transit Time", "Label Lag"):
        return f"{v:.1f}", "h"
    if f.kpi_name in ("Shipping Cost per Order", "Cost by Carrier"):
        return f"{v:.2f}", "$"
    return str(round(v, 3)), ""


def _fmt_wow(f: FactListEntry) -> str:
    if f.wow_delta is None:
        return "—"
    d = f.wow_delta
    arrow = "↑" if d > 0 else "↓" if d < 0 else "→"
    if f.kpi_name in ("On-Time Ship Rate", "On-Time Delivery Rate",
                       "Unshipped Orders Rate", "Shipment Match Rate", "Carrier Mix"):
        return f"{arrow} {abs(d * 100):.1f}pp WoW"
    if f.kpi_name in ("Order to Ship Time", "Transit Time", "Label Lag"):
        return f"{arrow} {abs(d):.1f}h WoW"
    if f.kpi_name in ("Shipping Cost per Order", "Cost by Carrier"):
        return f"{arrow} ${abs(d):.2f} WoW"
    return f"{arrow} {abs(d):.3f} WoW"


def _domain_score(facts: list[FactListEntry]) -> float:
    if not facts:
        return 5.0
    vals = [
        9.0 if f.threshold_status == ThresholdStatus.green else
        6.0 if f.threshold_status == ThresholdStatus.yellow else
        3.0 if f.threshold_status == ThresholdStatus.red else 5.5
        for f in facts
    ]
    return round(statistics.mean(vals), 1)


def _hist_rows_html(f: FactListEntry, row_class: str = "ph-row") -> str:
    bench = HISTORICAL_BENCHMARKS.get(f.kpi_name)
    if bench is None:
        return ""
    try:
        current = float(f.python_value)
    except (TypeError, ValueError):
        return ""

    direction = bench["direction"]
    unit      = bench["unit"]

    def fmt_h(val: float) -> str:
        if unit == "%":  return f"{val:.1f}%"
        if unit == "$":  return f"${val:.2f}"
        return f"{val:.1f}{unit}"

    def delta_parts(val: float) -> tuple[str, str]:
        delta = current - val
        if abs(delta) < 0.001:
            return "—", "dn"
        if unit == "%":
            disp = f"{'▲' if delta > 0 else '▼'} {abs(delta):.1f}pp"
        elif unit == "$":
            disp = f"{'▲' if delta > 0 else '▼'} ${abs(delta):.2f}"
        else:
            disp = f"{'▲' if delta > 0 else '▼'} {abs(delta):.1f}{unit}"

        if direction == "neutral":
            css = "dn"
        elif direction == "up":
            css = "dp" if delta > 0 else "dn"
        else:
            css = "dp" if delta < 0 else "dn"
        return disp, css

    periods = [
        ("Last week",   bench["last_week"]),
        ("Month avg",   bench["month_avg"]),
        ("3-mo avg",    bench["three_month_avg"]),
        ("Year avg",    bench["year_avg"]),
    ]
    rows = ""
    for label, val in periods:
        disp, css = delta_parts(val)
        rows += (
            f'<div class="{row_class}">'
            f'<span class="ph-period">{label}</span>'
            f'<span class="ph-val">{fmt_h(val)}</span>'
            f'<span class="ph-delta {css}">{disp}</span>'
            f'</div>'
        )
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# PDF TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════

def render_pdf_html(inp: Stage5Input) -> tuple[str, list[str]]:
    """
    Generate WeasyPrint-optimised HTML for the executive PDF report.
    Returns (html_string, sections_rendered).
    """
    factlist = inp.stage3_output.factlist
    insights = inp.stage4_output.verified_insights

    facts_by_domain: dict[str, list] = {}
    for f in factlist:
        facts_by_domain.setdefault(f.domain.value, []).append(f)

    insights_by_domain: dict[str, list] = {}
    for ins in insights:
        insights_by_domain.setdefault(ins.domain.value, []).append(ins)

    s6_blocks: dict[str, object] = {}
    if inp.stage6_output:
        for blk in inp.stage6_output.domain_blocks:
            s6_blocks[blk.domain.value] = blk

    n_green  = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.green)
    n_yellow = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.yellow)
    n_red    = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.red)
    n_info   = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.informational)

    red_facts    = [f for f in factlist if f.threshold_status == ThresholdStatus.red]
    yellow_facts = [f for f in factlist if f.threshold_status == ThresholdStatus.yellow]

    if red_facts:
        hl_status  = "red"
        hl_icon    = "●"
        headline   = (
            f"{len(red_facts)} metric{'s' if len(red_facts) > 1 else ''} require "
            f"immediate attention — {red_facts[0].kpi_name} is below threshold "
            f"at {_fmt_val(red_facts[0])}."
        )
    elif yellow_facts:
        hl_status  = "yellow"
        hl_icon    = "◐"
        headline   = (
            f"{len(yellow_facts)} metric{'s' if len(yellow_facts) > 1 else ''} flagged "
            f"for review — {yellow_facts[0].kpi_name} at {_fmt_val(yellow_facts[0])} "
            f"is below target."
        )
    else:
        hl_status  = "green"
        hl_icon    = "✓"
        headline   = "All operational metrics within target — no escalation required this week."

    match_rate_val = next(
        (f.final_value for f in factlist if f.kpi_name == "Shipment Match Rate"), None
    )
    footer_match = f"{match_rate_val:.1%}" if match_rate_val else "N/A"
    acceptance   = inp.stage4_output.claim_acceptance_rate
    agreement    = inp.stage4_output.cross_verifier_agreement
    disclosures  = [s.failure_reason for s in inp.degradation_signals]

    sections_rendered = []
    domain_html = ""

    for domain in DOMAINS:
        d_facts    = facts_by_domain.get(domain, [])
        d_insights = insights_by_domain.get(domain, [])
        if not d_facts:
            continue

        observations = [i for i in d_insights if i.claim_type == ClaimType.observation]
        hypotheses   = [i for i in d_insights if i.claim_type == ClaimType.hypothesis]
        actions      = [i for i in d_insights if i.claim_type == ClaimType.recommended_action]

        # KPI cards
        cards_html = ""
        for f in d_facts:
            s = f.threshold_status.value
            num, unit = _fmt_val_parts(f)
            wow = _fmt_wow(f)
            unit_html = f'<span class="kv-unit">{unit}</span>' if unit else ""
            cards_html += f"""
              <div class="kpi-card kpi-{s}">
                <div class="kpi-name">{f.kpi_name}</div>
                <div class="kpi-val-row">
                  <span class="kv-num">{num}</span>{unit_html}
                </div>
                <div class="kpi-wow">{wow}</div>
                <div class="kpi-chip kpi-chip-{s}">{s.replace('informational','INFO').upper()}</div>
              </div>"""

        # Insight bullets (Stage 6 if available, Stage 4 fallback)
        s6_blk = s6_blocks.get(domain)
        if s6_blk:
            obs_bullets = ""
            for obs in (observations + hypotheses)[:3]:
                obs_bullets += f'<div class="insight-item">{obs.claim_text}</div>'
            if not obs_bullets:
                obs_bullets = '<div class="insight-item insight-muted">KPI data above is authoritative this period.</div>'

            recs_html = ""
            for rec in s6_blk.recommendations[:2]:
                recs_html += f'<div class="s6-rec">{rec.text}</div>'

            analysis_html = f"""
              <div class="insight-panel">
                <div class="ip-label">Data Analysis</div>
                {obs_bullets}
              </div>
              <div class="s6-panel">
                <div class="s6-label">Expert Commentary</div>
                <div class="s6-commentary">{s6_blk.commentary}</div>
                <div class="s6-label s6-label-recs">Recommendations</div>
                {recs_html}
              </div>"""
        else:
            all_items = (observations + hypotheses)[:3] + actions[:2]
            bullets = ""
            for item in all_items:
                txt = item.claim_text
                cls = "insight-item"
                if item.claim_type == ClaimType.recommended_action:
                    act_txt = item.recommended_action or txt
                    bullets += f'<div class="insight-item insight-act"><span class="act-pill">Action</span>{act_txt}</div>'
                else:
                    bullets += f'<div class="{cls}">{txt}</div>'
            if not bullets:
                bullets = '<div class="insight-item insight-muted">No verified insights generated for this domain.</div>'

            analysis_html = f"""
              <div class="insight-panel">
                <div class="ip-label">Analysis &amp; Actions</div>
                {bullets}
              </div>"""

        domain_html += f"""
          <section class="domain-section" id="{domain}_block">
            <div class="domain-header">
              <span class="domain-name">{DOMAIN_LABELS[domain]}</span>
              <span class="domain-rule-line"></span>
            </div>
            <div class="kpi-grid">{cards_html}</div>
            {analysis_html}
          </section>"""

        sections_rendered.append(f"{domain}_block")

    sections_rendered += ["executive_headline", "verification_footer"]

    disc_items = (
        "".join(f"<div class='disc-item'>{d}</div>" for d in disclosures)
        if disclosures else "<div class='disc-item disc-none'>None — full pipeline completed.</div>"
    )

    if inp.stage6_output:
        s6_note = (
            f"Stage 6 Supply Chain Advisor: {len(inp.stage6_output.domain_blocks)} domain(s) · "
            f"{inp.stage6_output.total_chunks_retrieved} knowledge base chunks retrieved"
        )
    else:
        s6_note = "Stage 6 Supply Chain Advisor: unavailable this run"

    generated = datetime.now().strftime("%B %d, %Y  %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CAS·DAM Weekly Report — {inp.report_week}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="{GOOGLE_FONTS}" rel="stylesheet">
<style>
/* ── Reset ────────────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

/* ── Design tokens ────────────────────────────────────────── */
:root {{
  --gold:     #c9a84c;
  --ink:      #0d1117;
  --muted:    #64748b;
  --light:    #94a3b8;
  --rule:     #e2e8f0;
  --surface:  #f8fafc;
  --white:    #ffffff;

  --green:    #059669;  --green-bg:  #ecfdf5;
  --amber:    #d97706;  --amber-bg:  #fffbeb;
  --red:      #dc2626;  --red-bg:    #fef2f2;
  --info:     #64748b;  --info-bg:   #f8fafc;

  --ff-serif: 'Playfair Display', Georgia, 'Times New Roman', serif;
  --ff-sans:  'Inter', -apple-system, 'Segoe UI', sans-serif;
  --ff-mono:  'JetBrains Mono', 'SF Mono', Consolas, monospace;

  --margin: 40px;
}}

/* ── Page setup ───────────────────────────────────────────── */
@page {{ size: letter portrait; margin: 18px 0; }}

body {{
  font-family: var(--ff-sans);
  font-size: 10.5pt;
  color: var(--ink);
  background: var(--white);
  -webkit-font-smoothing: antialiased;
  line-height: 1.55;
  font-weight: 400;
}}

/* ── Masthead ─────────────────────────────────────────────── */
.masthead {{
  padding: 22px var(--margin) 0;
  background: var(--white);
}}
.masthead-inner {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 14px;
}}
.m-left {{ display: flex; flex-direction: column; gap: 3px; }}
.m-eyebrow {{
  font-family: var(--ff-sans);
  font-size: 8pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 2.5px;
  color: var(--gold);
}}
.m-title {{
  font-family: var(--ff-serif);
  font-size: 30pt;
  font-weight: 700;
  color: var(--ink);
  letter-spacing: -0.3px;
  line-height: 1.05;
}}
.m-subtitle {{
  font-family: var(--ff-sans);
  font-size: 10.5pt;
  font-weight: 400;
  color: var(--muted);
  letter-spacing: 0.2px;
}}
.m-right {{ text-align: right; }}
.m-week-label {{
  font-size: 8pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--muted);
  margin-bottom: 1px;
}}
.m-week {{
  font-family: var(--ff-sans);
  font-size: 14pt;
  font-weight: 700;
  color: var(--ink);
  line-height: 1.1;
}}
.m-runid {{
  font-family: var(--ff-mono);
  font-size: 8pt;
  color: var(--light);
  margin-top: 5px;
}}
.m-generated {{
  font-size: 8pt;
  color: var(--light);
  margin-top: 1px;
}}
.gold-bar {{
  height: 3px;
  background: var(--gold);
  margin: 0 calc(-1 * var(--margin));
}}
.ink-bar {{
  height: 1px;
  background: var(--ink);
  margin: 0 calc(-1 * var(--margin));
  margin-top: 0;
}}

/* ── Executive status bar ─────────────────────────────────── */
.exec-bar {{
  padding: 10px var(--margin);
  display: flex;
  align-items: center;
  gap: 10px;
  border-bottom: 1px solid var(--rule);
}}
.exec-bar.red    {{ background: var(--red-bg);   border-top: 3px solid var(--red);   }}
.exec-bar.yellow {{ background: var(--amber-bg); border-top: 3px solid var(--amber); }}
.exec-bar.green  {{ background: var(--green-bg); border-top: 3px solid var(--green); }}
.exec-icon {{ font-size: 11pt; line-height: 1; }}
.exec-bar.red    .exec-icon {{ color: var(--red);   }}
.exec-bar.yellow .exec-icon {{ color: var(--amber); }}
.exec-bar.green  .exec-icon {{ color: var(--green); }}
.exec-label {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--muted);
  white-space: nowrap;
}}
.exec-text {{
  font-size: 10.5pt;
  line-height: 1.4;
  color: var(--ink);
  font-weight: 500;
}}

/* ── Body ─────────────────────────────────────────────────── */
.body {{
  padding: 22px var(--margin) 0;
}}

/* ── Scorecard ────────────────────────────────────────────── */
.scorecard-eyebrow {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--muted);
  margin-bottom: 8px;
}}
.scorecard-strip {{
  display: flex;
  border: 1px solid var(--rule);
  margin-bottom: 26px;
}}
.sc-cell {{
  flex: 1;
  padding: 16px 10px 12px;
  text-align: center;
  border-right: 1px solid var(--rule);
  border-top: 3px solid transparent;
}}
.sc-cell:last-child {{ border-right: none; }}
.sc-cell.on-target  {{ border-top-color: var(--green); }}
.sc-cell.watch      {{ border-top-color: var(--amber); }}
.sc-cell.action     {{ border-top-color: var(--red);   }}
.sc-cell.info       {{ border-top-color: var(--light); }}
.sc-num {{
  display: block;
  font-family: var(--ff-sans);
  font-size: 44pt;
  font-weight: 800;
  line-height: 1;
  margin-bottom: 3px;
}}
.sc-cell.on-target .sc-num {{ color: var(--green); }}
.sc-cell.watch     .sc-num {{ color: var(--amber); }}
.sc-cell.action    .sc-num {{ color: var(--red);   }}
.sc-cell.info      .sc-num {{ color: var(--light); }}
.sc-lbl {{
  font-size: 7pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--muted);
}}

/* ── Domain section ───────────────────────────────────────── */
.domain-section {{
  margin-bottom: 10px;
  border: 1px solid var(--rule);
  border-top: 3px solid var(--gold);
  background: var(--surface);
}}
.kpi-card {{ page-break-inside: avoid; }}
.insight-panel {{ page-break-inside: avoid; }}
.s6-panel {{ page-break-inside: avoid; }}
.domain-header {{
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 10px 14px 8px;
  border-bottom: 1px solid var(--rule);
  background: var(--white);
}}
.domain-name {{
  font-family: var(--ff-sans);
  font-size: 8.5pt;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 2.5px;
  color: var(--ink);
  white-space: nowrap;
}}
.domain-rule-line {{
  flex: 1;
  height: 1px;
  background: var(--rule);
}}

/* ── KPI grid ─────────────────────────────────────────────── */
.kpi-grid {{
  display: flex;
  border-bottom: 1px solid var(--rule);
}}
.kpi-card {{
  flex: 1;
  min-width: 0;
  padding: 14px 16px 12px;
  border-right: 1px solid var(--rule);
  border-top: 3px solid transparent;
  background: var(--white);
}}
.kpi-card:last-child {{ border-right: none; }}
.kpi-card.kpi-green  {{ border-top-color: var(--green); }}
.kpi-card.kpi-yellow {{ border-top-color: var(--amber); }}
.kpi-card.kpi-red    {{ border-top-color: var(--red);   }}
.kpi-card.kpi-informational {{ border-top-color: var(--light); }}
.kpi-name {{
  font-size: 8pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--muted);
  margin-bottom: 8px;
}}
.kpi-val-row {{
  display: flex;
  align-items: baseline;
  gap: 2px;
  margin-bottom: 5px;
}}
.kv-num {{
  font-family: var(--ff-mono);
  font-size: 26pt;
  font-weight: 600;
  color: var(--ink);
  letter-spacing: -0.5px;
  line-height: 1;
}}
.kv-unit {{
  font-family: var(--ff-mono);
  font-size: 13pt;
  font-weight: 500;
  color: var(--muted);
}}
.kpi-wow {{
  font-family: var(--ff-mono);
  font-size: 8pt;
  color: var(--muted);
  margin-bottom: 7px;
}}
.kpi-chip {{
  display: inline-block;
  font-size: 7pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  padding: 2px 7px;
}}
.kpi-chip-green  {{ color: var(--green); background: var(--green-bg); }}
.kpi-chip-yellow {{ color: var(--amber); background: var(--amber-bg); }}
.kpi-chip-red    {{ color: var(--red);   background: var(--red-bg);   }}
.kpi-chip-informational {{ color: var(--info); background: var(--info-bg); }}

/* ── Insight & analysis panel ─────────────────────────────── */
.insight-panel {{
  background: var(--white);
  padding: 12px 14px 14px;
  border-top: 1px solid var(--rule);
}}
.ip-label {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: var(--ink);
  margin-bottom: 10px;
  padding-left: 8px;
  border-left: 3px solid var(--ink);
}}
.insight-item {{
  font-size: 9.5pt;
  line-height: 1.6;
  color: var(--ink);
  padding: 3px 0 3px 10px;
  border-left: 2px solid var(--rule);
  margin-bottom: 6px;
}}
.insight-item:last-child {{ margin-bottom: 0; }}
.insight-muted {{ color: var(--muted); font-style: italic; border-left-color: transparent; }}
.insight-act {{ border-left-color: var(--ink); }}
.act-pill {{
  font-size: 7pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  background: var(--ink);
  color: var(--white);
  padding: 1px 5px;
  margin-right: 6px;
  vertical-align: middle;
}}

/* ── Stage 6 expert panel ─────────────────────────────────── */
.s6-panel {{
  background: #fdf8ed;
  border-top: 2px solid var(--gold);
  padding: 12px 14px 14px;
}}
.s6-label {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: var(--gold);
  margin-bottom: 7px;
  padding-left: 8px;
  border-left: 3px solid var(--gold);
}}
.s6-label-recs {{
  margin-top: 14px;
}}
.s6-commentary {{
  font-size: 9.5pt;
  line-height: 1.65;
  color: var(--ink);
  margin-bottom: 10px;
}}
.s6-rec {{
  font-size: 9pt;
  color: var(--ink);
  padding: 6px 10px;
  background: var(--white);
  border-left: 3px solid var(--gold);
  border: 1px solid var(--rule);
  border-left: 3px solid var(--gold);
  margin-bottom: 5px;
  line-height: 1.55;
}}
.s6-rec:last-child {{ margin-bottom: 0; }}

/* ── Audit footer ─────────────────────────────────────────── */
.audit-footer {{
  background: var(--ink);
  color: rgba(255,255,255,0.55);
  padding: 18px var(--margin) 22px;
  margin-top: 24px;
  display: grid;
  grid-template-columns: 1.6fr 1fr;
  gap: 28px;
}}
.af-head {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: rgba(255,255,255,0.28);
  margin-bottom: 10px;
  display: block;
}}
.af-stats {{
  display: flex;
  gap: 24px;
  margin-bottom: 12px;
}}
.af-stat {{ display: flex; flex-direction: column; gap: 1px; }}
.af-val {{
  font-family: var(--ff-mono);
  font-size: 16pt;
  font-weight: 600;
  color: var(--white);
  line-height: 1;
}}
.af-lbl {{
  font-size: 8pt;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: rgba(255,255,255,0.3);
}}
.af-line {{
  font-size: 9pt;
  line-height: 1.8;
  color: rgba(255,255,255,0.45);
}}
.af-line strong {{ color: rgba(255,255,255,0.75); }}
.af-s6note {{
  font-size: 8pt;
  color: rgba(255,255,255,0.35);
  margin-top: 6px;
  font-style: italic;
}}
.af-stamp {{
  font-size: 7.5pt;
  color: rgba(255,255,255,0.2);
  margin-top: 10px;
  font-style: italic;
}}
.disc-item {{
  font-size: 8.5pt;
  line-height: 1.8;
  color: rgba(255,255,255,0.4);
  padding-left: 10px;
  position: relative;
}}
.disc-item::before {{ content: "·"; position: absolute; left: 0; }}
.disc-none {{ color: rgba(255,255,255,0.35); font-style: italic; padding-left: 0; }}
.disc-none::before {{ content: ""; }}

/* ── No-insights banner ───────────────────────────────────── */
.no-insights-banner {{
  background: var(--amber-bg);
  border: 1px solid var(--amber);
  border-left: 3px solid var(--amber);
  padding: 8px 12px;
  font-size: 8.5pt;
  color: #78350f;
  margin-bottom: 16px;
  font-style: italic;
}}
</style>
</head>
<body>

<!-- Masthead -->
<header class="masthead">
  <div class="masthead-inner">
    <div class="m-left">
      <div class="m-eyebrow">Chorus AI Systems &nbsp;·&nbsp; Confidential</div>
      <div class="m-title">Data Analytics Manager</div>
      <div class="m-subtitle">Weekly Operational Intelligence Report</div>
    </div>
    <div class="m-right">
      <div class="m-week-label">Week ending</div>
      <div class="m-week">{inp.report_week}</div>
      <div class="m-runid">{inp.run_id}</div>
      <div class="m-generated">{generated}</div>
    </div>
  </div>
  <div class="gold-bar"></div>
  <div class="ink-bar"></div>
</header>

<!-- Executive status -->
<div class="exec-bar {hl_status}" id="executive_headline">
  <span class="exec-icon">{hl_icon}</span>
  <span class="exec-label">Executive Status</span>
  <span class="exec-text">{headline}</span>
</div>

<!-- Body -->
<main class="body">

  <!-- Scorecard -->
  <div class="scorecard-eyebrow">Operational Scorecard &mdash; {len(factlist)} KPIs &middot; {inp.report_week}</div>
  <div class="scorecard-strip">
    <div class="sc-cell on-target">
      <span class="sc-num">{n_green}</span>
      <span class="sc-lbl">On Target</span>
    </div>
    <div class="sc-cell watch">
      <span class="sc-num">{n_yellow}</span>
      <span class="sc-lbl">Watch List</span>
    </div>
    <div class="sc-cell action">
      <span class="sc-num">{n_red}</span>
      <span class="sc-lbl">Needs Action</span>
    </div>
    <div class="sc-cell info">
      <span class="sc-num">{n_info}</span>
      <span class="sc-lbl">Informational</span>
    </div>
  </div>

  {"<div class='no-insights-banner'>Narrative analysis unavailable this run — KPI data above is authoritative.</div>" if not insights else ""}

  {domain_html}

</main>

<!-- Audit footer -->
<footer class="audit-footer" id="verification_footer">
  <div>
    <span class="af-head">Verification &amp; Data Quality</span>
    <div class="af-stats">
      <div class="af-stat">
        <span class="af-val">{footer_match}</span>
        <span class="af-lbl">Shipment Match</span>
      </div>
      <div class="af-stat">
        <span class="af-val">{acceptance:.0%}</span>
        <span class="af-lbl">Claim Acceptance</span>
      </div>
      <div class="af-stat">
        <span class="af-val">{agreement:.0%}</span>
        <span class="af-lbl">Verifier Agreement</span>
      </div>
    </div>
    <div class="af-line">
      <strong>Models:</strong> Llama&nbsp;3.3&nbsp;70B (Stages&nbsp;1&ndash;3&nbsp;&amp;&nbsp;6)
      &middot; DeepSeek&nbsp;V3 (Generation) &middot; Qwen2.5&nbsp;7B (Verification)
    </div>
    <div class="af-s6note">{s6_note}</div>
    <div class="af-stamp">Produced by Chorus AI Systems &mdash; multi-model verified pipeline. Not financial advice.</div>
  </div>
  <div>
    <span class="af-head">Degradation Disclosures</span>
    {disc_items}
  </div>
</footer>

</body>
</html>"""

    return html, sections_rendered


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════

def render_dashboard_html(inp: Stage5Input) -> str:
    """
    Generate a self-contained dark-theme static HTML dashboard.
    All data is baked in — suitable for GitHub Pages hosting.
    """
    factlist = inp.stage3_output.factlist
    insights = inp.stage4_output.verified_insights

    facts_by_domain: dict[str, list] = {}
    for f in factlist:
        facts_by_domain.setdefault(f.domain.value, []).append(f)

    insights_by_domain: dict[str, list] = {}
    for ins in insights:
        insights_by_domain.setdefault(ins.domain.value, []).append(ins)

    s6_blocks: dict[str, object] = {}
    if inp.stage6_output:
        for blk in inp.stage6_output.domain_blocks:
            s6_blocks[blk.domain.value] = blk

    n_green  = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.green)
    n_yellow = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.yellow)
    n_red    = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.red)
    n_info   = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.informational)

    red_facts    = [f for f in factlist if f.threshold_status == ThresholdStatus.red]
    yellow_facts = [f for f in factlist if f.threshold_status == ThresholdStatus.yellow]

    if red_facts:
        hl_status  = "red"
        hl_icon    = "●"
        headline   = (
            f"{len(red_facts)} metric{'s' if len(red_facts) > 1 else ''} require "
            f"immediate attention"
        )
        hl_sub = f"{red_facts[0].kpi_name} is below threshold at {_fmt_val(red_facts[0])}."
    elif yellow_facts:
        hl_status  = "yellow"
        hl_icon    = "◐"
        headline   = f"{len(yellow_facts)} metric{'s' if len(yellow_facts) > 1 else ''} flagged for review"
        hl_sub = f"{yellow_facts[0].kpi_name} at {_fmt_val(yellow_facts[0])} is below target."
    else:
        hl_status  = "green"
        hl_icon    = "✓"
        headline   = "All metrics on target"
        hl_sub = "No escalation required this week."

    match_rate_val = next(
        (f.final_value for f in factlist if f.kpi_name == "Shipment Match Rate"), None
    )
    footer_match = f"{match_rate_val:.1%}" if match_rate_val else "N/A"
    acceptance   = inp.stage4_output.claim_acceptance_rate
    agreement    = inp.stage4_output.cross_verifier_agreement
    disclosures  = [s.failure_reason for s in inp.degradation_signals]
    generated    = datetime.now().strftime("%B %d, %Y  %H:%M UTC")

    # ── Build domain sections ──────────────────────────────────────────────────
    domain_sections_html = ""

    for domain in DOMAINS:
        d_facts    = facts_by_domain.get(domain, [])
        d_insights = insights_by_domain.get(domain, [])
        if not d_facts:
            continue

        observations = [i for i in d_insights if i.claim_type == ClaimType.observation]
        hypotheses   = [i for i in d_insights if i.claim_type == ClaimType.hypothesis]
        actions      = [i for i in d_insights if i.claim_type == ClaimType.recommended_action]

        # Domain status dot summary
        status_dots = ""
        for f in d_facts:
            s = f.threshold_status.value
            status_dots += f'<span class="sdot sdot-{s}" title="{f.kpi_name}: {_fmt_val(f)}"></span>'

        # KPI cards with full historical comparison
        cards_html = ""
        for f in d_facts:
            s = f.threshold_status.value
            num, unit = _fmt_val_parts(f)
            wow = _fmt_wow(f)
            hist = _hist_rows_html(f, "ph-row")
            unit_disp = f'<span class="kv-unit-d">{unit}</span>' if unit else ""
            hist_block = f'<div class="hist-block">{hist}</div>' if hist else ""

            cards_html += f"""
              <div class="kpi-card-d kpi-d-{s}">
                <div class="kcd-name">{f.kpi_name}</div>
                <div class="kcd-val-row">
                  <span class="kcd-num">{num}</span>{unit_disp}
                </div>
                <div class="kcd-wow">{wow}</div>
                <div class="kcd-chip kcd-chip-{s}">{s.replace('informational','INFO').upper()}</div>
                {hist_block}
              </div>"""

        # Analysis: Stage 6 if available, Stage 4 fallback
        s6_blk = s6_blocks.get(domain)
        analysis_html = ""

        if s6_blk:
            obs_items = ""
            for obs in (observations + hypotheses)[:4]:
                obs_items += f'<div class="obs-item"><span class="obs-dot"></span>{obs.claim_text}</div>'
            if not obs_items:
                obs_items = '<div class="obs-item obs-muted">KPI data above is authoritative this period.</div>'

            recs_html = ""
            for idx, rec in enumerate(s6_blk.recommendations, 1):
                cite_str = ""
                if rec.source_fact_ids:
                    cite_str = f'<span class="rec-cite">{" · ".join(rec.source_fact_ids)}</span>'
                recs_html += f"""
                <div class="s6-rec-d">
                  <div class="s6-rec-num">{idx}</div>
                  <div class="s6-rec-body">
                    {rec.text}
                    {cite_str}
                  </div>
                </div>"""

            citations_str = ""
            if s6_blk.citation_sources:
                for src in s6_blk.citation_sources[:3]:
                    citations_str += f'<div class="cite-src">{src}</div>'

            analysis_html = f"""
              <div class="analysis-panel">
                <div class="ap-col">
                  <div class="ap-label">Data Analysis</div>
                  <div class="obs-list">{obs_items}</div>
                </div>
                <div class="ap-col ap-s6">
                  <div class="ap-label ap-label-gold">Expert Commentary</div>
                  <div class="s6-commentary-d">{s6_blk.commentary}</div>
                  <div class="ap-label ap-label-gold" style="margin-top:16px">Recommendations</div>
                  <div class="s6-recs-d">{recs_html}</div>
                  {"<div class='ap-label' style='margin-top:12px;font-size:10px'>Sources</div><div class='cite-list'>" + citations_str + "</div>" if citations_str else ""}
                </div>
              </div>"""
        else:
            all_items = (observations + hypotheses)[:4] + actions[:2]
            obs_items = ""
            for item in all_items:
                if item.claim_type == ClaimType.recommended_action:
                    txt = item.recommended_action or item.claim_text
                    obs_items += f'<div class="obs-item obs-act"><span class="obs-act-pill">Action</span>{txt}</div>'
                else:
                    obs_items += f'<div class="obs-item"><span class="obs-dot"></span>{item.claim_text}</div>'
            if not obs_items:
                obs_items = '<div class="obs-item obs-muted">No verified insights generated for this domain.</div>'

            analysis_html = f"""
              <div class="analysis-panel analysis-panel-full">
                <div class="ap-col">
                  <div class="ap-label">Analysis &amp; Actions</div>
                  <div class="obs-list">{obs_items}</div>
                </div>
              </div>"""

        # Provenance block (subtle, at bottom)
        prov_html = ""
        for f in d_facts:
            p = f.data_provenance
            prov_html += f"""
              <div class="prov-row">
                <span class="prov-id">{f.fact_id}</span>
                <span class="prov-name">{f.kpi_name}</span>
                <span class="prov-formula">{p.formula_used}</span>
                <span class="prov-rows">{p.row_count:,} rows</span>
              </div>"""

        domain_sections_html += f"""
          <section class="domain-section-d" id="d-{domain}">
            <div class="dsd-header">
              <div class="dsd-name">{DOMAIN_LABELS[domain]}</div>
              <div class="dsd-dots">{status_dots}</div>
              <div class="dsd-rule"></div>
            </div>
            <div class="kpi-grid-d">{cards_html}</div>
            {analysis_html}
            <details class="prov-details">
              <summary class="prov-summary">Data Provenance</summary>
              <div class="prov-table">{prov_html}</div>
            </details>
          </section>"""

    # ── Telemetry / pipeline stats ─────────────────────────────────────────────
    telemetry = inp.stage3_output  # factlist stage
    stage_telem = []
    for t in inp.stage1_output.field_mapping_log.mappings[:0]:  # not available here
        pass

    disc_html = (
        "".join(f'<div class="disc-row">{d}</div>' for d in disclosures)
        if disclosures else '<div class="disc-row disc-none">None — full pipeline completed.</div>'
    )

    s6_stat = ""
    if inp.stage6_output:
        s6_stat = (
            f"{len(inp.stage6_output.domain_blocks)} domain(s) · "
            f"{inp.stage6_output.total_chunks_retrieved} KB chunks retrieved"
        )
    else:
        s6_stat = "Unavailable this run"

    # ── Nav items (domain quick-links) ────────────────────────────────────────
    nav_links = ""
    for d in DOMAINS:
        if facts_by_domain.get(d):
            nav_links += f'<a href="#d-{d}" class="nav-link">{DOMAIN_LABELS[d]}</a>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CAS·DAM — {inp.report_week}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="{GOOGLE_FONTS}" rel="stylesheet">
<style>
/* ── Reset ────────────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
a {{ color: inherit; text-decoration: none; }}

/* ── Dark design tokens ───────────────────────────────────── */
:root {{
  --bg:        #070d1a;
  --surf-1:    #0d1627;
  --surf-2:    #112035;
  --border:    #1a2b42;
  --border-br: #243d5c;

  --text-1:    #f0f4f8;
  --text-2:    #8da2b8;
  --text-3:    #4e6680;

  --gold:      #c9a84c;
  --gold-dim:  rgba(201,168,76,0.12);

  --green:     #10b981;  --green-dim:  rgba(16,185,129,0.10);
  --amber:     #f59e0b;  --amber-dim:  rgba(245,158,11,0.10);
  --red:       #ef4444;  --red-dim:    rgba(239,68,68,0.10);
  --info:      #64748b;  --info-dim:   rgba(100,116,139,0.08);

  --ff-serif: 'Playfair Display', Georgia, serif;
  --ff-sans:  'Inter', -apple-system, 'Segoe UI', sans-serif;
  --ff-mono:  'JetBrains Mono', 'SF Mono', Consolas, monospace;

  --max-w: 1200px;
  --pad:   40px;
}}

/* ── Base ─────────────────────────────────────────────────── */
html {{ scroll-behavior: smooth; }}
body {{
  font-family: var(--ff-sans);
  background: var(--bg);
  color: var(--text-1);
  line-height: 1.55;
  font-size: 14px;
  -webkit-font-smoothing: antialiased;
}}

/* ── Navbar ───────────────────────────────────────────────── */
.navbar {{
  position: sticky;
  top: 0;
  z-index: 100;
  background: var(--surf-1);
  border-bottom: 1px solid var(--border);
  height: 56px;
  display: flex;
  align-items: center;
  padding: 0 var(--pad);
  gap: 32px;
}}
.nb-brand {{
  font-family: var(--ff-mono);
  font-size: 15px;
  font-weight: 600;
  color: var(--gold);
  letter-spacing: 2px;
  white-space: nowrap;
}}
.nb-links {{
  display: flex;
  gap: 0;
  overflow: hidden;
}}
.nav-link {{
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-3);
  padding: 0 16px;
  height: 56px;
  display: flex;
  align-items: center;
  border-right: 1px solid var(--border);
  transition: color 0.15s, background 0.15s;
}}
.nav-link:first-child {{ border-left: 1px solid var(--border); }}
.nav-link:hover {{ color: var(--text-1); background: var(--surf-2); }}
.nb-meta {{
  margin-left: auto;
  font-family: var(--ff-mono);
  font-size: 11px;
  color: var(--text-3);
  white-space: nowrap;
}}

/* ── Hero ─────────────────────────────────────────────────── */
.hero {{
  background: var(--surf-1);
  border-bottom: 1px solid var(--border);
  padding: 40px var(--pad) 36px;
}}
.hero-inner {{
  max-width: var(--max-w);
  margin: 0 auto;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 40px;
}}
.hero-left {{ display: flex; flex-direction: column; gap: 10px; }}
.hero-eyebrow {{
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 2.5px;
  color: var(--gold);
}}
.hero-title {{
  font-family: var(--ff-serif);
  font-size: 36px;
  font-weight: 700;
  color: var(--text-1);
  letter-spacing: -0.3px;
  line-height: 1.1;
}}
.hero-sub {{
  font-size: 16px;
  font-weight: 300;
  color: var(--text-2);
  line-height: 1.5;
}}
.hero-right {{ text-align: right; flex-shrink: 0; }}
.hero-week-label {{
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-3);
  margin-bottom: 2px;
}}
.hero-week {{
  font-family: var(--ff-sans);
  font-size: 18px;
  font-weight: 700;
  color: var(--text-1);
}}
.hero-runid {{
  font-family: var(--ff-mono);
  font-size: 11px;
  color: var(--text-3);
  margin-top: 6px;
}}
.hero-generated {{
  font-size: 11px;
  color: var(--text-3);
  margin-top: 2px;
}}

/* ── Status banner ────────────────────────────────────────── */
.status-banner {{
  max-width: var(--max-w);
  margin: 24px auto 0;
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 16px 20px;
  border: 1px solid var(--border);
  border-left: 4px solid transparent;
}}
.status-banner.red    {{ border-left-color: var(--red);   background: var(--red-dim);   }}
.status-banner.yellow {{ border-left-color: var(--amber); background: var(--amber-dim); }}
.status-banner.green  {{ border-left-color: var(--green); background: var(--green-dim); }}
.sb-icon {{
  font-size: 20px;
  line-height: 1;
  flex-shrink: 0;
}}
.status-banner.red    .sb-icon {{ color: var(--red);   }}
.status-banner.yellow .sb-icon {{ color: var(--amber); }}
.status-banner.green  .sb-icon {{ color: var(--green); }}
.sb-text {{ display: flex; flex-direction: column; gap: 2px; }}
.sb-headline {{
  font-size: 17px;
  font-weight: 600;
  color: var(--text-1);
}}
.sb-sub {{
  font-size: 13px;
  color: var(--text-2);
}}

/* ── Page wrapper ─────────────────────────────────────────── */
.page-wrap {{
  max-width: var(--max-w);
  margin: 0 auto;
  padding: 40px var(--pad) 80px;
}}

/* ── Scorecard ────────────────────────────────────────────── */
.sc-eyebrow {{
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-3);
  margin-bottom: 12px;
}}
.sc-strip {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1px;
  background: var(--border);
  border: 1px solid var(--border);
  margin-bottom: 52px;
}}
.sc-cell {{
  background: var(--surf-1);
  padding: 24px 16px 20px;
  text-align: center;
  border-top: 3px solid transparent;
  position: relative;
}}
.sc-cell::after {{
  content: '';
  display: block;
  position: absolute;
  inset: 0;
  pointer-events: none;
}}
.sc-cell.on-target  {{ border-top-color: var(--green); }}
.sc-cell.watch      {{ border-top-color: var(--amber); }}
.sc-cell.action     {{ border-top-color: var(--red);   }}
.sc-cell.info       {{ border-top-color: var(--info);  }}
.sc-num-d {{
  display: block;
  font-family: var(--ff-sans);
  font-size: 56px;
  font-weight: 800;
  line-height: 1;
  margin-bottom: 6px;
}}
.sc-cell.on-target .sc-num-d {{ color: var(--green); }}
.sc-cell.watch     .sc-num-d {{ color: var(--amber); }}
.sc-cell.action    .sc-num-d {{ color: var(--red);   }}
.sc-cell.info      .sc-num-d {{ color: var(--info);  }}
.sc-lbl-d {{
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: var(--text-3);
}}

/* ── Domain section ───────────────────────────────────────── */
.domain-section-d {{
  margin-bottom: 60px;
  scroll-margin-top: 72px;
}}
.dsd-header {{
  display: flex;
  align-items: center;
  gap: 20px;
  padding: 0 0 16px;
  border-top: 1px solid var(--border);
  padding-top: 32px;
  margin-bottom: 20px;
}}
.dsd-name {{
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 2.5px;
  color: var(--text-1);
  white-space: nowrap;
}}
.dsd-dots {{ display: flex; gap: 6px; align-items: center; }}
.sdot {{
  width: 8px;
  height: 8px;
  border-radius: 50%;
  cursor: default;
}}
.sdot-green  {{ background: var(--green); }}
.sdot-yellow {{ background: var(--amber); }}
.sdot-red    {{ background: var(--red);   }}
.sdot-informational {{ background: var(--info); }}
.dsd-rule {{ flex: 1; height: 1px; background: var(--border); }}

/* ── KPI grid (dashboard) ─────────────────────────────────── */
.kpi-grid-d {{
  display: flex;
  gap: 1px;
  background: var(--border);
  border: 1px solid var(--border);
  margin-bottom: 1px;
}}
.kpi-card-d {{
  flex: 1;
  min-width: 0;
  background: var(--surf-1);
  padding: 20px;
  border-top: 3px solid transparent;
}}
.kpi-d-green  {{ border-top-color: var(--green); }}
.kpi-d-yellow {{ border-top-color: var(--amber); }}
.kpi-d-red    {{ border-top-color: var(--red);   }}
.kpi-d-informational {{ border-top-color: var(--info); }}
.kcd-name {{
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--text-3);
  margin-bottom: 12px;
}}
.kcd-val-row {{
  display: flex;
  align-items: baseline;
  gap: 3px;
  margin-bottom: 6px;
}}
.kcd-num {{
  font-family: var(--ff-mono);
  font-size: 38px;
  font-weight: 600;
  color: var(--text-1);
  letter-spacing: -1.5px;
  line-height: 1;
}}
.kv-unit-d {{
  font-family: var(--ff-mono);
  font-size: 18px;
  font-weight: 400;
  color: var(--text-3);
}}
.kcd-wow {{
  font-family: var(--ff-mono);
  font-size: 11px;
  color: var(--text-3);
  margin-bottom: 10px;
}}
.kcd-chip {{
  display: inline-block;
  font-size: 9px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1px;
  padding: 3px 8px;
  border-radius: 2px;
  margin-bottom: 16px;
}}
.kcd-chip-green  {{ color: var(--green); background: var(--green-dim); }}
.kcd-chip-yellow {{ color: var(--amber); background: var(--amber-dim); }}
.kcd-chip-red    {{ color: var(--red);   background: var(--red-dim);   }}
.kcd-chip-informational {{ color: var(--info); background: var(--info-dim); }}

/* ── Historical comparison ─────────────────────────────────── */
.hist-block {{
  border-top: 1px solid var(--border);
  padding-top: 12px;
}}
.ph-row {{
  display: grid;
  grid-template-columns: 1fr 70px 70px;
  gap: 4px;
  padding: 3px 0;
  border-bottom: 1px solid var(--border);
  align-items: center;
}}
.ph-row:last-child {{ border-bottom: none; }}
.ph-period {{
  font-size: 11px;
  color: var(--text-3);
  font-style: italic;
}}
.ph-val {{
  font-family: var(--ff-mono);
  font-size: 11px;
  color: var(--text-2);
  text-align: right;
}}
.ph-delta {{
  font-family: var(--ff-mono);
  font-size: 11px;
  font-weight: 600;
  text-align: right;
}}
.ph-delta.dp {{ color: var(--green); }}
.ph-delta.dn {{ color: var(--red);   }}

/* ── Analysis panel ───────────────────────────────────────── */
.analysis-panel {{
  display: grid;
  grid-template-columns: 1fr 1.4fr;
  gap: 1px;
  background: var(--border);
  border: 1px solid var(--border);
  border-top: none;
  margin-bottom: 1px;
}}
.analysis-panel-full {{
  grid-template-columns: 1fr;
}}
.ap-col {{
  background: var(--surf-1);
  padding: 20px 22px;
}}
.ap-s6 {{
  background: var(--surf-2);
  border-left: 3px solid var(--gold);
}}
.ap-label {{
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-3);
  margin-bottom: 14px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border);
}}
.ap-label-gold {{
  color: var(--gold);
  border-bottom-color: var(--gold-dim);
}}
.obs-list {{ display: flex; flex-direction: column; gap: 8px; }}
.obs-item {{
  font-size: 13px;
  line-height: 1.6;
  color: var(--text-2);
  display: flex;
  gap: 10px;
  align-items: flex-start;
}}
.obs-dot {{
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--border-br);
  flex-shrink: 0;
  margin-top: 7px;
}}
.obs-act {{ align-items: flex-start; }}
.obs-act-pill {{
  font-size: 9px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  background: var(--gold-dim);
  color: var(--gold);
  padding: 2px 7px;
  border-radius: 2px;
  flex-shrink: 0;
  margin-top: 3px;
}}
.obs-muted {{ color: var(--text-3); font-style: italic; }}

/* ── Stage 6 ──────────────────────────────────────────────── */
.s6-commentary-d {{
  font-size: 13px;
  line-height: 1.7;
  color: var(--text-2);
  margin-bottom: 8px;
}}
.s6-recs-d {{ display: flex; flex-direction: column; gap: 8px; }}
.s6-rec-d {{
  display: flex;
  gap: 12px;
  align-items: flex-start;
  padding: 10px 12px;
  background: var(--surf-1);
  border: 1px solid var(--border);
  border-left: 3px solid var(--gold);
}}
.s6-rec-num {{
  font-family: var(--ff-mono);
  font-size: 13px;
  font-weight: 600;
  color: var(--gold);
  flex-shrink: 0;
  min-width: 16px;
}}
.s6-rec-body {{
  font-size: 13px;
  line-height: 1.6;
  color: var(--text-2);
}}
.rec-cite {{
  display: inline-block;
  font-family: var(--ff-mono);
  font-size: 10px;
  color: var(--text-3);
  margin-top: 4px;
}}
.cite-list {{ display: flex; flex-direction: column; gap: 2px; }}
.cite-src {{
  font-family: var(--ff-mono);
  font-size: 10px;
  color: var(--text-3);
}}

/* ── Provenance ───────────────────────────────────────────── */
.prov-details {{
  border: 1px solid var(--border);
  border-top: none;
  background: var(--bg);
}}
.prov-summary {{
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-3);
  padding: 10px 16px;
  cursor: pointer;
  user-select: none;
  list-style: none;
}}
.prov-summary::-webkit-details-marker {{ display: none; }}
.prov-summary::before {{ content: "▸ "; }}
details[open] .prov-summary::before {{ content: "▾ "; }}
.prov-table {{ padding: 0 16px 12px; }}
.prov-row {{
  display: grid;
  grid-template-columns: 50px 160px 1fr 80px;
  gap: 12px;
  padding: 6px 0;
  border-bottom: 1px solid var(--border);
  align-items: baseline;
}}
.prov-row:last-child {{ border-bottom: none; }}
.prov-id {{
  font-family: var(--ff-mono);
  font-size: 10px;
  color: var(--gold);
}}
.prov-name {{
  font-size: 11px;
  font-weight: 600;
  color: var(--text-2);
}}
.prov-formula {{
  font-size: 10px;
  color: var(--text-3);
  line-height: 1.4;
}}
.prov-rows {{
  font-family: var(--ff-mono);
  font-size: 10px;
  color: var(--text-3);
  text-align: right;
}}

/* ── System footer ────────────────────────────────────────── */
.sys-footer {{
  background: var(--surf-1);
  border-top: 1px solid var(--border);
  padding: 40px var(--pad);
}}
.sf-inner {{
  max-width: var(--max-w);
  margin: 0 auto;
  display: grid;
  grid-template-columns: 1.5fr 1fr 1fr;
  gap: 40px;
}}
.sf-head {{
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-3);
  margin-bottom: 16px;
}}
.sf-stats {{ display: flex; gap: 24px; margin-bottom: 16px; }}
.sf-stat {{ display: flex; flex-direction: column; gap: 2px; }}
.sf-val {{
  font-family: var(--ff-mono);
  font-size: 22px;
  font-weight: 600;
  color: var(--text-1);
  line-height: 1;
}}
.sf-lbl {{
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: var(--text-3);
}}
.sf-line {{
  font-size: 12px;
  line-height: 1.9;
  color: var(--text-3);
}}
.sf-line strong {{ color: var(--text-2); }}
.sf-stamp {{
  font-size: 11px;
  color: var(--text-3);
  margin-top: 12px;
  font-style: italic;
  line-height: 1.7;
}}
.disc-row {{
  font-size: 12px;
  line-height: 1.9;
  color: var(--text-3);
  padding-left: 12px;
  position: relative;
}}
.disc-row::before {{ content: "·"; position: absolute; left: 0; }}
.disc-none {{ font-style: italic; padding-left: 0; }}
.disc-none::before {{ content: ""; }}
.sf-gold-bar {{
  max-width: var(--max-w);
  margin: 0 auto 24px;
  height: 2px;
  background: linear-gradient(90deg, var(--gold) 0%, transparent 100%);
}}
</style>
</head>
<body>

<!-- Navbar -->
<nav class="navbar">
  <div class="nb-brand">CAS·DAM</div>
  <div class="nb-links">
    {nav_links}
  </div>
  <div class="nb-meta">{inp.report_week}</div>
</nav>

<!-- Hero -->
<div class="hero">
  <div class="hero-inner">
    <div class="hero-left">
      <div class="hero-eyebrow">Chorus AI Systems &nbsp;·&nbsp; Operational Intelligence</div>
      <div class="hero-title">Data Analytics Manager</div>
      <div class="hero-sub">Weekly Supply Chain Intelligence Report</div>
    </div>
    <div class="hero-right">
      <div class="hero-week-label">Week ending</div>
      <div class="hero-week">{inp.report_week}</div>
      <div class="hero-runid">{inp.run_id}</div>
      <div class="hero-generated">{generated}</div>
    </div>
  </div>
  <div class="status-banner {hl_status}">
    <span class="sb-icon">{hl_icon}</span>
    <div class="sb-text">
      <div class="sb-headline">{headline}</div>
      <div class="sb-sub">{hl_sub}</div>
    </div>
  </div>
</div>

<!-- Page content -->
<div class="page-wrap">

  <!-- Scorecard -->
  <div class="sc-eyebrow">Operational Scorecard &mdash; {len(factlist)} KPIs this period</div>
  <div class="sc-strip">
    <div class="sc-cell on-target">
      <span class="sc-num-d">{n_green}</span>
      <span class="sc-lbl-d">On Target</span>
    </div>
    <div class="sc-cell watch">
      <span class="sc-num-d">{n_yellow}</span>
      <span class="sc-lbl-d">Watch List</span>
    </div>
    <div class="sc-cell action">
      <span class="sc-num-d">{n_red}</span>
      <span class="sc-lbl-d">Needs Action</span>
    </div>
    <div class="sc-cell info">
      <span class="sc-num-d">{n_info}</span>
      <span class="sc-lbl-d">Informational</span>
    </div>
  </div>

  {domain_sections_html}

</div>

<!-- System footer -->
<footer class="sys-footer">
  <div class="sf-gold-bar"></div>
  <div class="sf-inner">
    <div>
      <div class="sf-head">Verification &amp; Data Quality</div>
      <div class="sf-stats">
        <div class="sf-stat">
          <span class="sf-val">{footer_match}</span>
          <span class="sf-lbl">Shipment Match</span>
        </div>
        <div class="sf-stat">
          <span class="sf-val">{acceptance:.0%}</span>
          <span class="sf-lbl">Claim Acceptance</span>
        </div>
        <div class="sf-stat">
          <span class="sf-val">{agreement:.0%}</span>
          <span class="sf-lbl">Verifier Agreement</span>
        </div>
      </div>
      <div class="sf-line">
        <strong>Stage 6:</strong> {s6_stat}
      </div>
      <div class="sf-stamp">
        Produced by Chorus AI Systems — CAS·DAM multi-model verified pipeline.<br>
        Llama 3.3 70B · DeepSeek V3 · Qwen2.5 7B · Python cross-verification.<br>
        Not financial advice.
      </div>
    </div>
    <div>
      <div class="sf-head">Models &amp; Pipeline</div>
      <div class="sf-line">
        <strong>Stages 1–3, 6</strong><br>Llama 3.3 70B Instruct Turbo<br>
        <strong>Stage 4 Generation</strong><br>DeepSeek V3<br>
        <strong>Stage 4 Verification</strong><br>Qwen2.5 7B<br>
        <strong>Stage 5</strong><br>Deterministic (no LLM)
      </div>
    </div>
    <div>
      <div class="sf-head">Degradation Disclosures</div>
      {disc_html}
    </div>
  </div>
</footer>

</body>
</html>"""

    return html
