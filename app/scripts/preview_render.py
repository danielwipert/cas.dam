"""
preview_render.py
Renders preview HTML files from the most recent saved report data,
with mock Stage 6 content for design verification.
Run from app/ directory: python scripts/preview_render.py
"""
import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.schemas import (
    Stage5Input, Stage1Output, Stage2Output, Stage3Output, Stage4Output,
    FactListEntry, InsightClaim, FieldMappingLog,
    Stage6Output, Stage6DomainBlock, Stage6Recommendation,
    KPIDomain,
)
from core.report_renderer import render_pdf_html, render_dashboard_html

REPORT = "output/report_data/DAM-20260420-220842-56289b.json"

with open(REPORT) as f:
    data = json.load(f)

factlist = [FactListEntry(**f) for f in data["factlist"]]
insights = [InsightClaim(**i) for i in data["insights"]]

s6_out = Stage6Output(
    domain_blocks=[
        Stage6DomainBlock(
            domain=KPIDomain.fulfillment,
            commentary=(
                "Fulfillment velocity has deteriorated meaningfully this week. "
                "Order-to-ship time of 46.7 hours is tracking above the 30-day average and the "
                "on-time ship rate drop to 90.5% — a 2.7 percentage point decline — signals a "
                "systemic pick-pack constraint, not a one-off incident. The unshipped orders rate "
                "at 3.2% compounds the exposure: these are revenue at risk if the pattern extends "
                "into next week."
            ),
            recommendations=[
                Stage6Recommendation(
                    text=(
                        "Conduct same-day triage on the 3.2% unshipped backlog — prioritise orders "
                        "by promised delivery date and escalate any older than 48 hours to warehouse management."
                    ),
                    source_chunk_ids=["chunk_001", "chunk_002"],
                    source_fact_ids=["F001", "F002", "F003"],
                ),
                Stage6Recommendation(
                    text=(
                        "Audit the pick-pack scheduling for the current week's shift pattern; the "
                        "on-time ship rate decline correlates historically with understaffed mid-week windows."
                    ),
                    source_chunk_ids=["chunk_003"],
                    source_fact_ids=["F002"],
                ),
            ],
            chunk_citations=["chunk_001", "chunk_002", "chunk_003"],
            citation_sources=["Warehouse Operations Playbook (2025 Ed.)", "Q3 Fulfillment Post-Mortem"],
        ),
        Stage6DomainBlock(
            domain=KPIDomain.carrier_performance,
            commentary=(
                "Carrier performance is the most acute concern in this report. The on-time delivery "
                "rate fell 6.6 percentage points week-over-week to 91.4% — the largest single-week "
                "drop in the trailing 90-day period. Transit time of 152.2 hours remains within green "
                "thresholds but its 12.5-hour increase is a leading indicator. Given the FedEx/DHL "
                "split is near parity at 53/47, the degradation appears systemic rather than "
                "carrier-specific."
            ),
            recommendations=[
                Stage6Recommendation(
                    text=(
                        "Pull carrier-level transit data for the current week to isolate whether the "
                        "on-time delivery shortfall is concentrated in FedEx Ground or DHL Ecommerce "
                        "before escalating to carrier account managers."
                    ),
                    source_chunk_ids=["chunk_010", "chunk_011"],
                    source_fact_ids=["F004", "F005", "F006"],
                ),
                Stage6Recommendation(
                    text=(
                        "If the degradation persists into next week, trigger the carrier escalation "
                        "protocol — request a root-cause explanation from both carriers within 48 hours "
                        "per the SLA agreement."
                    ),
                    source_chunk_ids=["chunk_012"],
                    source_fact_ids=["F005"],
                ),
            ],
            chunk_citations=["chunk_010", "chunk_011", "chunk_012"],
            citation_sources=["Carrier SLA Framework v2.1", "Network Performance Benchmarks H1 2026"],
        ),
        Stage6DomainBlock(
            domain=KPIDomain.cost,
            commentary=(
                "Cost metrics remain informational given limited history, but the trajectory is worth "
                "noting. FedEx average cost per shipment at $14.96 is $4.34 above DHL at $10.62 — a "
                "premium that will become material as volume grows. With the current 53% FedEx share, "
                "the blended cost of $12.11 per order is drifting upward. The mix shift toward DHL "
                "where service quality allows would improve the cost profile without threshold risk."
            ),
            recommendations=[
                Stage6Recommendation(
                    text=(
                        "Model the cost impact of shifting 5-10 percentage points of eligible Ground "
                        "volume from FedEx to DHL Ecommerce; use the current $4.34 per-shipment delta "
                        "as the basis for the business case."
                    ),
                    source_chunk_ids=["chunk_020"],
                    source_fact_ids=["F007", "F008"],
                ),
                Stage6Recommendation(
                    text=(
                        "Establish cost thresholds for F007 and F008 at the 4-week mark when "
                        "informational status lifts; propose targets based on the current month "
                        "average as the baseline."
                    ),
                    source_chunk_ids=["chunk_021"],
                    source_fact_ids=["F007"],
                ),
            ],
            chunk_citations=["chunk_020", "chunk_021"],
            citation_sources=["Carrier Rate Card Archive (FedEx/DHL)", "Cost Optimisation Playbook"],
        ),
        Stage6DomainBlock(
            domain=KPIDomain.operational_integrity,
            commentary=(
                "Operational integrity is a split picture. Label lag at 3.6 hours is within green "
                "thresholds but has increased 0.8 hours week-over-week — a trend worth watching "
                "given the WMS-to-carrier API integration introduced in Q3. The shipment match rate "
                "at 93.8% remains in red; while it improved 1.8 points from last week, the 6.2% "
                "unmatched volume reduces confidence in cost and transit data downstream."
            ),
            recommendations=[
                Stage6Recommendation(
                    text=(
                        "Investigate the 6.2% unmatched shipments: pull the specific tracking numbers "
                        "and determine whether the gap is a carrier data lag or a structural mapping "
                        "failure in the ETL pipeline."
                    ),
                    source_chunk_ids=["chunk_030", "chunk_031"],
                    source_fact_ids=["F009", "F010"],
                ),
                Stage6Recommendation(
                    text=(
                        "Set a target of 96% match rate within 4 weeks; assign ownership to the data "
                        "engineering team and schedule a weekly review until the red threshold is cleared."
                    ),
                    source_chunk_ids=["chunk_032"],
                    source_fact_ids=["F010"],
                ),
            ],
            chunk_citations=["chunk_030", "chunk_031", "chunk_032"],
            citation_sources=["Data Quality Standards v1.3", "ETL Pipeline Documentation"],
        ),
    ],
    domains_skipped=[],
    total_chunks_retrieved=40,
)

s1 = Stage1Output(
    canonical_orders=[], canonical_shipments=[], canonical_carrier_shipments=[],
    field_mapping_log=FieldMappingLog(run_id=data["run_id"], mappings=[], ambiguous_field_count=0),
)
s2 = Stage2Output(reconciliation_shipments=[], exact_match_rate=0.938,
                  fuzzy_match_volume=0, unmatched_count=30)
s3 = Stage3Output(factlist=factlist, kpi_mismatch_count=0, python_verified=True)
s4 = Stage4Output(
    verified_insights=insights,
    claim_count_generated=len(insights),
    claim_acceptance_rate=data.get("claim_acceptance_rate") or 0.82,
    cross_verifier_agreement=data.get("cross_verifier_agreement") or 1.0,
    stripped_claim_log=[],
    domain_recommendations={},
)

inp = Stage5Input(
    stage1_output=s1, stage2_output=s2, stage3_output=s3,
    stage4_output=s4, stage6_output=s6_out,
    degradation_signals=[], run_id=data["run_id"], report_week=data["report_week"],
)

pdf_html, sections = render_pdf_html(inp)
dash_html = render_dashboard_html(inp)

os.makedirs("output/reports", exist_ok=True)
os.makedirs("output/site", exist_ok=True)

with open("output/reports/preview_pdf.html", "w", encoding="utf-8") as f:
    f.write(pdf_html)
with open("output/reports/preview_dash.html", "w", encoding="utf-8") as f:
    f.write(dash_html)
with open("output/site/index.html", "w", encoding="utf-8") as f:
    f.write(dash_html)

print(f"PDF preview:       output/reports/preview_pdf.html  ({len(pdf_html):,} chars)")
print(f"Dashboard preview: output/reports/preview_dash.html ({len(dash_html):,} chars)")
print(f"Site (GH Pages):   output/site/index.html")
print(f"Sections: {sections}")
