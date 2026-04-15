"""
Opportunities module — runs a senior-strategist audit on each account using Anthropic Claude.

Pre-computes daily for all accounts. Results cached in memory and served via /api/opportunities.
"""
import os
import json
import logging
import threading
from datetime import datetime, timedelta

import requests

from datetime import date, timedelta

from fetch_data import (
    get_token, gaql, MCCS, fetch_campaigns, compute_date_range,
)

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")

# Cache: { (account_id, mcc): {"generated_at": iso, "content": str, "error": str} }
_opps_cache: dict = {}
_opps_lock = threading.Lock()


DIAGNOSTIC_PROMPT = """You are a senior Google Ads strategist auditing this Google Ads account.

Silently evaluate the account against every diagnostic question listed below. Do NOT answer every question — only surface findings where you spot something actionable: a problem, a risk, a wasted spend issue, or a scaling opportunity.

DIAGNOSTIC QUESTIONS TO EVALUATE:

OVERALL ACCOUNT HEALTH
1. Are total conversions, revenue, and ROAS trending up or down WoW and MoM?
2. Has CPA shifted meaningfully in any direction?
3. Is total spend pacing on budget or under/over-delivering?
4. Are any campaigns limited by budget right now?
5. Has impression share changed significantly — is the loss from budget or rank?
6. Are there any disapproved ads, policies, or account-level warnings?

CAMPAIGN-LEVEL PERFORMANCE
7. Which campaigns improved or declined the most in ROAS/revenue this week vs last?
8. Are any campaigns spending but generating zero or near-zero conversions?
9. Are branded and non-branded campaigns tracked and evaluated separately?
10. Is branded ROAS inflating the overall account picture?
11. What share of total revenue is coming from brand vs non-brand?
12. Are any campaigns cannibalising each other — same products, same audiences, overlapping search terms?

SHOPPING / PMAX / PRODUCT-LEVEL
13. Which products or product groups are driving the most revenue?
14. Are there products getting lots of clicks but no conversions?
15. Are there products with strong conversion rates but very low impression share?
16. Are any products disapproved or not serving due to feed issues?
17. Are there products with zero spend that should be getting traffic?
18. Is the product grouping structure granular enough?

SEARCH TERMS & KEYWORDS
19. Are there search terms wasting spend that should be negated?
20. Are there converting search terms not covered by existing keywords?
21. Has match type drift caused keywords to trigger on irrelevant queries?
22. Are there keyword conflicts between campaigns?
23. Are there keywords with strong CTR but poor conversion rate, or vice versa?

BIDDING & BUDGET
24. Is the bid strategy appropriate for the campaign goal?
25. Are bid strategy targets realistic given recent performance?
26. Are there campaigns with budget that could be reallocated from underperformers to scaling opportunities?

CREATIVE & ADS
27. Are ad strength ratings Good or Excellent — or are there Poor ones?
28. Are there enough headline and description variants for RSAs to optimise?
29. Are sitelinks, callouts, structured snippets in use?

COMPETITIVE & MARKET CONTEXT
30. Has auction insight share changed — are competitors gaining or losing ground?
31. Has AOV changed — and if so, is it a product mix shift?

STRATEGIC & STRUCTURAL
32. Is the campaign structure aligned with business goals — or bloated with legacy campaigns?
33. Are there campaigns that should be consolidated or split?
34. Are there opportunities to launch campaign types the account is not using yet?

OUTPUT FORMAT:

Return a concise bullet-point summary using these categories. Only include a category if there is something to report. Skip empty categories entirely.

🔴 Issues (things that need fixing now)
🟡 Risks (things trending in the wrong direction or worth watching)
💰 Wasted Spend (specific areas where budget is being burned inefficiently)
🟢 Opportunities (areas with room to scale or improve)
📝 Notable Changes (recent changes that may be driving current performance)

For each bullet point:
- State the finding clearly in one sentence
- Include the relevant metric or data point
- Suggest a specific next step where possible

Keep the entire output under 500 words. Be direct, skip fluff, and prioritise by impact. Use British English spelling (optimise, analyse, etc). Never use em dashes.

ACCOUNT DATA:

{data}
"""


def _fetch_account_context(account_id: str, mcc_key: str) -> dict:
    """Pull all the data Claude needs to audit this account."""
    token = get_token(mcc_key)
    login_id = MCCS[mcc_key]["login_customer_id"]
    ctx = {"account_id": account_id, "mcc": mcc_key}

    # Campaign breakdown last 7 days
    try:
        start, end = compute_date_range(days=7)
        camps, _ = fetch_campaigns(token, account_id, login_id, start, end)
        ctx["campaigns_7d"] = camps
        ctx["totals_7d"] = {
            "cost": round(sum(c["cost"] for c in camps), 2),
            "revenue": round(sum(c["revenue"] for c in camps), 2),
            "conversions": round(sum(c["conversions"] for c in camps), 1),
        }
        if ctx["totals_7d"]["cost"] > 0:
            ctx["totals_7d"]["roas"] = round(ctx["totals_7d"]["revenue"] / ctx["totals_7d"]["cost"], 2)
    except Exception as e:
        ctx["campaigns_7d_error"] = str(e)

    # Previous 7 days (for WoW)
    try:
        today = date.today()
        prev_end = today - timedelta(days=8)
        prev_start = today - timedelta(days=14)
        camps_prev, _ = fetch_campaigns(
            token, account_id, login_id,
            prev_start.strftime("%Y-%m-%d"), prev_end.strftime("%Y-%m-%d")
        )
        ctx["totals_prev_7d"] = {
            "cost": round(sum(c["cost"] for c in camps_prev), 2),
            "revenue": round(sum(c["revenue"] for c in camps_prev), 2),
            "conversions": round(sum(c["conversions"] for c in camps_prev), 1),
        }
        if ctx["totals_prev_7d"]["cost"] > 0:
            ctx["totals_prev_7d"]["roas"] = round(ctx["totals_prev_7d"]["revenue"] / ctx["totals_prev_7d"]["cost"], 2)
        ctx["campaigns_prev_7d"] = [{
            "name": c["name"], "cost": c["cost"], "revenue": c["revenue"],
            "conversions": c["conversions"], "roas": c["roas"],
        } for c in camps_prev]
    except Exception as e:
        ctx["campaigns_prev_7d_error"] = str(e)

    # 30-day totals for MoM context
    try:
        start, end = compute_date_range(days=30)
        camps30, _ = fetch_campaigns(token, account_id, login_id, start, end)
        ctx["totals_30d"] = {
            "cost": round(sum(c["cost"] for c in camps30), 2),
            "revenue": round(sum(c["revenue"] for c in camps30), 2),
            "conversions": round(sum(c["conversions"] for c in camps30), 1),
        }
        if ctx["totals_30d"]["cost"] > 0:
            ctx["totals_30d"]["roas"] = round(ctx["totals_30d"]["revenue"] / ctx["totals_30d"]["cost"], 2)
    except Exception as e:
        ctx["totals_30d_error"] = str(e)

    # Top search terms by cost (last 30d) — to spot waste
    try:
        rows = gaql(token, account_id, login_id, """
            SELECT search_term_view.search_term, campaign.name,
                   metrics.cost_micros, metrics.clicks, metrics.conversions,
                   metrics.conversions_value
            FROM search_term_view
            WHERE segments.date DURING LAST_30_DAYS
            ORDER BY metrics.cost_micros DESC
            LIMIT 100
        """)
        ctx["top_search_terms"] = [{
            "term": r["searchTermView"]["searchTerm"],
            "campaign": r["campaign"]["name"],
            "cost": int(r["metrics"].get("costMicros", 0)) / 1_000_000,
            "clicks": int(r["metrics"].get("clicks", 0)),
            "conv": float(r["metrics"].get("conversions", 0)),
            "rev": float(r["metrics"].get("conversionsValue", 0)),
        } for r in rows]
    except Exception as e:
        ctx["top_search_terms_error"] = str(e)

    # Zero-conversion high-spend search terms
    try:
        rows = gaql(token, account_id, login_id, """
            SELECT search_term_view.search_term, campaign.name,
                   metrics.cost_micros, metrics.clicks, metrics.conversions
            FROM search_term_view
            WHERE segments.date DURING LAST_30_DAYS
              AND metrics.conversions = 0
              AND metrics.cost_micros > 5000000
            ORDER BY metrics.cost_micros DESC
            LIMIT 30
        """)
        ctx["zero_conv_terms"] = [{
            "term": r["searchTermView"]["searchTerm"],
            "campaign": r["campaign"]["name"],
            "cost": int(r["metrics"].get("costMicros", 0)) / 1_000_000,
            "clicks": int(r["metrics"].get("clicks", 0)),
        } for r in rows]
    except Exception as e:
        ctx["zero_conv_terms_error"] = str(e)

    # Ad disapprovals / policy issues
    try:
        rows = gaql(token, account_id, login_id, """
            SELECT ad_group_ad.ad.id, ad_group_ad.policy_summary.approval_status,
                   ad_group_ad.policy_summary.review_status,
                   ad_group.name, campaign.name
            FROM ad_group_ad
            WHERE ad_group_ad.policy_summary.approval_status IN ('DISAPPROVED', 'APPROVED_LIMITED')
              AND ad_group_ad.status = 'ENABLED'
            LIMIT 30
        """)
        ctx["disapprovals"] = [{
            "campaign": r["campaign"]["name"],
            "ad_group": r["adGroup"]["name"],
            "status": r["adGroupAd"]["policySummary"].get("approvalStatus"),
        } for r in rows]
    except Exception as e:
        ctx["disapprovals_error"] = str(e)

    # Change history last 7 days
    try:
        rows = gaql(token, account_id, login_id, """
            SELECT change_event.change_date_time, change_event.change_resource_type,
                   change_event.resource_change_operation, change_event.user_email,
                   campaign.name
            FROM change_event
            WHERE change_event.change_date_time DURING LAST_7_DAYS
            ORDER BY change_event.change_date_time DESC
            LIMIT 30
        """)
        ctx["recent_changes"] = [{
            "when": r["changeEvent"]["changeDateTime"],
            "type": r["changeEvent"]["changeResourceType"],
            "op": r["changeEvent"]["resourceChangeOperation"],
            "campaign": r.get("campaign", {}).get("name"),
        } for r in rows]
    except Exception as e:
        ctx["recent_changes_error"] = str(e)

    return ctx


def _call_anthropic(prompt: str) -> str:
    """Call Claude via Anthropic API."""
    if not ANTHROPIC_API_KEY:
        return "ANTHROPIC_API_KEY not set in Render environment."
    logger.info("Calling Anthropic API — model=%s key_prefix=%s", ANTHROPIC_MODEL, ANTHROPIC_API_KEY[:20])

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": ANTHROPIC_MODEL,
            "max_tokens": 1500,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=120,
    )
    if not resp.ok:
        logger.error("Anthropic error %s: %s", resp.status_code, resp.text[:500])
        return f"API error {resp.status_code}: {resp.text[:200]}"
    return resp.json()["content"][0]["text"]


def generate_opportunities(account_id: str, mcc_key: str, account_name: str = "") -> dict:
    """Generate opportunities for one account. Returns {generated_at, content, error}."""
    try:
        ctx = _fetch_account_context(account_id, mcc_key)
        prompt = DIAGNOSTIC_PROMPT.format(
            data=json.dumps(ctx, indent=2, default=str)[:50000]
        )
        if account_name:
            prompt = f"Account name: {account_name}\n\n{prompt}"
        content = _call_anthropic(prompt)
        result = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "content": content,
            "account_id": account_id,
            "account_name": account_name,
        }
    except Exception as e:
        logger.exception("Opportunities failed for %s", account_id)
        result = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "error": str(e),
            "account_id": account_id,
            "account_name": account_name,
        }
    with _opps_lock:
        _opps_cache[(account_id, mcc_key)] = result
    return result


def get_cached(account_id: str, mcc_key: str) -> dict | None:
    with _opps_lock:
        return _opps_cache.get((account_id, mcc_key))


def regenerate_all(cached_data: dict) -> None:
    """Pre-compute opportunities for every account. Runs daily."""
    count = 0
    for mcc_key in ["happy", "upscale"]:
        accounts = cached_data.get(mcc_key, [])
        for acc in accounts:
            acc_id = str(acc.get("accountId", ""))
            if not acc_id:
                continue
            try:
                generate_opportunities(acc_id, mcc_key, acc.get("name", ""))
                count += 1
                logger.info("Opportunities generated for %s (%s)", acc.get("name"), acc_id)
            except Exception as e:
                logger.error("Opportunities failed for %s: %s", acc_id, e)
    logger.info("Opportunities: regenerated for %d accounts", count)
