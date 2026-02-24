"""Analytic operator templates for semantic SQL planning guidance."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OperatorTemplate:
    """Semantic operator hint used to prime SQL generation."""

    key: str
    description: str
    sql_shape_hint: str
    phrases: tuple[str, ...]
    signal_tokens: tuple[str, ...] = ()


@dataclass(frozen=True)
class OperatorMatch:
    """Scored operator-template match for a user query."""

    template: OperatorTemplate
    score: int


_OPERATOR_TEMPLATES: tuple[OperatorTemplate, ...] = (
    # Core aggregations and ranking
    OperatorTemplate(
        key="top_n_ranking",
        description="Rank entities by a metric and keep the top set.",
        sql_shape_hint="GROUP BY entity, ORDER BY metric DESC, LIMIT N",
        phrases=("top", "highest", "best", "leading"),
        signal_tokens=("rank", "score", "amount", "revenue", "count"),
    ),
    OperatorTemplate(
        key="bottom_n_ranking",
        description="Rank entities by a metric and keep the lowest set.",
        sql_shape_hint="GROUP BY entity, ORDER BY metric ASC, LIMIT N",
        phrases=("bottom", "lowest", "worst", "least"),
        signal_tokens=("rank", "score", "amount", "revenue", "count"),
    ),
    OperatorTemplate(
        key="share_of_total",
        description="Compute contribution percentage per group.",
        sql_shape_hint="metric / SUM(metric) OVER () AS share_pct",
        phrases=("share of", "percentage of total", "contribution", "mix"),
        signal_tokens=("percent", "ratio", "contribution"),
    ),
    OperatorTemplate(
        key="pareto_80_20",
        description="Identify contributors that make up most of a total.",
        sql_shape_hint="cumulative share window over descending metric",
        phrases=("pareto", "80/20", "vital few", "cumulative contribution"),
        signal_tokens=("cumulative", "share", "contribution"),
    ),
    OperatorTemplate(
        key="growth_rate",
        description="Measure period-over-period growth percentage.",
        sql_shape_hint="(current - previous) / NULLIF(previous,0)",
        phrases=("growth", "increase", "decrease", "lift"),
        signal_tokens=("current", "previous", "period", "delta"),
    ),
    OperatorTemplate(
        key="mom_growth",
        description="Month-over-month trend comparison.",
        sql_shape_hint="DATE_TRUNC('month') + LAG(metric)",
        phrases=("month over month", "mom", "monthly growth"),
        signal_tokens=("month", "lag", "trend"),
    ),
    OperatorTemplate(
        key="wow_growth",
        description="Week-over-week trend comparison.",
        sql_shape_hint="DATE_TRUNC('week') + LAG(metric)",
        phrases=("week over week", "wow", "weekly growth"),
        signal_tokens=("week", "lag", "trend"),
    ),
    OperatorTemplate(
        key="yoy_growth",
        description="Year-over-year period comparison.",
        sql_shape_hint="join or lag by same period previous year",
        phrases=("year over year", "yoy", "vs last year"),
        signal_tokens=("year", "comparison", "period"),
    ),
    OperatorTemplate(
        key="moving_average",
        description="Smooth metric fluctuations with rolling window.",
        sql_shape_hint="AVG(metric) OVER (ORDER BY date ROWS BETWEEN N PRECEDING AND CURRENT ROW)",
        phrases=("moving average", "rolling average", "rolling mean"),
        signal_tokens=("window", "date", "trend"),
    ),
    OperatorTemplate(
        key="running_total",
        description="Cumulative total across ordered periods.",
        sql_shape_hint="SUM(metric) OVER (ORDER BY date)",
        phrases=("running total", "cumulative", "accumulated"),
        signal_tokens=("cumulative", "ordered", "date"),
    ),
    OperatorTemplate(
        key="variance_to_target",
        description="Compare actual metric versus target benchmark.",
        sql_shape_hint="actual - target and actual/target deltas",
        phrases=("vs target", "against target", "plan vs actual", "variance"),
        signal_tokens=("target", "actual", "goal", "benchmark"),
    ),
    OperatorTemplate(
        key="forecast_projection",
        description="Project a future metric from recent trend/baseline.",
        sql_shape_hint="baseline aggregate + scenario multiplier",
        phrases=("project", "forecast", "what if", "scenario"),
        signal_tokens=("baseline", "assume", "projection", "uplift"),
    ),
    # Funnel and lifecycle
    OperatorTemplate(
        key="conversion_rate",
        description="Compute ratio between start and completed actions.",
        sql_shape_hint="completed_count / NULLIF(start_count,0)",
        phrases=("conversion", "converted", "success rate", "completion rate"),
        signal_tokens=("stage", "completed", "started", "rate"),
    ),
    OperatorTemplate(
        key="funnel_dropoff",
        description="Measure drop-off between funnel stages.",
        sql_shape_hint="stage_n_count vs previous stage count",
        phrases=("funnel", "drop-off", "dropoff", "leakage"),
        signal_tokens=("stage", "step", "journey", "pipeline"),
    ),
    OperatorTemplate(
        key="retention_cohort",
        description="Track retained users/accounts by cohort start date.",
        sql_shape_hint="cohort month x active month matrix",
        phrases=("cohort retention", "retention curve", "cohort analysis"),
        signal_tokens=("cohort", "retained", "period", "active"),
    ),
    OperatorTemplate(
        key="churn_rate",
        description="Measure attrition over a period.",
        sql_shape_hint="churned / opening population",
        phrases=("churn", "attrition", "dropout"),
        signal_tokens=("closed", "inactive", "lost", "cancelled"),
    ),
    OperatorTemplate(
        key="reactivation_rate",
        description="Share of dormant entities that became active again.",
        sql_shape_hint="reactivated / dormant base",
        phrases=("reactivation", "win back", "returned users"),
        signal_tokens=("inactive", "active again", "return"),
    ),
    OperatorTemplate(
        key="ltv_cac",
        description="Compare lifetime value to acquisition cost.",
        sql_shape_hint="ltv / NULLIF(cac,0)",
        phrases=("ltv cac", "lifetime value", "customer acquisition cost"),
        signal_tokens=("ltv", "cac", "marketing", "payback"),
    ),
    # Inventory and supply chain
    OperatorTemplate(
        key="stockout_risk",
        description="Estimate stockout risk using on-hand, reserved, and reorder levels.",
        sql_shape_hint="(on_hand - reserved) <= reorder_level with ranking",
        phrases=("stockout risk", "out of stock risk", "inventory risk", "reorder risk"),
        signal_tokens=("on_hand", "reserved", "reorder", "sku", "inventory"),
    ),
    OperatorTemplate(
        key="inventory_vs_sales_gap",
        description="Reconcile inventory movement against recorded sales.",
        sql_shape_hint="inventory delta (LEAD/LAG) minus sold quantity by store/product/time",
        phrases=(
            "inventory movement",
            "recorded sales",
            "inventory vs sales",
            "reconciliation gap",
            "inventory gap",
        ),
        signal_tokens=(
            "snapshot",
            "on_hand",
            "reserved",
            "sold",
            "quantity",
            "store_id",
            "product_id",
            "business_date",
        ),
    ),
    OperatorTemplate(
        key="inventory_turnover",
        description="Estimate turnover as COGS over average inventory.",
        sql_shape_hint="cogs / NULLIF(avg_inventory,0)",
        phrases=("inventory turnover", "turns", "stock turnover"),
        signal_tokens=("inventory", "average inventory", "cogs"),
    ),
    OperatorTemplate(
        key="days_of_supply",
        description="Compute remaining days before stock depletion.",
        sql_shape_hint="on_hand / avg_daily_demand",
        phrases=("days of supply", "days on hand", "cover days"),
        signal_tokens=("on_hand", "daily demand", "depletion"),
    ),
    OperatorTemplate(
        key="fill_rate",
        description="Measure fulfilled quantity against ordered/requested quantity.",
        sql_shape_hint="SUM(received_qty)/SUM(ordered_qty)",
        phrases=("fill rate", "service level", "order fill"),
        signal_tokens=("ordered", "received", "fulfilled", "po"),
    ),
    OperatorTemplate(
        key="lead_time",
        description="Measure elapsed time from order to receipt.",
        sql_shape_hint="DATE(received_at) - DATE(ordered_at)",
        phrases=("lead time", "delivery delay", "supplier delay"),
        signal_tokens=("ordered_at", "received_at", "expected_at", "supplier"),
    ),
    OperatorTemplate(
        key="late_delivery_rate",
        description="Share of deliveries arriving after expected date.",
        sql_shape_hint="received_at > expected_at ratio",
        phrases=("late delivery rate", "late deliveries", "on time delivery"),
        signal_tokens=("expected", "received", "supplier", "delay"),
    ),
    OperatorTemplate(
        key="shrinkage_rate",
        description="Measure inventory loss due to spoilage/damage/theft.",
        sql_shape_hint="loss_qty or loss_cost over inventory/sales baseline",
        phrases=("shrinkage", "inventory loss", "waste rate", "spoilage rate"),
        signal_tokens=("waste", "loss", "damaged", "expired"),
    ),
    OperatorTemplate(
        key="waste_cost_ratio",
        description="Waste cost as share of revenue or COGS.",
        sql_shape_hint="SUM(waste_cost)/NULLIF(SUM(revenue),0)",
        phrases=("waste cost", "waste ratio", "loss to waste"),
        signal_tokens=("waste", "estimated_cost", "revenue", "product"),
    ),
    OperatorTemplate(
        key="weekend_weekday_lift",
        description="Compare weekend versus weekday performance lift.",
        sql_shape_hint="CASE day_of_week in weekend bucket, compare aggregates",
        phrases=("weekend vs weekday", "weekend lift", "weekday lift"),
        signal_tokens=("date", "day", "weekend", "weekday", "category", "store"),
    ),
    # Finance and risk
    OperatorTemplate(
        key="gross_margin",
        description="Revenue minus COGS and margin percent.",
        sql_shape_hint="(revenue-cogs) and (revenue-cogs)/NULLIF(revenue,0)",
        phrases=("gross margin", "margin", "gross profit margin"),
        signal_tokens=("revenue", "cogs", "unit_cost", "unit_price"),
    ),
    OperatorTemplate(
        key="net_margin",
        description="Net income over revenue.",
        sql_shape_hint="net_income / NULLIF(revenue,0)",
        phrases=("net margin", "profit margin", "net profit"),
        signal_tokens=("income", "expense", "revenue"),
    ),
    OperatorTemplate(
        key="default_rate",
        description="Defaulted loans over total loans.",
        sql_shape_hint="COUNT(default_flag)/COUNT(*) with delinquency threshold",
        phrases=("default rate", "loan default", "credit default"),
        signal_tokens=("loan", "days_past_due", "default", "delinquent"),
    ),
    OperatorTemplate(
        key="delinquency_rate",
        description="Loans/accounts past due over active book.",
        sql_shape_hint="COUNT(past_due)/COUNT(active)",
        phrases=("delinquency", "past due rate", "dpd"),
        signal_tokens=("past_due", "days_past_due", "loan", "account"),
    ),
    OperatorTemplate(
        key="npl_ratio",
        description="Non-performing loans over total loans.",
        sql_shape_hint="SUM(non_performing_balance)/SUM(total_loan_balance)",
        phrases=("npl ratio", "non performing loan", "npl"),
        signal_tokens=("loan", "balance", "non-performing"),
    ),
    OperatorTemplate(
        key="provision_coverage",
        description="Provision reserves over non-performing exposures.",
        sql_shape_hint="provision_amount / NULLIF(npl_amount,0)",
        phrases=("provision coverage", "coverage ratio", "loan loss reserve"),
        signal_tokens=("provision", "reserve", "npl"),
    ),
    OperatorTemplate(
        key="failed_transaction_rate",
        description="Failed transactions over total attempts.",
        sql_shape_hint="COUNT(status='failed')/COUNT(*)",
        phrases=("failed transaction rate", "payment failure", "transaction failure"),
        signal_tokens=("transaction", "status", "failed", "declined"),
    ),
    OperatorTemplate(
        key="fraud_rate",
        description="Suspected fraudulent events over total events.",
        sql_shape_hint="COUNT(fraud_flag)/COUNT(*)",
        phrases=("fraud rate", "fraudulent", "suspicious transactions"),
        signal_tokens=("fraud", "risk", "alert", "chargeback"),
    ),
    OperatorTemplate(
        key="chargeback_rate",
        description="Chargebacks as share of settled transactions.",
        sql_shape_hint="COUNT(chargeback)/COUNT(settled)",
        phrases=("chargeback rate", "chargebacks"),
        signal_tokens=("chargeback", "dispute", "settled"),
    ),
    OperatorTemplate(
        key="net_interest_income",
        description="Interest income minus interest expense.",
        sql_shape_hint="SUM(interest_income)-SUM(interest_expense)",
        phrases=("net interest income", "nii"),
        signal_tokens=("interest", "income", "expense"),
    ),
    OperatorTemplate(
        key="cost_income_ratio",
        description="Operating cost over operating income.",
        sql_shape_hint="operating_cost / NULLIF(operating_income,0)",
        phrases=("cost income ratio", "efficiency ratio"),
        signal_tokens=("cost", "income", "operating"),
    ),
    OperatorTemplate(
        key="liquidity_ratio",
        description="Coverage of short-term obligations with liquid assets.",
        sql_shape_hint="liquid_assets / short_term_liabilities",
        phrases=("liquidity ratio", "coverage ratio", "liquidity coverage"),
        signal_tokens=("liquid", "liability", "coverage"),
    ),
    OperatorTemplate(
        key="capital_adequacy",
        description="Capital adequacy/capital ratio checks.",
        sql_shape_hint="capital / risk_weighted_assets",
        phrases=("capital adequacy", "capital ratio", "car"),
        signal_tokens=("capital", "rwa", "risk weighted"),
    ),
    # Customer analytics
    OperatorTemplate(
        key="active_users",
        description="Count unique active entities in a time window.",
        sql_shape_hint="COUNT(DISTINCT entity_id) with activity filter",
        phrases=("active users", "active accounts", "active customers"),
        signal_tokens=("active", "status", "last_seen", "event"),
    ),
    OperatorTemplate(
        key="new_vs_returning",
        description="Split behavior between first-time and returning entities.",
        sql_shape_hint="first_seen date comparison / CASE new vs returning",
        phrases=("new vs returning", "first time", "repeat customers"),
        signal_tokens=("first", "repeat", "returning", "customer"),
    ),
    OperatorTemplate(
        key="segment_performance",
        description="Compare KPI across customer/product segments.",
        sql_shape_hint="GROUP BY segment with KPI aggregates",
        phrases=("segment", "cohort", "tier", "persona"),
        signal_tokens=("segment", "category", "tier", "group"),
    ),
    OperatorTemplate(
        key="r_f_m_scoring",
        description="Recency, frequency, monetary scoring pattern.",
        sql_shape_hint="aggregate recency/frequency/value and score buckets",
        phrases=("rfm", "recency frequency monetary", "customer value score"),
        signal_tokens=("last_purchase", "frequency", "amount"),
    ),
    OperatorTemplate(
        key="basket_analysis",
        description="Analyze co-purchase affinity between items.",
        sql_shape_hint="self-join transactions by basket/order id and pair counts",
        phrases=("basket", "bought together", "affinity", "market basket"),
        signal_tokens=("order_id", "product", "pair", "co-purchase"),
    ),
    # Temporal and anomaly analytics
    OperatorTemplate(
        key="time_series_trend",
        description="Time-bucketed trend by day/week/month.",
        sql_shape_hint="DATE_TRUNC bucket, aggregate metric, ORDER BY bucket",
        phrases=("trend", "over time", "time series", "daily", "weekly", "monthly"),
        signal_tokens=("date", "time", "timestamp", "period"),
    ),
    OperatorTemplate(
        key="seasonality",
        description="Compare recurring patterns by weekday/month/season.",
        sql_shape_hint="extract dow/month and compare normalized metric",
        phrases=("seasonality", "seasonal", "weekday pattern", "monthly pattern"),
        signal_tokens=("day", "month", "season", "period"),
    ),
    OperatorTemplate(
        key="anomaly_detection",
        description="Identify spikes/drops relative to baseline.",
        sql_shape_hint="z-score or pct deviation from moving baseline",
        phrases=("anomaly", "spike", "outlier", "unusual"),
        signal_tokens=("baseline", "stddev", "deviation", "outlier"),
    ),
    OperatorTemplate(
        key="volatility",
        description="Measure variability over time.",
        sql_shape_hint="STDDEV(metric) by entity/period",
        phrases=("volatility", "variability", "unstable"),
        signal_tokens=("stddev", "variance", "spread"),
    ),
    OperatorTemplate(
        key="change_point",
        description="Find significant regime shifts in trend.",
        sql_shape_hint="window comparisons before vs after candidate date",
        phrases=("change point", "structural break", "regime shift"),
        signal_tokens=("before", "after", "break", "shift"),
    ),
    OperatorTemplate(
        key="lag_lead_comparison",
        description="Compare metric to prior/future period.",
        sql_shape_hint="LAG/LEAD window functions",
        phrases=("lag", "lead", "previous period", "next period"),
        signal_tokens=("lag", "lead", "period"),
    ),
    # Operational quality
    OperatorTemplate(
        key="sla_breach_rate",
        description="Share of records breaching SLA threshold.",
        sql_shape_hint="COUNT(duration > sla_limit)/COUNT(*)",
        phrases=("sla breach", "breach rate", "missed sla"),
        signal_tokens=("sla", "duration", "deadline", "breach"),
    ),
    OperatorTemplate(
        key="cycle_time",
        description="Elapsed time across process stages.",
        sql_shape_hint="completed_at - started_at aggregations",
        phrases=("cycle time", "turnaround time", "processing time"),
        signal_tokens=("started", "completed", "duration", "elapsed"),
    ),
    OperatorTemplate(
        key="backlog_ageing",
        description="Age distribution of open work items.",
        sql_shape_hint="CURRENT_DATE - created_at bucketed by age bands",
        phrases=("backlog ageing", "aging", "open items age"),
        signal_tokens=("created_at", "open", "pending", "age"),
    ),
    OperatorTemplate(
        key="throughput",
        description="Volume processed per unit time.",
        sql_shape_hint="COUNT(*) or SUM(volume) by time bucket",
        phrases=("throughput", "processed per day", "velocity"),
        signal_tokens=("count", "volume", "daily", "hourly"),
    ),
    OperatorTemplate(
        key="first_pass_yield",
        description="Share completed without rework/retry.",
        sql_shape_hint="COUNT(no_rework)/COUNT(total)",
        phrases=("first pass yield", "fp y", "no rework"),
        signal_tokens=("retry", "rework", "first pass"),
    ),
    # Comparative analytics
    OperatorTemplate(
        key="benchmark_comparison",
        description="Compare entities against benchmark/control group.",
        sql_shape_hint="entity metric vs benchmark metric delta",
        phrases=("benchmark", "vs benchmark", "compared to", "control group"),
        signal_tokens=("benchmark", "control", "comparison"),
    ),
    OperatorTemplate(
        key="before_after",
        description="Compare KPI before and after an event/date.",
        sql_shape_hint="CASE period_before_after then aggregate and diff",
        phrases=("before and after", "pre vs post", "after launch"),
        signal_tokens=("before", "after", "event date", "launch"),
    ),
    OperatorTemplate(
        key="a_b_test",
        description="Evaluate treatment vs control performance.",
        sql_shape_hint="group by variant and compare primary metric",
        phrases=("a/b", "ab test", "treatment vs control", "experiment"),
        signal_tokens=("variant", "control", "treatment", "experiment"),
    ),
    OperatorTemplate(
        key="ratio_analysis",
        description="Compute key ratios between related measures.",
        sql_shape_hint="numerator / NULLIF(denominator,0)",
        phrases=("ratio", "per", "as a percentage of"),
        signal_tokens=("numerator", "denominator", "percent", "per"),
    ),
    OperatorTemplate(
        key="efficiency",
        description="Output per unit of input/resource.",
        sql_shape_hint="output_metric / input_metric",
        phrases=("efficiency", "productivity", "yield"),
        signal_tokens=("output", "input", "resource", "per"),
    ),
    # Risk and compliance
    OperatorTemplate(
        key="exposure_concentration",
        description="Concentration risk by segment/counterparty.",
        sql_shape_hint="share by counterparty/segment with top concentration",
        phrases=("concentration", "exposure concentration", "single name risk"),
        signal_tokens=("exposure", "counterparty", "segment", "share"),
    ),
    OperatorTemplate(
        key="limit_breach",
        description="Detect records crossing configured limits.",
        sql_shape_hint="CASE metric > limit then breach flag",
        phrases=("limit breach", "threshold breach", "policy breach"),
        signal_tokens=("limit", "threshold", "breach", "policy"),
    ),
    OperatorTemplate(
        key="alerts_summary",
        description="Summarize alert counts and severity distribution.",
        sql_shape_hint="GROUP BY severity/type and count",
        phrases=("alerts", "incidents", "exceptions", "violations"),
        signal_tokens=("alert", "severity", "incident", "type"),
    ),
    # Data quality and reconciliation
    OperatorTemplate(
        key="null_rate",
        description="Measure missingness by column/table/segment.",
        sql_shape_hint="SUM(CASE WHEN col IS NULL THEN 1 END)/COUNT(*)",
        phrases=("null rate", "missing values", "completeness"),
        signal_tokens=("null", "missing", "blank", "completeness"),
    ),
    OperatorTemplate(
        key="duplicate_rate",
        description="Identify duplicate records by business key.",
        sql_shape_hint="GROUP BY key HAVING COUNT(*) > 1",
        phrases=("duplicates", "duplicate rate", "dedupe"),
        signal_tokens=("duplicate", "business key", "count"),
    ),
    OperatorTemplate(
        key="reconciliation",
        description="Compare two sources and quantify mismatch.",
        sql_shape_hint="FULL OUTER JOIN or aligned aggregates with diff",
        phrases=("reconcile", "reconciliation", "mismatch", "difference between systems"),
        signal_tokens=("source", "target", "difference", "gap"),
    ),
    OperatorTemplate(
        key="freshness_lag",
        description="Measure latency between event time and load/update time.",
        sql_shape_hint="MAX(loaded_at - event_at) and distribution",
        phrases=("freshness", "data lag", "latency", "staleness"),
        signal_tokens=("event_time", "loaded_at", "updated_at", "lag"),
    ),
)


def _normalize_text(text: str) -> str:
    return " ".join((text or "").lower().strip().split())


def _table_token_index(table_columns: dict[str, list[str]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for table, columns in table_columns.items():
        table_lower = table.lower()
        table_tokens = table_lower.replace(".", " ").replace("_", " ").split()
        for token in table_tokens:
            if token:
                index.setdefault(token, table)
        for col in columns:
            col_lower = col.lower()
            col_tokens = col_lower.replace("_", " ").split()
            for token in col_tokens:
                if token:
                    index.setdefault(token, table)
    return index


def match_operator_templates(query: str, *, limit: int = 8) -> list[OperatorMatch]:
    """Score and return operator templates that match the query."""
    text = _normalize_text(query)
    if not text:
        return []

    matches: list[OperatorMatch] = []
    for template in _OPERATOR_TEMPLATES:
        score = 0
        for phrase in template.phrases:
            phrase_norm = _normalize_text(phrase)
            if phrase_norm and phrase_norm in text:
                score += max(3, len(phrase_norm.split()))
        for token in template.signal_tokens:
            token_norm = token.lower().strip()
            if token_norm and token_norm in text:
                score += 1
        if score > 0:
            matches.append(OperatorMatch(template=template, score=score))

    matches.sort(key=lambda item: (item.score, item.template.key), reverse=True)
    return matches[: max(1, limit)]


def build_operator_guidance(
    query: str,
    *,
    table_columns: dict[str, list[str]] | None = None,
    max_templates: int = 8,
    max_table_hints: int = 3,
) -> str:
    """Build compact operator-template hints for prompt injection."""
    matches = match_operator_templates(query, limit=max_templates)
    if not matches:
        return ""

    token_index = _table_token_index(table_columns or {})

    lines = ["**Analytic operator hints (semantic patterns):**"]
    for item in matches:
        template = item.template
        candidate_tables: list[str] = []
        for token in template.signal_tokens:
            table = token_index.get(token.lower())
            if table and table not in candidate_tables:
                candidate_tables.append(table)
            if len(candidate_tables) >= max_table_hints:
                break

        table_suffix = (
            f" Likely tables: {', '.join(candidate_tables)}." if candidate_tables else ""
        )
        lines.append(
            f"- {template.key}: {template.description} "
            f"SQL shape: {template.sql_shape_hint}.{table_suffix}"
        )

    return "\n".join(lines)


def operator_template_count() -> int:
    """Return total number of built-in operator templates."""
    return len(_OPERATOR_TEMPLATES)
