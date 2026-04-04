"""Registered domain packs for retrieval and SQL planning."""

from __future__ import annotations

from typing import Any

from backend.domain_packs.base import DomainPack
from backend.models.agent import GeneratedSQL, SQLAgentInput


def _finance_aliases(value: str) -> list[str]:
    aliases: set[str] = set()
    normalized = value.strip().lower()
    if not normalized:
        return []

    if "deposit" in normalized or normalized == "credit":
        aliases.update({"deposits", "deposit", "credit", "credits", "inflow", "inflows"})
    if "withdraw" in normalized or normalized == "debit":
        aliases.update({"withdrawal", "withdrawals", "debit", "debits", "outflow", "outflows"})
    if "customer segment" in normalized or normalized == "segment":
        aliases.update({"segment", "segments", "customer segment", "customer segments"})
    if "net flow" in normalized:
        aliases.update({"net flow", "cash flow", "inflow minus outflow"})

    aliases.discard(normalized)
    return sorted(aliases)


def _grocery_aliases(value: str) -> list[str]:
    aliases: set[str] = set()
    normalized = value.strip().lower()
    if not normalized:
        return []

    if "stockout" in normalized or "out of stock" in normalized:
        aliases.update(
            {"stockout", "out of stock", "inventory risk", "reorder", "on hand", "reserved"}
        )
    if "inventory" in normalized:
        aliases.update({"stock", "on hand", "reserved", "reorder", "snapshot"})
    if normalized in {"sku", "skus"}:
        aliases.update({"sku", "skus", "product", "products", "item", "items"})

    aliases.discard(normalized)
    return sorted(aliases)


def _finance_template_score(query_lower: str, template_text: str) -> float:
    score = 0.0
    query_mentions_deposit = any(
        phrase in query_lower for phrase in ("deposit", "deposits", "credit", "credits")
    )
    query_mentions_withdrawal = any(
        phrase in query_lower
        for phrase in ("withdraw", "withdrawal", "withdrawals", "debit", "debits")
    )
    query_mentions_latest_anchor = (
        "last deposit date" in query_lower or "latest deposit date" in query_lower
    )
    template_has_deposit = any(
        token in template_text for token in ("deposit", "deposits", "credit", "credits", "inflow")
    )
    template_has_withdrawal = any(
        token in template_text
        for token in ("withdraw", "withdrawal", "withdrawals", "debit", "debits", "outflow")
    )
    template_is_account_balance = any(
        token in template_text for token in ("balance", "account opening", "opened_at", "current balance")
    )
    template_has_anchor = "latest_date_anchor" in template_text or "latest deposit date" in template_text

    if query_mentions_deposit and template_has_deposit:
        score += 2.5
    if query_mentions_withdrawal and template_has_withdrawal:
        score += 2.5
    if query_mentions_latest_anchor and template_has_anchor:
        score += 3.0
    if query_mentions_deposit and template_is_account_balance and not template_has_deposit:
        score -= 3.5
    if query_mentions_withdrawal and template_is_account_balance and not template_has_withdrawal:
        score -= 3.5

    return score


def _grocery_template_score(query_lower: str, template_text: str) -> float:
    score = 0.0
    query_mentions_stockout = any(
        phrase in query_lower for phrase in ("stockout", "out of stock", "inventory risk", "reorder risk")
    )
    query_mentions_inventory_signals = any(
        phrase in query_lower for phrase in ("on-hand", "on hand", "reserved", "reorder", "sku", "skus")
    )
    template_has_inventory_risk = any(
        token in template_text
        for token in (
            "stockout",
            "inventory risk",
            "reorder",
            "on hand",
            "on_hand",
            "reserved",
            "sku",
            "inventory_snapshot",
        )
    )
    template_is_sales_focused = any(
        token in template_text for token in ("revenue", "sales", "gross margin", "discount")
    )
    if query_mentions_stockout and template_has_inventory_risk:
        score += 3.0
    if query_mentions_inventory_signals and template_has_inventory_risk:
        score += 2.0
    if query_mentions_stockout and template_is_sales_focused and not template_has_inventory_risk:
        score -= 3.0
    return score


def _resolve_table_name(agent: Any, input: SQLAgentInput, required: str) -> str | None:
    table_name_map = agent._collect_available_table_names(input)
    return next(
        (
            table_name
            for key, table_name in table_name_map.items()
            if key == required or key.endswith(f".{required}")
        ),
        None,
    )


def _build_finance_net_flow_sql(agent: Any, input: SQLAgentInput) -> GeneratedSQL | None:
    query = (input.query or "").lower()
    required_signals = ("deposit", "withdraw", "net flow", "segment", "week")
    if not all(signal in query for signal in required_signals):
        return None

    transactions_table_name = _resolve_table_name(agent, input, "bank_transactions")
    accounts_table_name = _resolve_table_name(agent, input, "bank_accounts")
    customers_table_name = _resolve_table_name(agent, input, "bank_customers")
    if not transactions_table_name or not accounts_table_name or not customers_table_name:
        return None

    db_type = input.database_type or getattr(agent.config.database, "db_type", "postgresql")
    if db_type != "postgresql":
        return None

    transactions_table = agent.catalog.format_table_reference(transactions_table_name, db_type=db_type)
    accounts_table = agent.catalog.format_table_reference(accounts_table_name, db_type=db_type)
    customers_table = agent.catalog.format_table_reference(customers_table_name, db_type=db_type)
    if not transactions_table or not accounts_table or not customers_table:
        return None

    sql = (
        "WITH anchor AS ("
        f" SELECT MAX(t.business_date) AS max_business_date FROM {transactions_table} t"
        " WHERE t.status = 'posted'"
        "), weekly_segment_flow AS ("
        " SELECT"
        "   DATE_TRUNC('week', t.business_date)::date AS week_start,"
        "   c.segment AS segment,"
        "   SUM(CASE WHEN t.direction = 'credit' THEN t.amount ELSE 0 END) AS deposits,"
        "   SUM(CASE WHEN t.direction = 'debit' THEN t.amount ELSE 0 END) AS withdrawals,"
        "   SUM(CASE WHEN t.direction = 'credit' THEN t.amount ELSE -t.amount END) AS net_flow"
        f" FROM {transactions_table} t"
        f" JOIN {accounts_table} a ON a.account_id = t.account_id"
        f" JOIN {customers_table} c ON c.customer_id = a.customer_id"
        " CROSS JOIN anchor"
        " WHERE t.status = 'posted'"
        "   AND t.business_date >= (anchor.max_business_date - INTERVAL '7 weeks')"
        " GROUP BY 1, 2"
        "), with_wow AS ("
        " SELECT"
        "   week_start,"
        "   segment,"
        "   deposits,"
        "   withdrawals,"
        "   net_flow,"
        "   net_flow - LAG(net_flow) OVER (PARTITION BY segment ORDER BY week_start)"
        "     AS wow_net_flow_change"
        " FROM weekly_segment_flow"
        "), segment_decline AS ("
        " SELECT"
        "   segment,"
        "   SUM(CASE WHEN wow_net_flow_change < 0 THEN wow_net_flow_change ELSE 0 END)"
        "     AS total_wow_decline,"
        "   ROW_NUMBER() OVER ("
        "     ORDER BY SUM(CASE WHEN wow_net_flow_change < 0 THEN wow_net_flow_change ELSE 0 END)"
        "   ) AS decline_rank"
        " FROM with_wow"
        " GROUP BY segment"
        ")"
        " SELECT"
        "   w.week_start,"
        "   w.segment,"
        "   w.deposits,"
        "   w.withdrawals,"
        "   w.net_flow,"
        "   w.wow_net_flow_change,"
        "   (COALESCE(d.decline_rank, 999) <= 2) AS top_decline_driver"
        " FROM with_wow w"
        " LEFT JOIN segment_decline d ON d.segment = w.segment"
        " ORDER BY w.week_start DESC, w.segment"
    )
    return GeneratedSQL(
        sql=sql,
        explanation=(
            "Computed weekly deposits, withdrawals, and net flow by segment over the latest 8 weeks, "
            "then flagged top 2 segments with largest cumulative week-over-week net-flow decline."
        ),
        used_datapoints=[],
        confidence=0.78,
        assumptions=[
            "Deposits are inferred as transactions where direction = 'credit'.",
            "Withdrawals are inferred as transactions where direction = 'debit'.",
            "Window anchors to latest posted business_date in bank_transactions.",
        ],
        clarifying_questions=[],
    )


def _build_finance_loan_default_rate_sql(agent: Any, input: SQLAgentInput) -> GeneratedSQL | None:
    query = (input.query or "").lower()
    has_default_rate_intent = "default rate" in query or ("default" in query and "rate" in query)
    if not (has_default_rate_intent and "loan" in query and "segment" in query):
        return None

    loans_table_name = _resolve_table_name(agent, input, "bank_loans") or "public.bank_loans"
    customers_table_name = _resolve_table_name(agent, input, "bank_customers") or "public.bank_customers"

    db_type = input.database_type or getattr(agent.config.database, "db_type", "postgresql")
    if db_type != "postgresql":
        return None

    loans_table = agent.catalog.format_table_reference(loans_table_name, db_type=db_type)
    customers_table = agent.catalog.format_table_reference(customers_table_name, db_type=db_type)
    if not loans_table or not customers_table:
        return None

    sql = (
        "SELECT"
        "   c.segment,"
        "   COUNT(*) AS total_loans,"
        "   COUNT(*) FILTER ("
        "     WHERE l.days_past_due >= 90 OR l.status = 'non_performing'"
        "   ) AS defaulted_loans,"
        "   ROUND("
        "     100.0 * COUNT(*) FILTER ("
        "       WHERE l.days_past_due >= 90 OR l.status = 'non_performing'"
        "     ) / NULLIF(COUNT(*), 0),"
        "     2"
        "   ) AS default_rate_pct,"
        "   ROUND(AVG(l.days_past_due)::numeric, 2) AS avg_days_past_due"
        f" FROM {loans_table} l"
        f" JOIN {customers_table} c ON c.customer_id = l.customer_id"
        " GROUP BY c.segment"
        " ORDER BY default_rate_pct DESC, avg_days_past_due DESC"
    )

    return GeneratedSQL(
        sql=sql,
        explanation="Computed loan default rate by customer segment using 90+ DPD/non-performing default proxy.",
        used_datapoints=[],
        confidence=0.8,
        assumptions=[
            "Default proxy uses days_past_due >= 90 or status = 'non_performing'.",
            "Segment comes from bank_customers joined via customer_id.",
        ],
        clarifying_questions=[],
    )


def _build_grocery_stockout_risk_sql(agent: Any, input: SQLAgentInput) -> GeneratedSQL | None:
    query = (input.query or "").lower()
    if not ("stockout" in query and "risk" in query):
        return None
    if not any(token in query for token in ("sku", "skus", "product", "products", "inventory")):
        return None

    snapshots_table_name = _resolve_table_name(agent, input, "grocery_inventory_snapshots")
    products_table_name = _resolve_table_name(agent, input, "grocery_products")
    if not snapshots_table_name or not products_table_name:
        return None

    db_type = input.database_type or getattr(agent.config.database, "db_type", "postgresql")
    if db_type != "postgresql":
        return None

    snapshots_table = agent.catalog.format_table_reference(snapshots_table_name, db_type=db_type)
    products_table = agent.catalog.format_table_reference(products_table_name, db_type=db_type)
    if not snapshots_table or not products_table:
        return None

    top_n = agent._extract_template_parameter_value(
        query_lower=query,
        param_name="top_n",
        param_def={"type": "integer", "default": 5},
    )
    if not isinstance(top_n, int) or top_n <= 0:
        top_n = 5

    sql = (
        "WITH anchor AS ("
        f" SELECT MAX(snapshot_date) AS max_snapshot_date FROM {snapshots_table}"
        "), latest_weekly_snapshots AS ("
        " SELECT"
        "   s.product_id,"
        "   s.store_id,"
        "   s.on_hand_qty,"
        "   s.reserved_qty,"
        "   s.snapshot_date,"
        "   ROW_NUMBER() OVER ("
        "     PARTITION BY s.product_id, s.store_id"
        "     ORDER BY s.snapshot_date DESC"
        "   ) AS rn"
        f" FROM {snapshots_table} s"
        " CROSS JOIN anchor a"
        " WHERE s.snapshot_date >= a.max_snapshot_date - INTERVAL '7 days'"
        "), stock_levels AS ("
        " SELECT"
        "   lws.product_id,"
        "   lws.store_id,"
        "   gp.sku,"
        "   gp.product_name,"
        "   (lws.on_hand_qty - lws.reserved_qty) AS available_qty,"
        "   gp.reorder_level,"
        "   (lws.on_hand_qty - lws.reserved_qty) - gp.reorder_level AS stockout_risk_score,"
        "   lws.snapshot_date"
        " FROM latest_weekly_snapshots lws"
        f" JOIN {products_table} gp ON lws.product_id = gp.product_id"
        " WHERE lws.rn = 1"
        ")"
        " SELECT"
        "   product_id,"
        "   store_id,"
        "   sku,"
        "   product_name,"
        "   available_qty,"
        "   reorder_level,"
        "   stockout_risk_score,"
        "   snapshot_date"
        " FROM stock_levels"
        " ORDER BY stockout_risk_score ASC, product_id, store_id"
        f" LIMIT {top_n}"
    )
    return GeneratedSQL(
        sql=sql,
        explanation=(
            "Ranked SKUs by stockout risk using the latest weekly inventory snapshots, "
            "available quantity, and reorder level."
        ),
        used_datapoints=[],
        confidence=0.8,
        assumptions=[
            "Stockout risk is defined as available_qty minus reorder_level; lower values indicate higher risk.",
            "Window anchors to the latest snapshot_date in grocery_inventory_snapshots.",
        ],
        clarifying_questions=[],
    )


DOMAIN_PACKS: tuple[DomainPack, ...] = (
    DomainPack(
        name="finance",
        expand_aliases=_finance_aliases,
        adjust_template_score=_finance_template_score,
        sql_builders=(
            ("loan_default_rate_template", _build_finance_loan_default_rate_sql),
            ("net_flow_template", _build_finance_net_flow_sql),
        ),
    ),
    DomainPack(
        name="grocery",
        expand_aliases=_grocery_aliases,
        adjust_template_score=_grocery_template_score,
        sql_builders=(("stockout_risk_template", _build_grocery_stockout_risk_sql),),
    ),
)
