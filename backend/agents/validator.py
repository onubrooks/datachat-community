"""
ValidatorAgent: SQL validation with security and performance checks.

This agent performs rule-based validation on generated SQL queries:
- Syntax validation using sqlparse
- Security checks (SQL injection patterns)
- Schema validation (table/column existence)
- Performance warnings (SELECT *, missing WHERE, missing LIMIT)

NO LLM calls - pure rule-based validation for speed and reliability.
"""

import logging
import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Comment, Keyword, Whitespace

from backend.agents.base import BaseAgent
from backend.models import (
    SQLValidationError,
    ValidatedSQL,
    ValidationWarning,
    ValidatorAgentInput,
    ValidatorAgentOutput,
)

logger = logging.getLogger(__name__)


class ValidatorAgent(BaseAgent):
    """
    SQL validation agent with security and performance checks.

    This agent validates SQL queries using rule-based checks:
    1. Syntax validation (sqlparse)
    2. Security validation (SQL injection patterns)
    3. Schema validation (table/column existence)
    4. Performance validation (common anti-patterns)

    NO LLM calls - designed for speed and deterministic results.
    """

    # SQL injection patterns (case-insensitive)
    INJECTION_PATTERNS = [
        r";\s*DROP\s+",
        r";\s*DELETE\s+",
        r";\s*UPDATE\s+.*WHERE\s+1\s*=\s*1",
        r";\s*INSERT\s+",
        r";\s*ALTER\s+",
        r";\s*CREATE\s+",
        r";\s*TRUNCATE\s+",
        r"EXEC\s*\(",
        r"EXECUTE\s*\(",
        r"sp_executesql",
        r"xp_cmdshell",
        r"--\s*$",  # SQL comments at end
        r"/\*.*\*/",  # Block comments
        r"UNION\s+.*SELECT",  # Union-based injection
        r"OR\s+1\s*=\s*1",  # Always-true conditions
        r"OR\s+'[^']*'\s*=\s*'[^']*'",  # String-based always-true
    ]

    # Performance anti-patterns
    PERFORMANCE_PATTERNS = {
        "select_star": r"SELECT\s+\*\s+FROM",
        "missing_where": r"FROM\s+[a-zA-Z0-9_.]+\s*(?:;|$)",  # FROM without WHERE
        "missing_limit": r"SELECT\s+.*\s+FROM\s+.*(?<!LIMIT\s+\d+)\s*(?:;|$)",
    }

    def __init__(self):
        """Initialize ValidatorAgent."""
        super().__init__(name="ValidatorAgent")

    async def execute(self, input: ValidatorAgentInput) -> ValidatorAgentOutput:
        """
        Validate SQL query with security and performance checks.

        Args:
            input: ValidatorAgentInput with generated SQL and schema context

        Returns:
            ValidatorAgentOutput with validation results

        Raises:
            ValueError: If input validation fails
        """
        # Validate input
        if not input.generated_sql.sql.strip():
            raise ValueError("SQL cannot be empty")

        logger.info(
            f"[{self.name}] Validating SQL (database={input.target_database}, "
            f"strict={input.strict_mode})"
        )

        sql = input.generated_sql.sql.strip()
        errors: list[SQLValidationError] = []
        warnings: list[ValidationWarning] = []
        suggestions: list[str] = []

        # 1. Syntax validation
        syntax_errors = self._validate_syntax(sql, input.target_database)
        errors.extend(syntax_errors)

        # 2. Security validation
        security_errors = self._validate_security(sql)
        errors.extend(security_errors)

        # 3. Schema validation (if datapoints provided)
        if input.generated_sql.used_datapoints:
            schema_errors, schema_warnings = self._validate_schema(
                sql, input.generated_sql.used_datapoints
            )
            errors.extend(schema_errors)
            warnings.extend(schema_warnings)

        # 4. Performance validation
        perf_warnings, perf_suggestions = self._validate_performance(sql)
        warnings.extend(perf_warnings)
        suggestions.extend(perf_suggestions)

        # Calculate performance score (0-1, higher is better)
        performance_score = self._calculate_performance_score(sql, perf_warnings)

        # Determine if SQL is valid and safe
        is_valid = len(errors) == 0
        is_safe = len([e for e in errors if e.error_type == "security"]) == 0

        # In strict mode, warnings count as errors
        if input.strict_mode and len(warnings) > 0:
            is_valid = False
            # Convert warnings to errors
            for warning in warnings:
                errors.append(
                    SQLValidationError(
                        error_type="other",
                        message=f"[STRICT MODE] {warning.message}",
                        severity="medium",
                    )
                )

        validated_sql = ValidatedSQL(
            is_valid=is_valid,
            sql=sql,
            errors=errors,
            warnings=warnings,
            suggestions=suggestions,
            is_safe=is_safe,
            performance_score=performance_score,
        )

        logger.info(
            f"[{self.name}] Validation complete: valid={is_valid}, safe={is_safe}, "
            f"errors={len(errors)}, warnings={len(warnings)}, score={performance_score:.2f}"
        )

        return ValidatorAgentOutput(
            success=True,
            validated_sql=validated_sql,
            metadata=self._create_metadata(),
        )

    def _validate_syntax(self, sql: str, target_database: str) -> list[SQLValidationError]:
        """
        Validate SQL syntax using sqlparse.

        Args:
            sql: SQL query to validate
            target_database: Target database type for dialect-specific checks

        Returns:
            List of syntax errors
        """
        errors: list[SQLValidationError] = []

        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)

            if not parsed:
                errors.append(
                    SQLValidationError(
                        error_type="syntax",
                        message="Failed to parse SQL - invalid syntax",
                        severity="critical",
                    )
                )
                return errors

            # Check for multiple statements (potential injection)
            if len(parsed) > 1:
                errors.append(
                    SQLValidationError(
                        error_type="syntax",
                        message="Multiple SQL statements detected - only single SELECT allowed",
                        severity="critical",
                    )
                )

            # Get first statement
            stmt = parsed[0]

            # Check if it's a SELECT or WITH statement
            # IMPORTANT: If it starts with WITH, we need to check the main statement
            # after the CTE to prevent "WITH t AS (...) DELETE FROM users"
            first_token = stmt.token_first(skip_ws=True, skip_cm=True)
            if first_token:
                token_value = first_token.value.upper()

                if token_value == "WITH":
                    # CTE detected - find the main statement after CTE
                    main_statement_keyword = self._find_main_statement_after_cte(stmt)
                    if main_statement_keyword != "SELECT":
                        errors.append(
                            SQLValidationError(
                                error_type="security",
                                message=f"CTE can only precede SELECT statements, found: {main_statement_keyword}",
                                severity="critical",
                            )
                        )
                elif token_value != "SELECT":
                    # Not WITH, not SELECT - reject
                    if first_token.ttype in (Keyword, Keyword.DML, Keyword.CTE):
                        errors.append(
                            SQLValidationError(
                                error_type="syntax",
                                message=f"Only SELECT queries allowed, found: {token_value}",
                                severity="critical",
                            )
                        )
                    else:
                        errors.append(
                            SQLValidationError(
                                error_type="syntax",
                                message="Query must start with SELECT or WITH keyword",
                                severity="critical",
                            )
                        )
            else:
                errors.append(
                    SQLValidationError(
                        error_type="syntax",
                        message="Query must start with SELECT or WITH keyword",
                        severity="critical",
                    )
                )

            # Database-specific validation
            if target_database == "postgresql":
                # Check for PostgreSQL-specific syntax issues
                if "LIMIT" in sql.upper() and "OFFSET" in sql.upper():
                    # Valid PostgreSQL syntax
                    pass
            elif target_database == "clickhouse":
                # Check for ClickHouse-specific syntax
                if "FINAL" in sql.upper():
                    # Valid ClickHouse modifier
                    pass

        except Exception as e:
            errors.append(
                SQLValidationError(
                    error_type="syntax",
                    message=f"Syntax parsing failed: {str(e)}",
                    severity="critical",
                )
            )

        return errors

    def _validate_security(self, sql: str) -> list[SQLValidationError]:
        """
        Validate SQL for security issues (injection patterns).

        Args:
            sql: SQL query to validate

        Returns:
            List of security errors
        """
        errors: list[SQLValidationError] = []

        # Check for SQL injection patterns
        for pattern in self.INJECTION_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE | re.MULTILINE):
                errors.append(
                    SQLValidationError(
                        error_type="security",
                        message=f"Potential SQL injection detected: matches pattern '{pattern}'",
                        severity="critical",
                    )
                )

        # Check for dangerous functions
        dangerous_functions = [
            "LOAD_FILE",
            "INTO OUTFILE",
            "INTO DUMPFILE",
            "SYSTEM",
            "SHELL",
        ]
        sql_upper = sql.upper()
        for func in dangerous_functions:
            if func in sql_upper:
                errors.append(
                    SQLValidationError(
                        error_type="security",
                        message=f"Dangerous function detected: {func}",
                        severity="critical",
                    )
                )

        return errors

    def _validate_schema(
        self, sql: str, used_datapoint_ids: list[str]
    ) -> tuple[list[SQLValidationError], list[ValidationWarning]]:
        """
        Validate SQL against schema from DataPoints.

        Note: This is a basic implementation that checks table names.
        Full column validation would require access to the actual DataPoints.

        Args:
            sql: SQL query to validate
            used_datapoint_ids: DataPoint IDs used in query generation

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[SQLValidationError] = []
        warnings: list[ValidationWarning] = []

        # Extract table names from SQL for potential future use
        # Currently not used but will be needed when we implement full schema validation
        _ = self._extract_table_names(sql)  # noqa: F841

        # Note: In a full implementation, we would:
        # 1. Load DataPoints by ID
        # 2. Extract available tables and columns
        # 3. Validate all table/column references against extracted table_names
        # For now, we do basic validation assuming datapoint_ids are provided

        if not used_datapoint_ids:
            warnings.append(
                ValidationWarning(
                    warning_type="other",
                    message="No DataPoint IDs provided - skipping schema validation",
                    suggestion="Ensure SQLAgent provides used_datapoints for schema validation",
                )
            )

        return errors, warnings

    def _validate_performance(self, sql: str) -> tuple[list[ValidationWarning], list[str]]:
        """
        Validate SQL for performance issues.

        Args:
            sql: SQL query to validate

        Returns:
            Tuple of (warnings, suggestions)
        """
        warnings: list[ValidationWarning] = []
        suggestions: list[str] = []

        sql_upper = sql.upper()

        # Check for SELECT *
        if re.search(self.PERFORMANCE_PATTERNS["select_star"], sql_upper):
            warnings.append(
                ValidationWarning(
                    warning_type="performance",
                    message="SELECT * detected - selecting all columns may impact performance",
                    suggestion="Specify only the columns you need",
                )
            )
            suggestions.append("Replace SELECT * with specific column names")

        # Check for missing WHERE clause (on non-aggregation queries)
        if "WHERE" not in sql_upper and "GROUP BY" not in sql_upper:
            # Only warn if it's not a simple aggregation
            if not re.search(r"SELECT\s+COUNT\(\*\)\s+FROM", sql_upper):
                warnings.append(
                    ValidationWarning(
                        warning_type="performance",
                        message="Query has no WHERE clause - may scan entire table",
                        suggestion="Add WHERE clause to filter data",
                    )
                )
                suggestions.append("Consider adding a WHERE clause to limit data scanned")

        # Check for missing LIMIT on large result sets
        if "LIMIT" not in sql_upper and "COUNT" not in sql_upper:
            warnings.append(
                ValidationWarning(
                    warning_type="performance",
                    message="Query has no LIMIT clause - may return large result set",
                    suggestion="Add LIMIT clause for large tables",
                )
            )
            suggestions.append("Consider adding LIMIT to prevent large result sets")

        # Check for inefficient JOINs
        if "JOIN" in sql_upper:
            # Count number of joins
            join_count = sql_upper.count("JOIN")
            if join_count > 4:
                warnings.append(
                    ValidationWarning(
                        warning_type="performance",
                        message=f"Query has {join_count} JOINs - may be slow on large tables",
                        suggestion="Consider denormalizing data or using materialized views",
                    )
                )

        # Check for DISTINCT on large result sets
        if "DISTINCT" in sql_upper and "LIMIT" not in sql_upper:
            warnings.append(
                ValidationWarning(
                    warning_type="performance",
                    message="DISTINCT without LIMIT may be slow on large tables",
                    suggestion="Add LIMIT or ensure proper indexes exist",
                )
            )

        return warnings, suggestions

    def _extract_table_names(self, sql: str) -> set[str]:
        """
        Extract table names from SQL query.

        Args:
            sql: SQL query

        Returns:
            Set of table names (uppercase)
        """
        table_names: set[str] = set()

        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return table_names

            stmt = parsed[0]

            # Extract CTE names first
            cte_names = self._extract_cte_names(sql)

            # Find FROM and JOIN clauses
            for token in stmt.tokens:
                if isinstance(token, Identifier):
                    # Direct table reference
                    table_name = token.get_real_name()
                    if table_name and table_name.upper() not in cte_names:
                        table_names.add(table_name.upper())
                elif isinstance(token, IdentifierList):
                    # Multiple tables
                    for identifier in token.get_identifiers():
                        table_name = identifier.get_real_name()
                        if table_name and table_name.upper() not in cte_names:
                            table_names.add(table_name.upper())

        except Exception as e:
            logger.warning(f"Failed to extract table names: {e}")

        return table_names

    def _extract_cte_names(self, sql: str) -> set[str]:
        """
        Extract CTE names from WITH clause.

        Args:
            sql: SQL query

        Returns:
            Set of CTE names (uppercase)
        """
        cte_names: set[str] = set()

        # Pattern: WITH cte_name AS (...)
        cte_pattern = r"WITH\s+([a-zA-Z0-9_]+)\s+AS\s*\("
        cte_matches = re.findall(cte_pattern, sql, re.IGNORECASE)
        for cte_name in cte_matches:
            cte_names.add(cte_name.upper())

        # Pattern: , cte_name AS (...)
        additional_cte_pattern = r",\s*([a-zA-Z0-9_]+)\s+AS\s*\("
        additional_ctes = re.findall(additional_cte_pattern, sql, re.IGNORECASE)
        for cte_name in additional_ctes:
            cte_names.add(cte_name.upper())

        return cte_names

    def _find_main_statement_after_cte(self, stmt) -> str:
        """
        Find the main statement keyword after a CTE (WITH clause).

        This prevents attacks like "WITH t AS (...) DELETE FROM users"
        by ensuring CTEs can only precede SELECT statements.

        Args:
            stmt: Parsed SQL statement object from sqlparse

        Returns:
            Main statement keyword (e.g., "SELECT", "DELETE", "UPDATE", "UNKNOWN")
        """
        # Walk through tokens to find the main statement after CTE
        # CTE structure: WITH cte_name AS (...) SELECT ...
        # We need to skip past the CTE definition and find the next DML keyword

        in_cte_definition = False
        paren_depth = 0

        for token in stmt.tokens:
            # Skip whitespace and comments
            if token.ttype in (Whitespace, Comment.Single, Comment.Multiline):
                continue

            # If we see WITH, we're starting CTE
            if token.ttype == Keyword.CTE or (
                token.ttype == Keyword and token.value.upper() == "WITH"
            ):
                in_cte_definition = True
                continue

            # Track parentheses depth in CTE definition
            if in_cte_definition:
                token_str = str(token)
                # Count opening parens
                paren_depth += token_str.count("(")
                # Count closing parens
                paren_depth -= token_str.count(")")

                # If we've closed all parens and see a keyword, that's our main statement
                if paren_depth == 0 and token.ttype in (Keyword, Keyword.DML):
                    keyword = token.value.upper()
                    # Check for SELECT, DELETE, UPDATE, INSERT, etc.
                    if keyword in ("SELECT", "DELETE", "UPDATE", "INSERT", "REPLACE"):
                        return keyword

        # If we can't determine the main statement, return UNKNOWN
        # This will cause validation to fail, which is safer
        return "UNKNOWN"

    def _calculate_performance_score(self, sql: str, warnings: list[ValidationWarning]) -> float:
        """
        Calculate performance score based on query characteristics.

        Score ranges from 0 (worst) to 1 (best).

        Args:
            sql: SQL query
            warnings: Performance warnings

        Returns:
            Performance score (0-1)
        """
        score = 1.0

        # Deduct points for each warning
        for warning in warnings:
            if warning.warning_type == "performance":
                if "SELECT *" in warning.message:
                    score -= 0.15
                elif "WHERE clause" in warning.message:
                    score -= 0.20
                elif "LIMIT clause" in warning.message:
                    score -= 0.10
                elif "JOINs" in warning.message:
                    score -= 0.15
                elif "DISTINCT" in warning.message:
                    score -= 0.10

        # Bonus for good practices
        sql_upper = sql.upper()

        if "WHERE" in sql_upper:
            score += 0.05

        if "LIMIT" in sql_upper:
            score += 0.05

        # Clamp score to [0, 1]
        return max(0.0, min(1.0, score))
