import json
import unittest

from app import EXAMPLES, ROOT, answer, retrieve, validate_and_preview


class StakeholderSQLTests(unittest.TestCase):
    def test_monthly_net_revenue_respects_refunds_and_cancelled_orders(self):
        result = answer("What is net revenue by month?", "demo", preview=True)
        self.assertEqual(result["preview"], [
            {"month": "2026-01", "net_revenue_usd": 140.0},
            {"month": "2026-02", "net_revenue_usd": 60.0},
        ])
        self.assertIn("net_revenue", result["context_used"])

    def test_demo_mode_rejects_unknown_questions_instead_of_pretending_to_generate(self):
        with self.assertRaisesRegex(ValueError, "only --list-examples"):
            answer("What is the average order amount?", "demo")

    def test_restricted_columns_and_non_select_are_rejected(self):
        for sql in ("SELECT email FROM customers", "DELETE FROM orders", "SELECT * FROM sqlite_master",
                    "SELECT order_id FROM orders; DELETE FROM orders"):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                validate_and_preview(sql, preview=True)

    def test_retrieval_grounds_product_question(self):
        docs = json.loads((ROOT / "catalog.json").read_text())
        selected = [d["id"] for d in retrieve("Which products have the highest net sales?", docs)]
        self.assertIn("products", selected)
        self.assertIn("net_revenue", selected)

    def test_each_offline_example_compiles(self):
        for question in EXAMPLES:
            with self.subTest(question=question):
                self.assertIsNotNone(answer(question, "demo", preview=True)["preview"])


if __name__ == "__main__":
    unittest.main()
