# Grocery Store DataPoints

This folder contains a realistic sample DataPoint bundle for DataPoint-driven testing.

Includes:
- Schema DataPoints for grocery inventory/sales operations
- Business metric DataPoints for revenue, margin, stockouts, and waste
- Process DataPoints for nightly inventory and daily sales rollups

Use with:

```bash
datachat dp sync --datapoints-dir datapoints/examples/grocery_store
```

or file-by-file:

```bash
datachat dp add schema datapoints/examples/grocery_store/table_grocery_stores_001.json
```
