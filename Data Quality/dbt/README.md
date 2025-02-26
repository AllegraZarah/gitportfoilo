# Data Quality with dbt

## Project Description
Ensuring data reliability, accuracy, and trustworthiness throughout the transformation process. This directory provides a structured approach to data integrity in analytical workflows using dbt. By combining automated tests with custom validations, it strengthens data quality across the trading pipeline, enforcing strict controls on source data, transformations, and outputs.   

Leveraging dbt’s native tests and custom macros, this project delivers a scalable, production-ready solution. From enforcing referential integrity to detecting statistical anomalies, each test is designed to uphold rigorous data governance standards.


&nbsp;
## Project Overview

| Project Component      | Focus Area                 | Key Technologies      |  
|------------------------|---------------------------|----------------------|  
| [dbt Tests](#dbt-tests)         | Standard Data Validation  | dbt, SQL            |  
| [Custom Tests](#custom-tests)      | Advanced Business Rules   | dbt Macros, SQL     |  
| [dbt Expectations](#dbt_expectations)  | Statistical & Anomaly Detection | dbt, dbt_expectations |  
| [Airflow DAG](#automation-with-airflow)      | Automated Test Execution      | Python, Airflow          | 


&nbsp;

## Testing Strategy

### 1. Source Data Validation
- **Freshness Checks**: Ensures data recency.
- **Completeness**: Validates required fields.
- **Referential Integrity**: Verifies relationships between tables.
- **Format Validation**: Checks data formats (e.g., dates, codes).

### 2. Transformation Testing
- **Business Rule Validation**: Enforces domain-specific rules.
- **Consistency Checks**: Ensures data consistency across models.
- **Range Validation**: Verifies numerical values within expected bounds.

### 3. Output Validation
- **Row Count Checks**: Monitors expected volume ranges.
- **Null Ratio Analysis**: Tracks missing data patterns.
- **Statistical Validation**: Identifies anomalies.


&nbsp;

## Tools and Implementation

### <u>[dbt Tests](./)</u>
```yaml
# Example test configuration
tests:
  - not_null
  - unique
  - relationships:
      to: ref('dim_product')
      field: product_code
```

### <u>[Custom Tests](./macros/custom_tests/)</u>
For advanced validation, custom macros extend dbt's testing framework:
```sql
-- Example custom test
default__test_price_movement.sql
SELECT *
FROM {{ model }}
WHERE price_change > threshold_value
```

### <u>[dbt_expectations](./)</u>
We leverage `dbt_expectations` for enhanced testing:
```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 0
      strictly_positive: true
  - dbt_expectations.expect_row_values_to_have_recent_data:
      datepart: day
      interval: 1
```

&nbsp;

## Monitoring and Alerting

### [Automation with Airflow](./dbt_dag.py)
Integrated dbt tests into Airflow workflow, to:
- Run tests automatically after data loads
- Trigger alerts when tests fail
- Maintain a history of test results

### Test Failure Tracking
- Daily test execution via airflow orchestration.
- [Failure notifications via email](./email_util.py).
- Historical test results tracking in Open MetaDtaa for trend analysis.

### Quality Metrics Dashboard
- Test coverage metrics.
- Failure rate trends.
- Data quality score tracking.

&nbsp;

## Getting Started

To run the tests locally:
```bash
dbt test
```

To view test documentation:
```bash
dbt docs generate
dbt docs serve
```

&nbsp;
## Contact

For questions about implementation details or collaboration opportunities, please reach out through GitHub issues or direct messages.