# Data Quality with Great Expectations

This directory delivers a comprehensive data quality framework built with Great Expectations. It validates critical data assets to ensure accuracy, consistency, and trustworthiness, empowering data-driven decision-making. By combining flexible expectation suites with custom validations, the framework proactively identifies anomalies and upholds rigorous data governance standards.

Reliable data quality is essential for generating actionable insights. By automating validations and monitoring data integrity, this project minimizes manual intervention, reduces errors, and builds confidence in your analytical processes.

&nbsp;
## Project Overview

| Project Component                   | Focus Area                                             | Key Technologies                                          |
|-------------------------------------|--------------------------------------------------------|-----------------------------------------------------------|
| [Expectation Suite & Validation](#expectation-suite--validation)  | Define and validate data quality expectations via Jupyter Notebook and JSON outputs | Great Expectations, Jupyter Notebook                      |
| [Custom Expectations](#custom-expectations)             | Extend validation capabilities with domain-specific logic | Python, Great Expectations (e.g.: `expect_column_values_to_be_between_quartile_limits`) |
| [Orchestration & Scheduling](#orchestration--scheduling)      | Automate and schedule validation runs with result persistence | Python, Airflow                                           |

&nbsp;
## [Expectation Suite & Validation](./01_gx_configuration_public.fact_trades.ipynb)

- **Definition & Execution:**  
  A Jupyter Notebook is used to build and validate a comprehensive suite of expectations that cover aspects like data completeness, uniqueness, and statistical boundaries.

- **Validation Process:**  
  The JSON file is executed to produce detailed validation results. This execution captures key metrics and outcomes of the data quality checks, ensuring that every expectation is rigorously tested.

- **Output & Tracking:**  
  Validation outcomes are exported as JSON files, providing detailed metrics on data quality. These outputs enable historical tracking and facilitate a continuous improvement process.

&nbsp;
## [Custom Expectations](./custom_expectations/)

Beyond standard validations, custom expectations enable tailored checks specific to your data domains. Multiple custom expectations are included to address nuanced validation needs.

- **Example Usage:**  
  One such custom expectation, `expect_column_values_to_be_between_quartile_limits`, calculates the interquartile range (IQR) to flag outliers. For instance, its core logic involves:
  ```python
  q1 = column.quantile(q=0.25)
  q3 = column.quantile(q=0.75)
  iqr = q3 - q1
  lower = q1 - 1.5 * iqr
  upper = q3 + 1.5 * iqr
  return column.map(lambda x: lower < x < upper)
  ```
  This logic ensures that values falling outside the typical range are flagged, thereby improving the reliability of your data.

&nbsp;
## [Orchestration & Scheduling](./orchestration/)

- **Automation:**  
  A dedicated Python class (`GXOperator`) orchestrates the execution of expectation suites. It automates the process of running validations, capturing results, and storing them as JSON for further analysis.

- **Scheduled Execution:**  
  An Airflow DAG triggers these validations on a regular schedule. This integration is crucial as it guarantees continuous monitoring of data quality, ensuring that any deviations are promptly identified and addressed.

&nbsp;
## Monitoring & Impact

- **Real-Time Alerts & Historical Insights:**  
  The framework supports real-time monitoring through scheduled validations, which can be integrated with alerting systems (e.g., Slack) to notify teams of issues as they occur. Historical tracking of validation results enables trend analysis and proactive data quality management.

- **Business Impact:**  
  By reducing data errors and ensuring consistency, the framework enhances the reliability of your analytics. This leads to better decision-making, increased operational efficiency, and improved trust in your data infrastructure.

&nbsp;
## Future Enhancements

- **Real-Time Streaming Validations:** Expand the framework to validate streaming data in real time.
- **Extended Custom Expectations:** Develop additional custom validations to cover more complex data scenarios.
- **Enhanced Dashboards:** Build advanced dashboards for comprehensive monitoring of data quality metrics and trends.

&nbsp;
## Documentation & Maintenance

- Detailed usage examples and documentation are provided to help you extend and maintain the framework.
- Regular updates ensure the solution evolves alongside emerging data quality challenges.

&nbsp;
## Contact

For questions about implementation details or collaboration opportunities, please reach out through GitHub issues or direct messages.