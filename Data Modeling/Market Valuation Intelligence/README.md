# Market Valuation Intelligence

Transforming raw pricing data into sophisticated market intelligence through dbt-powered data models. This repository showcases a collection of projects that enable market valuation analysis, from daily price tracking to complex index calculations, providing a single source of truth for price-related analytics across the organization.

**Note:** These models are dbt models and have been structured accordingly. To avoid cluttering, this dbt project directory only features the dbt models directory, the dbt_project.yml file, sources.yml file, profiles.yml file (for reference), and this README.md file for documentation.

&nbsp;
## Project Overview

| Project | Focus Area | Key Capabilities |
|---------|------------|------------------|
| [Price Analysis](#price-analysis) | Daily Price Dynamics | End-of-day price tracking, Period-over-period changes |
| [Price Model Validation](#price-model-validation) | Price Methodology Testing | Multi-source price validation, Location-adjusted pricing |
| [Price Index](#price-index) | Market Movement Tracking | Weighted index calculation, Period-over-period comparisons |

&nbsp;
## Tools & Technologies<small>
- **SQL**: Complex data transformations and model creation
- **dbt**: Data transformation workflow management, testing, and documentation
- **Apache Airflow**: Workflow orchestration and automation
- **PostgreSQL**: Database management and query optimization
- **Power BI**: Interactive visualization and reporting
</small>

&nbsp;
## Projects in Detail

### <u>[Price Analysis](./models/Price%20Analysis)</u>
*Comprehensive daily price tracking and change analysis*

This project consolidates end-of-day price data with transaction logs to create a standardized view of price movements across different time periods. It serves as the organization's single source of truth for price dynamics, enabling consistent analysis of price movements across various timeframes.

#### Key Features
- Daily price consolidation (opening, closing, min, max)
- Period-over-period change calculations
- Price range validation and standardization
- Historical trend analysis

#### Technical Highlights
- Complex window functions for period comparisons
- Deduplication logic for price data
- Multiple timeframe calculations in single pass
- dbt transformations with documentation

#### Impact
- Standardized price calculations across the organization
- Single source of truth for transaction price dynamics
- Enhanced price movement analysis capabilities
- Improved decision-making through consistent price data

_One record in the final table represents the start, end, min, and maximum price of a particular product for a particular day and how that has changed from the previous day, start of the week (month, quarter, season, year), this time last week (month, quarter)_

***
---
&nbsp;

### <u>[Price Model Validation](./models/Price%20Model%20Validation)</u>
*Location-adjusted price validation system*

This project implements a validation framework for a new pricing methodology, combining multiple price sources to validate and adjust prices based on location-specific factors. The model determines transaction prices for each product backing an asset, using mid-point open prices when necessary, and adjusts values based on transportation factors.

#### Key Features
- Multi-source price integration
- Location-based price normalization
- Missing price interpolation using mid-point methodology
- Market fluidity validation

#### Technical Highlights
- Recursive CTEs for price chain calculation
- Weighted average computations
- Normalization procedures
- dbt models with extensive documentation

#### Impact
- Validated new pricing methodology
- Enabled location-specific price adjustments
- Provided framework for price simulation and testing
- Streamlined methodology adoption by backend team

_One record in the final table represents the price for a particular asset adjusted to its location based on the transportation factor_

***
---
&nbsp;

### <u>[Price Index](./models/Price%20Index)</u>
*Weighted index system for group price tracking*

This project creates a sophisticated price index system similar to equity indices, enabling tracking of price movements across product groups through a single value. The index derives its value from the total value of specified product groups, with each product assigned a specific weight.

#### Key Features
- Product group weighting system
- Base period reference implementation
- Yearly moving average volume calculation
- Automated access privilege management through dbt post-hooks

#### Technical Highlights
- Multi-period comparison calculations using window functions
- Complex percentage change computations
- Base period calculations
- dbt post-hook implementations for access control

#### Impact
- Simplified tracking of group price movements
- Enhanced market trend analysis capabilities
- Standardized index calculation methodology
- Improved market movement visibility

_One record in the final table represents the price index for a particular day and how that has changed from the previous day, start of the week (month, quarter, season, year), this time last week (month, quarter)_

***
---
&nbsp;

## Best Practices & Lessons Learned

1. **dbt Model Organization**
   - Clear model dependency management
   - Consistent naming conventions
   - Effective use of dbt documentation
   - Posthook for persistant access configuration in non-incremental model

2. **Performance Optimization**
   - Efficient use of CTEs for complex calculations
   - Strategic materialization choices
   - Optimized period-over-period calculations

&nbsp;
## Future Development Plans

1. **Model Enhancement**
   - Implement additional validation checks
   - Expand period-over-period analysis
   - Add more sophisticated weighting methodologies
   - Convert model to incremental models

2. **Documentation & Testing**
   - Enhance dbt test coverage
   - Expand model documentation
   - Add data quality checks

3. **Performance & Scalability**
   - Optimize large-scale calculations
   - Implement parallel processing where applicable
   - Enhance incremental processing logic

&nbsp;
## Contact

For questions about implementation details or collaboration opportunities, please reach out through the project repository or direct messages.