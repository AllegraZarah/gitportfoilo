"""
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations
"""

from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnPairMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesBetweenQuartileLimitsByCategory(ColumnPairMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.between_quartile_limits_by_category"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        import numpy as np
        import pandas as pd
        
        # Create a dictionary to store value limits for each category
        value_limits = {}
    
        # Calculate quartiles and value limits for each category
        unique_categories = set(column_B)
        for category in unique_categories:
            # Filter data for the current category
            category_data = [value for value, cat_type in zip(column_A, column_B) if cat_type == category]
            
            # Calculate quartiles for the current category
            q1 = np.quantile(category_data, 0.25)
            q3 = np.quantile(category_data, 0.75)
            iqr = q3 - q1
            
            # Calculate value limits for the current category
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Store the value limits for the category
            value_limits[category] = (lower_bound, upper_bound)
            # df = pd.DataFrame(value_limits)

        result = pd.Series(column_A.index.map(lambda x: value_limits[column_B[x]][0] < column_A[x] < value_limits[column_B[x]][1]))

        return result == True

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
        import numpy as np
        import pandas as pd
        
        # Create a dictionary to store value limits for each category
        value_limits = {}
    
        # Calculate quartiles and value limits for each category
        unique_categories = set(column_B)
        for category in unique_categories:
            # Filter data for the current category
            category_data = [value for value, cat_type in zip(column_A, column_B) if cat_type == category]
            
            # Calculate quartiles for the current category
            q1 = np.quantile(category_data, 0.25)
            q3 = np.quantile(category_data, 0.75)
            iqr = q3 - q1
            
            # Calculate value limits for the current category
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Store the value limits for the category
            value_limits[category] = (lower_bound, upper_bound)
            # df = pd.DataFrame(value_limits)

        result = pd.Series(column_A.index.map(lambda x: value_limits[column_B[x]][0] < column_A[x] < value_limits[column_B[x]][1]))

        return result == True

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeBetweenQuartileLimitsByCategory(ColumnPairMapExpectation):
    """Expect values in this column to fall between the estimated upper quartile limit and lower quartile limit per specified categories as defined in a catgeory column."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "category": ['a', 'a', 'a', 'a', 'a', 'b', 'b', 'b', 'b', 'b'],
                "no_outlier": [15, 20, 25, 30, 35, 40, 42, 45, 48, 50],
                "both_with_outlier": [12, 15, 14, 13, 98, 22, 24, 100, 21, 25],
                "one_with_outlier": [20, 4, 2, 7, 6, 22, 24, 23, 21, 25]
            },
            "only_for": ["pandas", "postgresql"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "no_outlier",
                           "column_B": "category",
                           "mostly": 1.0},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "one_with_outlier",
                           "column_B": "category",
                           "mostly": 1.0},
                    "out": {"success": False},
                },
                {
                    "title": "basic_negative_test_2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "both_with_outlier",
                           "column_B": "category",
                           "mostly": 1.0},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.between_quartile_limits_by_category"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_A",
        "column_B",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeBetweenQuartileLimitsByCategory().print_diagnostic_checklist()