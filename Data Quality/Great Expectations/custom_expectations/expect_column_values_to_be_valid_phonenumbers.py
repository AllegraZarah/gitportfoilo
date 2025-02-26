"""
This is a template for creating custom ColumnPairMapExpectations.
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
class ColumnValuesToBeValidPhonenumbers(ColumnPairMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.to_be_valid_phonenumbers"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        import pandas as pd
        import phonenumbers
    
        def is_valid_phone_number(column_A, column_B):
            try:
                # Parse the phone number
                parsed_number = phonenumbers.parse(column_A, column_B)
                
                # Check if the number is valid for the specified region
                return phonenumbers.is_valid_number(parsed_number)
            except:
                # Handle invalid phone number format
                return False
        
        # Create a DataFrame with the phone numbers and country codes
        df = pd.DataFrame({'Phone Numbers': column_A, 'Country Code': column_B})
        
        # Apply the validation function to each row
        df['is_valid_phone'] = df.apply(lambda row: is_valid_phone_number(row['Phone Numbers'], row['Country Code']), axis=1)
        
        return df['is_valid_phone']

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
        import pandas as pd
        import phonenumbers
    
        def is_valid_phone_number(column_A, column_B):
            try:
                # Parse the phone number
                parsed_number = phonenumbers.parse(column_A, column_B)
                
                # Check if the number is valid for the specified region
                return phonenumbers.is_valid_number(parsed_number)
            except:
                # Handle invalid phone number format
                return False
        
        # Create a DataFrame with the phone numbers and country codes
        df = pd.DataFrame({'Phone Numbers': column_A, 'Country Code': column_B})
        
        # Apply the validation function to each row
        df['is_valid_phone'] = df.apply(lambda row: is_valid_phone_number(row['Phone Numbers'], row['Country Code']), axis=1)
        
        return df['is_valid_phone']
        

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidPhonenumbers(ColumnPairMapExpectation):
    """Expect values in the column to match the syntax of phonenumbers as defined by the python phonenumers library."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "phone1": ["+2348106006741", "+13019792738", "15025920945", "+447463281499",
                           "+447394540006", "8036518622", "07032035962", "+234 806 952 4204", "+44 7951 561560"],
                "phone2": ["+2348106006741", "+13019792738", "15025920945", "+447463281499",
                           "+4477463281499", "8036518622", "07032035962", "81060067741", "07951571569"],
                "country": ["NG", "US", "US", "UK",
                            "UK", "NG","NG", "NG", "UK"]
            },
            "only_for": ["pandas", "postgresql"],
            "tests": [
                {
                        "title": "basic_positive_test",
                        "exact_match_out": False,
                        "include_in_gallery": True,
                        "in": {"column_A": "phone1",
                               "column_B": "country",
                               "mostly": 1.0},
                        "out": {"success": True},
                    },
                    {
                        "title": "basic_negative_test",
                        "exact_match_out": False,
                        "include_in_gallery": True,
                        "in": {"column_A": "phone2",
                               "column_B": "country",
                               "mostly": 1.0},
                        "out": {"success": False},
                    },
            ]
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.to_be_valid_phonenumbers"

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
    ExpectColumnValuesToBeValidPhonenumbers().print_diagnostic_checklist()