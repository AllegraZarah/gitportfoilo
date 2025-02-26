import great_expectations as gx
import pandas as pd
import os
import json
import re
from datetime import datetime

class GXOperator:

    def __init__(self,
                data_context_root_dir="include/great_expectations",
                datasource_name = 'datateam_datasource'
                ) -> None:
        
        self.datasource_name = datasource_name
        self.context = gx.get_context(context_root_dir= data_context_root_dir)

    def run_expectations(self,
                        data_asset_name,
                        expectation_suite_name):
        
        datasource = self.context.get_datasource(self.datasource_name)
        batch_request = datasource.get_asset(data_asset_name).build_batch_request()
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name= expectation_suite_name
        )
        checkpoint_name = data_asset_name + 'checkpoint'
        checkpoint = self.context.add_or_update_checkpoint(
            name=checkpoint_name, validator=validator
        )
        checkpoint_result = checkpoint.run()
        result_json = checkpoint_result.to_json_dict()

        #persist json result into this folder
        folder_path = 'include/gx_json_results'
        # Check if the folder exists
        if not os.path.exists(folder_path):
            # If it doesn't exist, create the folder
            os.makedirs(folder_path)

        file_name = f'{folder_path}/{data_asset_name}_result.json'
        with open(file_name, "w") as json_file:
            json.dump(result_json, json_file)

        return result_json


    def _regex_expectations(self,st_point =  None, regex_col = None):

        result_set = {'expectation_count':0,
                    'expectation_count_true': 0}


        for ex_result in st_point :
            if ex_result['expectation_config']['expectation_type'] == "expect_column_values_to_match_regex" and ex_result['expectation_config']['kwargs']['column'] == regex_col and ex_result['success'] == True :
                result_set['expectation_count'] += 1
                result_set['expectation_count_true'] += 1
            elif ex_result['expectation_config']['expectation_type'] == "expect_column_values_to_match_regex" and ex_result['expectation_config']['kwargs']['column'] == regex_col and ex_result['success'] == False :
                result_set['expectation_count'] += 1
            else :
                pass
            
        return result_set['expectation_count'],result_set['expectation_count_true']

    def _count_expectations(self,st_point =  None, expectation_name = None):

        result_set = {'expectation_count':0,
                    'expectation_count_true': 0}


        for ex_result in st_point :
            if ex_result['expectation_config']['expectation_type'] == expectation_name and ex_result['success'] == True :
                result_set['expectation_count'] += 1
                result_set['expectation_count_true'] += 1
            elif ex_result['expectation_config']['expectation_type'] == expectation_name and ex_result['success'] == False:
                result_set['expectation_count'] += 1
            else :
                pass
            
        return result_set['expectation_count'],result_set['expectation_count_true']

    def _extract_expectation_results(self,data):
        ##extract the required columns from the json file 
        run_key  = list(data["run_results"].keys())[0]
        statistics = data["run_results"][run_key]['validation_result']['statistics']
        meta = data["run_results"][run_key]['validation_result']['meta']        
        st_point = data["run_results"][run_key]['validation_result']['results']

        not_null,not_null_true = self._count_expectations(st_point,expectation_name="expect_column_values_to_not_be_null")
        unique,unique_true = self._count_expectations(st_point,expectation_name="expect_column_values_to_be_unique")
        ## regex
        first_name,first_name_true = self._regex_expectations(st_point,regex_col="first_name")
        last_name,last_name_true = self._regex_expectations(st_point,regex_col="last_name")
        email,email_true = self._regex_expectations(st_point,regex_col="email")

        

        dataset_expectations = [[datetime.now(),
                                meta['batch_spec']['data_asset_name'],
                                meta['batch_spec']['table_name'],
                                meta['batch_spec']['schema_name'],
                                meta['expectation_suite_name'],
                                datetime.fromisoformat(meta['run_id']['run_time']).strftime('%Y-%m-%d:%H:%M:%S'),
                                round(statistics['success_percent'],3),
                                statistics['evaluated_expectations'],  
                                statistics['successful_expectations'],
                                statistics['unsuccessful_expectations'],
                                data['success'],
                                not_null,
                                not_null_true,
                                unique,
                                unique_true,
                                first_name,
                                first_name_true,
                                last_name,
                                last_name_true,
                                email,
                                email_true]] 
        ### respective column names for the data
        column = ['created',
                'data_asset_name',
                'table_name',
                'schema_name',
                'expectation_suite_name',
                'run_time',
                'success_percent',
                'evaluated_expectations',
                'successful_expectations',
                'unsuccessful_expectations',
                'success',
                "expectation_not_null",
                "expectation_not_null_true",
                "expectation_unique",
                "expectation_unique_true",
                "expectation_first_name",
                "expectation_first_name_true",
                "expectation_last_name",
                "expectation_last_name_true",
                "expectation_email",
                "expectation_email_true"]
        
        df = pd.DataFrame(dataset_expectations,columns=column)
        return df
    
    def expectations_to_db(self,checkpoint_jsonresult,source_engine,db_table =  'expectations_result',db_schema = 'greatexpectations'):
        extracted_resulf_df =  self._extract_expectation_results(data = checkpoint_jsonresult)
        extracted_resulf_df.to_sql(name = db_table,index = False,schema = db_schema,if_exists='append',con = source_engine)
