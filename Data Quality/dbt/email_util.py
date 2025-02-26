from airflow.utils.email import send_email_smtp

def failure_email(context):
    subject = "[Airflow] DAG {0} Failed".format(context['task_instance_key_str'])

    html_content = """
                    DAG: {0}<br>
                    Task: {1}<br>
                    Failed at: {2}
                    """.format(context['task_instance'].dag_id, context['task_instance'].task_id, context['ts'])
    
    send_email_smtp(to=['xxx@custom.com'], subject=subject , html_content=html_content)