SciDAP - Scientific Data Analysis Platform 
==========================================

SciDAP enables you to reproduce experiments, share the data and results, choose your collaborators and organize your working environment.

To enable Common Workflow Language ([CWL](https://github.com/common-workflow-language/common-workflow-language)) on a workstation or dedicated hardware it was integrated into [Airflow](https://github.com/airbnb/airflow). Full airflow documentation available on http://pythonhosted.org/airflow/.


CWL Airflow integration
==========================================

Install from source
-------------------

* Airflow
```
git clone https://github.com/SciDAP/airflow.git
cd airflow
sudo python setup.py install
```
* SciDAP airflow modules
```
git clone https://github.com/SciDAP/scidap.git
cd modules
sudo python setup.py install
```

Example
-------
```Python
from scidap.cwldag import CWLDAG

...

default_args = {
    'owner': owner,
    'start_date': start_day,
    'email': [email],
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': end_day,
    'on_failure_callback': fail_callback
}

...

dag = CWLDAG(
    dag_id=dag_id,
    cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
    schedule_interval=timedelta(days=1),
    default_args=default_args)
dag.create()

```


CWL Airflow examples: https://github.com/SciDAP/workflows
