from airflow import DAG
import sys

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

sys.path.append("../")

default_arg = {'owner': 'airflow', 'start_date': '2020-12-10'}


def check_prodId_access_func(**kwargs):
    ti = kwargs['ti']
    has_access = ti.xcom_pull(task_ids='check_prodId_access', key='prodId_access')

    if has_access == 1:
        return 'get_cur_MQ_depth_max_MQ_depth'
    else:
        return 'L1_escalation'

def is_MQ_depth_below_threshold_func(**kwargs):
    ti = kwargs['ti']
    current_mq_depth = ti.xcom_pull(task_ids='check_prodId_access', key='prodId_access')
    threshold_mq_depth = ti.xcom_pull(task_ids='check_prodId_access', key='prodId_access')

    if current_mq_depth < threshold_mq_depth:
        return 'resolution_module'
    else:
        return ''

def is_IPcmdb_ACTION_func(**kwargs):
    ti = kwargs['ti']
    action = ti.xcom_pull(task_ids='IPcmdb_ACTION', key='IPcmdb_ACTION')

    if action == 'ESCALATE':
        return 'L2_escalation'
    elif action == 'REPLY':
        return 'dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir'
    elif action == 'DUMP':
        return 'check_bailout_message_count_has_reached'
    elif action == 'MONITOR':
        return 'monitor_for_specific_time_based_on_freq'

def compare_each_msgid_w_files_in_that_dir_func(**kwargs):
    ti = kwargs['ti']
    msg_ids = ti.xcom_pull(task_ids='dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir', key='msg_id')

    # todo based on values it returns
    is_maching = True
    if is_maching:
        return 'L2_escalation'
    else:
        return 'replay_err_msgs_err_q_to_process'

def compare_each_msgid_w_files_in_that_dir2_func(**kwargs):
    ti = kwargs['ti']
    msg_ids = ti.xcom_pull(task_ids='dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir2', key='msg_id')

    # todo based on values it returns
    is_maching = True
    if is_maching:
        return 'L2_escalation'
    else:
        return 'replay_err_msgs_err_q_to_process'

def check_backout_msgs_func(**kwargs):
    ti = kwargs['ti']
    msg_ids = ti.xcom_pull(task_ids='# todo', key='msg_id')

    backout_message_result = True

    if backout_message_result:
        return 'dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir'
    else:
        return 'resolution_module'

def check_bailout_message_count_has_reached_func(**kwargs):
    ti = kwargs['ti']
    count = ti.xcom_pull(task_ids='# todo may be new tas kto get data', key='disk_space')

    if count > 0:  # todo fix numbers
        return 'L2_escalation'
    else:
        return 'check_disk_space'

def is_disk_space_func(**kwargs):

    ti = kwargs['ti']
    disk_space = ti.xcom_pull(task_ids='check_disk_space', key='disk_space')
    destroy = True # todo get this value

    if disk_space > 80:
        return 'L2_escalation'
    elif destroy:
        return 'dump_and_destroy_a_copy_of_msgs_from_QUEUE'
    else:
        return 'dump_a_copy_of_msgs_from_QUEUE'




dag = DAG(
    'mq',
    default_args=default_arg,
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

own_sock_alert = BashOperator(
    dag=dag,
    task_id='own_sock_alert',
    bash_command='echo 1',
)

fetch_alert_details = BashOperator(
    dag=dag,
    task_id='fetch_alert_details',
    bash_command='echo 2',
)

fetch_config_details_from_ipcmdb = BashOperator(
    dag=dag,
    task_id='fetch_config_details_from_ipcmdb',
    bash_command='echo 2',
)

#############   PSW

check_prodId_access = BashOperator(  # maybe we dont need this step if fetch_config_details_from_ipcmdb task provide access details. So xcom_push can be done there.
    dag=dag,
    task_id='check_prodId_access',
    # bash_command='echo "{{ ti.xcom_push(key="k1", value="v1") }}" "{{ti.xcom_push(key="k2", value="v2") }}"',
    bash_command='echo "{{ ti.xcom_push(key="prodId_access", value=" #todo later set this from script") }}"',
)

BRANCH_check_prodId_access = BranchPythonOperator(
    task_id='BRANCH_check_prodId_access',
    dag=dag,
    python_callable=check_prodId_access_func,
)

get_cur_MQ_depth_max_MQ_depth = BashOperator(
    dag=dag,
    task_id='get_cur_MQ_depth_max_MQ_depth',
    bash_command='echo "{{ ti.xcom_push(key="current_mq_depth", value="#todo") }}" "{{ti.xcom_push(key="threshold_mq_depth", value="#todo") }}"',
)

#############

L1_escalation = BashOperator(
    dag=dag,
    task_id='L1_escalation',
    bash_command='echo 2',
)

BRANCH_is_MQ_depth_below_threshold = BranchPythonOperator(
    task_id='BRANCH_is_MQ_depth_below_threshold',
    dag=dag,
    python_callable=is_MQ_depth_below_threshold_func,
)

resolution_module = BashOperator(
    dag=dag,
    task_id='resolution_module',
    bash_command='echo 2',
)
############# ACTIONS


IPcmdb_ACTION = BashOperator(  # Not sure if the cvalue of ACTION can be taken from task fetch_config_details_from_ipcmdb ??
    dag=dag,
    task_id='IPcmdb_ACTION',
    bash_command='echo "{{ ti.xcom_push(key="IPcmdb_ACTION", value=" #todo later set this from script") }}"',
)

BRANCH_ipcmdb_action = BranchPythonOperator(
    task_id='BRANCH_ipcmdb_action',
    dag=dag,
    python_callable=is_IPcmdb_ACTION_func,
)

## ESCALATE

L2_escalation = BashOperator(
    dag=dag,
    task_id='L2_escalation',
    bash_command='echo 2',
)

## MONITOR

monitor_for_specific_time_based_on_freq = BashOperator(
    dag=dag,
    task_id='monitor_for_specific_time_based_on_freq',
    bash_command='echo 2',
)

## REPLY

dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir = BashOperator(
    dag=dag,
    task_id='dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir',
    bash_command='echo "{{ ti.xcom_push(key="msg_id", value=" #todo later set this from script") }}"',
)

dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir2 = BashOperator(
    dag=dag,
    task_id='dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir2',
    bash_command='echo "{{ ti.xcom_push(key="msg_id", value=" #todo later set this from script") }}"',
)

BRANCH_compare_each_msgid_w_files_in_that_dir = BranchPythonOperator(
    task_id='BRANCH_compare_each_msgid_w_files_in_that_dir',
    dag=dag,
    python_callable=compare_each_msgid_w_files_in_that_dir_func,
)

BRANCH_compare_each_msgid_w_files_in_that_dir2 = BranchPythonOperator(
    task_id='BRANCH_compare_each_msgid_w_files_in_that_dir2',
    dag=dag,
    python_callable=compare_each_msgid_w_files_in_that_dir2_func,
)

replay_err_msgs_err_q_to_process = BashOperator(
    dag=dag,
    task_id='replay_err_msgs_err_q_to_process',
    bash_command='echo 2',
)

wait_for_configured_time = BashOperator(
    dag=dag,
    task_id='wait_for_configured_time',
    bash_command='echo 2',
)

BRANCH_check_backout_msgs = BranchPythonOperator(
    task_id='BRANCH_check_backout_msgs',
    dag=dag,
    python_callable=check_backout_msgs_func,
)


## DUMP

BRANCH_check_bailout_message_count_has_reached = BranchPythonOperator(
    dag=dag,
    task_id='BRANCH_check_bailout_message_count_has_reached',
    python_callable=check_bailout_message_count_has_reached_func,
)

check_disk_space = BashOperator(
    dag=dag,
    task_id='check_disk_space',
    bash_command='echo "{{ ti.xcom_push(key="disk_space", value=" #todo later set this from script") }}"',
)

BRANCH_is_disk_space = BranchPythonOperator(
    dag=dag,
    task_id='BRANCH_is_disk_space',
    python_callable=is_disk_space_func,
)

dump_and_destroy_a_copy_of_msgs_from_QUEUE = BashOperator(
    dag=dag,
    task_id='dump_and_destroy_a_copy_of_msgs_from_QUEUE',
    bash_command='echo 2',
)

dump_a_copy_of_msgs_from_QUEUE = BashOperator(
    dag=dag,
    task_id='dump_a_copy_of_msgs_from_QUEUE',
    bash_command='echo 2',
)

notification_to_dump_loc = BashOperator(
    dag=dag,
    task_id='notification_to_dump_loc',
    bash_command='echo 2',
)

STOP = DummyOperator(
    dag=dag,
    task_id='stop',
)


own_sock_alert >> fetch_alert_details >> fetch_config_details_from_ipcmdb >> check_prodId_access >> BRANCH_check_prodId_access
BRANCH_check_prodId_access >> L1_escalation >> STOP
BRANCH_check_prodId_access >> get_cur_MQ_depth_max_MQ_depth >> BRANCH_is_MQ_depth_below_threshold

BRANCH_is_MQ_depth_below_threshold >> resolution_module >> STOP
BRANCH_is_MQ_depth_below_threshold >> IPcmdb_ACTION >> BRANCH_ipcmdb_action

# ESCALATE
BRANCH_ipcmdb_action >> L2_escalation >> STOP

# REPLY
BRANCH_ipcmdb_action >> dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir >> BRANCH_compare_each_msgid_w_files_in_that_dir

BRANCH_compare_each_msgid_w_files_in_that_dir >> replay_err_msgs_err_q_to_process >> wait_for_configured_time >> BRANCH_check_backout_msgs
# BRANCH_check_backout_msgs >> dump_err_msgs_to_file_and_fetch_msgid_from_dump_dir2 >> BRANCH_compare_each_msgid_w_files_in_that_dir2  ## NOT ACYCLIC
BRANCH_check_backout_msgs >> resolution_module >> STOP

# # MONITOR
# BRANCH_ipcmdb_action >> monitor_for_specific_time_based_on_freq >> BRANCH_is_MQ_depth_below_threshold
# BRANCH_is_MQ_depth_below_threshold >> L2_escalation >> STOP
# BRANCH_is_MQ_depth_below_threshold >> resolution_module >> STOP
#
# # DUMP
# BRANCH_ipcmdb_action >> BRANCH_check_bailout_message_count_has_reached
# BRANCH_check_bailout_message_count_has_reached >> L2_escalation >> STOP
# BRANCH_check_bailout_message_count_has_reached >> BRANCH_is_disk_space
# BRANCH_is_disk_space >> L2_escalation >> STOP
# BRANCH_is_disk_space >> dump_and_destroy_a_copy_of_msgs_from_QUEUE >> notification_to_dump_loc >> BRANCH_is_MQ_depth_below_threshold
# BRANCH_is_disk_space >> dump_a_copy_of_msgs_from_QUEUE

