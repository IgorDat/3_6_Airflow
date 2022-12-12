from datetime import datetime
import psycopg2
import pandas as pd
from airflow.models.xcom import XCom

import random
import os.path

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    conn_to_airflow = BaseHook.get_connection(conn_id)
    return conn_to_airflow

def hello():
    print("Hello!")

def connect_to_psql(**kwargs):
    ti = kwargs['ti']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port,\
                                                             conn_to_airflow.login, conn_to_airflow.password,\
                                                                 conn_to_airflow.schema
    
    ti.xcom_push(value = [pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='conn_to_airflow')
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    # conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = pg_conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, num integer, data varchar);")
    cursor.execute("INSERT INTO test_table (num, data) VALUES (%s, %s)",(100, "abc'def"))
    
    #cursor.fetchall()
    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def read_from_psql(**kwargs):
    ti = kwargs['ti']
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = ti.xcom_pull(key='conn_to_airflow', task_ids='conn_to_psql')

    # pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port,\
    #                                                          pg_conn.login, pg_conn.password, pg_conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = pg_conn.cursor()

    cursor.execute("SELECT * FROM test_table;")
    print(cursor.fetchone())
    
    cursor.close()
    pg_conn.close()

def python_branch(**kwargs):
    ti = kwargs['ti']

    creds = ti.xcom_pull(key='conn_to_airflow', task_ids='conn_to_psql')
    if len(creds) == 5:
        return "accurate"
    else:
        return "inaccurate"
#загрузите файл и передайте количество строк в xcom        
def openfile(**kwargs):
    ti = kwargs['ti']
    fp=Variable.get("file_path")
    # read the csv file
    df=pd.read_csv(fp)
       
    # display dataset
    print(len(df))

    ti.xcom_push("return_value", len(df))

#a.      Создайте еще один PythonOperator, который генерирует два произвольных числа и печатает их. Добавьте вызов нового оператора в конец вашего pipeline с помощью >>.    
def twonumbers(**kwargs):
    ti = kwargs['ti']
    first_n=random.random()
    sec_n=random.random()
    print ('Случайные числа: ', first_n,' ', sec_n)

    ti.xcom_push("num_A", first_n)
    ti.xcom_push("num_B", sec_n)
    #, sec_n)

#c.      Если запуск произошел успешно, попробуйте изменить логику вашего Python-оператора следующим образом – 
#сгенерированные числа должны записываться в текстовый файл – через пробел.
 #При этом должно соблюдаться условие, что каждые новые два числа должны записываться с новой строки не затирая предыдущие. 

def twonumbers_to_file(**kwargs):
    ti = kwargs['ti']
   
    #выталкиваем из стека Xcomm по ключу
    first_n=ti.xcom_pull(task_ids='two_numbers', key='num_A')
    sec_n=ti.xcom_pull(task_ids='two_numbers', key='num_B')
    print ('Случайные числа в файл: ',' ', first_n, sec_n)

#e.      Измените еще раз логику вашего оператора из пунктов 12.а – 12.с.
# При каждой новой записи произвольных чисел в конец файла, вычисленная сумма на шаге 12.d должна затираться.
    # df2 = pd.DataFrame({"num_A":[first_n], 'num_B': [sec_n]})
    #if os.path.isfile(fp):
    #без последней строки, header=0 если заголовки  в первой строке
    #  df=pandas.read_csv(fp, header=0)[:-1]
    #  df = df.append(df2, ignore_index = True )
    #else:
    #  df=df2  
    
    
    #create DataFrame следующую строку в случае задания e. надо закомментировать, а всё, что выше раскомментировать
    # и вместо условия о существовании файла просто добавить  df.to_csv(fp, mode='w', index= False , header= True)
    df = pd.DataFrame({"num_A":[first_n], 'num_B': [sec_n]})
    #view DataFrame
    df
    fp=Variable.get("file_nums_path")
    #проверяем существование файла
    if os.path.isfile(fp):
      df.to_csv(fp, mode='a', index= False , header= False)
    else:
      df.to_csv(fp, mode='w', index= False , header= True)  



#d. Создайте новый оператор, который подключается к файлу и вычисляет сумму всех чисел из первой колонки, затем сумму всех чисел из второй колонки
#   и рассчитывает разность полученных сумм. Вычисленную разность необходимо записать в конец того же файла, не затирая его содержимого.
def summ_to_file():
    
    #считываем из вариэйблс путь
    fp=Variable.get("file_nums_path")
    
    #проверяем существование файла
    if os.path.isfile(fp):
      df = pd.read_csv(fp, delimiter=',')
    else:
      #create DataFrame
      df = pd.DataFrame({"num_A":[0], 'num_B': [0]})
      #view DataFrame
      df 
      df.to_csv(fp, mode='w', index= False , header= True)
    #вычисляем сумму первой и второй колонок и заодно их разность
    sum1col = df['num_A'].sum()
    sum2col = df['num_B'].sum()
    difcol = sum1col - sum2col
    #добавим колонку в пандас , содержащую разность
    df=df.insert(2, "Diff", difcol)
    #перезапишем файл
    #почему-то не работает, может файл занят?
    #df.to_csv(fp, mode='w', index= False, header= True)
   
    # просто допишем разность в первую колонку в новую строку
    df1 = pd.DataFrame({"num_A":[difcol], 'num_B': [0]})
    df1.to_csv(fp, mode='a', index= False, header= False)



# A DAG represents a workflow, a collection of tasks
#f.       Настройте ваш DAG таким образом, чтобы он запускался каждую минуту в течение 5 минут, т.е. максимум 6 раз, каждую минуту, т.е. все звезды надо поставить 
with DAG(dag_id="first_dag", start_date=datetime(2022, 1, 1), max_active_runs=6, schedule="* * * * *") as dag:
    
    accurate = DummyOperator(
        task_id = 'accurate'
    )
    inaccurate = DummyOperator(
        task_id = 'inaccurate'
    )

    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)
    python_task1 = PythonOperator(task_id="file_open", python_callable = openfile, do_xcom_push=True)
    python_task2 = PythonOperator(task_id="two_numbers", python_callable = twonumbers, do_xcom_push=True)
    python_task3 = PythonOperator(task_id="two_numbers_to_file", python_callable = twonumbers_to_file, do_xcom_push=True)
    python_task4 = PythonOperator(task_id="summ_to_file", python_callable = summ_to_file, do_xcom_push=True)

    conn_to_psql_tsk = PythonOperator(task_id="conn_to_psql", python_callable = connect_to_psql)
    read_from_psql_tsk = PythonOperator(task_id="read_from_psql", python_callable = read_from_psql)

    sql_sensor = SqlSensor(
            task_id='sql_sensor_check',
            poke_interval=60,
            timeout=180,
            soft_fail=False,
            retries=2,
            sql="select count(*) from test_table",
            conn_id=Variable.get("conn_id"),
        dag=dag)

    bash_task2 = BashOperator(task_id="bye", bash_command="echo bye, baby, bye", do_xcom_push=False)
    
    choose_best_model = BranchPythonOperator(
        task_id = 'branch_oper',
        python_callable = python_branch,
        do_xcom_push = False
    )

    # Set dependencies between tasks
    bash_task >> python_task >> python_task1 >> python_task2 >> python_task3 >> python_task4 >> conn_to_psql_tsk >> read_from_psql_tsk >> sql_sensor \
        >> bash_task2 >> choose_best_model >> [accurate, inaccurate] 
