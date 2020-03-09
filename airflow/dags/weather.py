from datetime import timedelta

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from w3lib.url import add_or_replace_parameters


def get_weather(url_params):
    db_hook = SqliteHook(conn_name_attr='sqlite_default ')
    api_hook = HttpHook(http_conn_id='http_default', method='GET')

    url = add_or_replace_parameters(f'v1/history/daily', url_params)

    resp = api_hook.run(url)
    data = resp.json()['data']

    # usually I don't really care about this, but in case of big data, I guess it may be very useful
    del resp

    weather_insert = """
    insert or ignore into weather 
    (station_id, record_date, temperature, 
    temperature_min, temperature_max, winddirection, windspeed, sunshine, pressure) 
    values (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    for day in data:
        db_hook.run(
            weather_insert,
            parameters=(
                url_params['station'],
                day['date'],
                day['temperature'],
                day['temperature_min'],
                day['temperature_max'],
                day['winddirection'],
                day['windspeed'],
                day['sunshine'],
                day['pressure']
            )
        )


def process_weather(station, field_to_process):
    db_hook = SqliteHook(conn_name_attr='sqlite_default')

    weather_select = (
        f'select record_date, {field_to_process} '
        f'from weather where station_id={station} '
        f'order by record_date;'
    )

    data = db_hook.get_pandas_df(weather_select)
    average = data.rolling(3, center=True).mean().rename(columns={field_to_process: 'average'})
    data = data.merge(average, left_index=True, right_index=True)

    del average

    weather_update = """
    update weather
    set average = ?
    where station_id=? and record_date=?;
    """

    # iteration over data is used like a hack to avoid using either DataFrame.itertuples(), iteritems() or iterrows(),
    # which may be a bottleneck in case if number of rows is more then several thousands

    data.apply(
        lambda row: db_hook.run(weather_update, parameters=(row['average'], station, row['record_date'])),
        axis=1
    )


default_args = {
    'owner': 'Tarasen',
    'start_date': days_ago(1),
}

dag = DAG(dag_id='weather',
          default_args=default_args,
          schedule_interval='0 * * * *',
          dagrun_timeout=timedelta(seconds=5))

get_weather_task = PythonOperator(
    task_id='get_weather',
    provide_context=False,
    python_callable=get_weather,
    op_kwargs={
        'url_params':
            {
                'station': '72502',
                'start': '2020-01-01',
                'end': '2020-01-31',
                'key': 'Lj4ITfBD'
            },
    },
    dag=dag)

process_weather_task = PythonOperator(
    task_id='process_weather',
    provide_context=False,
    python_callable=process_weather,
    op_kwargs={
        'station': '72502',
        'field_to_process': 'pressure'
    },
    dag=dag)

process_weather_task.set_upstream(get_weather_task)
