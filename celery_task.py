import pymssql
import pymysql
from threading import Thread
from threading import Semaphore
from celery.schedules import crontab
from celery import Celery


class Config():
    broker_url  = 'redis://127.0.0.1:6379' # redis 存储发送端来的任务队列
    result_backed = 'redis://127.0.0.1:6379' # 用redis存储执行端执行得到结果
    timezone = 'Asia/Shanghai' # 设置时间的时区  注意这里是上海

    beat_schedule = {
        'task1' : {
            'task':'celery_task.run',
            'schedule' : crontab(minute=0, hour=0)  # , day_of_week=[1, 2, 3, 4, 5]
        },

    }

celery_app  = Celery(__name__)
celery_app.config_from_object(Config)


def get_store():
    conn = pymssql.connect("", "", "", "")
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""
            select a.*,  b.hwbh as hw from sphwph a join huoweizl b on a.hw=b.hw
            where shl <>'0' 
    """)
    result = cursor.fetchall()
    for ret in result:
        for key, val in ret.items():
            ret[key] = str(val).strip().encode('latin-1').decode('gbk')
    return result


def insert_store(data_dict,sema):

    sema.acquire()
    print(data_dict)
    conn = pymysql.connect(host='',
                        user='',
                        password='1',
                        database='',
                        cursorclass=pymysql.cursors.DictCursor)
   

    with conn:
        with conn.cursor() as cursor:
            keys = ','.join(data_dict.keys())
            values = ','.join(['%s'] *len(data_dict))
            sql = 'INSERT INTO {table}({keys}) VALUES({values})'.format(table="ksoa_sphwph", keys=keys, values=values)
            cursor.execute(sql, tuple(data_dict.values()))
        conn.commit()
    sema.release()


# 开启同步任务celery -A celery_task.celery_app worker -l info -P gevent --logfile=celerylog.log
# 开启定时 celery -A celery_task.celery_app beat

# linux部署
# celery -A celery_task.celery_app worker -l info --beat --logfile=celerylog.log


@celery_app.task
def run():
    conn = pymysql.connect(host='',
                            user='',
                            password='',
                            database='',
                            cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    cursor.execute("delete from ksoa_sphwph")
    conn.commit()
    
    sema = Semaphore(10)
    data_list = get_store()
    thread_list = []
    for ret_dict in data_list:
        t = Thread(target=insert_store, args=(ret_dict,sema))
        t.start()
        thread_list.append(t)
    
    for t in thread_list:
        t.join()

    conn.close()
    cursor.close()

    return "任务执行完成！"


if __name__ == "__main__":
    run()
   
