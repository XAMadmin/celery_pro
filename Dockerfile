FROM python:3-slim

ADD ./  /code
WORKDIR /code

RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple/
RUN pip3 config set install.trusted-host pypi.tuna.tsinghua.edu.cn
RUN pip3 install -r requirement.txt

# 时区设置
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
&& echo 'Asia/Shanghai' >/etc/timezone

# celery -A celery_task.celery_app worker -l info --beat --logfile=celerylog.log
ENTRYPOINT ["celery", "-A", "celery_task.celery_app", "worker", "-l", "info", "--beat", "--logfile=celerylog.log"]
