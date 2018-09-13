FROM python:3.7.0-alpine3.7

# PYTHONPATH に追加
ENV PYTHONPATH=/using-pandas:$PYTHONPATH

# パッケージのインストール
RUN apk add --no-cache \
    # mysqlclient
    musl-dev \
    gcc \
    mariadb-dev

# ライブラリをインストール
COPY ./requirements.txt /root
RUN pip install -r /root/requirements.txt
