FROM python:3.7.0-alpine3.7

# PYTHONPATH に追加
ENV PYTHONPATH=/csv_to_db:$PYTHONPATH

# パッケージのインストール
RUN apk add --no-cache \
    # PyMySQL
    linux-headers \
    gcc \
    g++ \
    libffi-dev \
    openssl-dev

# ライブラリをインストール
COPY ./requirements.txt /root
RUN pip install -r /root/requirements.txt
