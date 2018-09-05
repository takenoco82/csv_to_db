FROM python:3.7

# PYTHONPATH に追加
ENV PYTHONPATH=/using-pandas:$PYTHONPATH

# ライブラリをインストール
COPY ./requirements.txt /root
RUN pip install -r /root/requirements.txt
