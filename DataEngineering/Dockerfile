FROM bitnami/spark

WORKDIR /opt/bitnami/spark/yelp

COPY ./requirements.txt .

ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/bitnami/spark/yelp/secrets/gcp_key.json"

RUN pip install -r requirements.txt

CMD ["/bin/bash","spark_submit.sh"]
