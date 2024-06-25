FROM python

COPY . /predict

WORKDIR /predict

RUN pip install -r requirements.txt

ENTRYPOINT ["python","predict.py"]
