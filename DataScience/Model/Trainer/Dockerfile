FROM python

COPY . /trainer

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "train.py"]
