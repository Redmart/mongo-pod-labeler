FROM python:3.7.2-stretch

RUN pip install kubernetes pymongo

COPY ./mongo-labeler.py .

ENTRYPOINT [ "python" ]

CMD ["./mongo-labeler.py"]