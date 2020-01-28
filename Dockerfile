FROM nandanrao/pyspark-notebook

RUN pip install --user \
        orjson \
        clize

COPY . .

CMD ["python", "fetch.py"]
