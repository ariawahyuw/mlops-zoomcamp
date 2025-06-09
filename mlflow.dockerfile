
FROM python:3.12

RUN pip install mlflow==3.0.0rc0

EXPOSE 5000

CMD [ \
    "mlflow", "server", \
    "--backend-store-uri", "sqlite:///home/data/backend.db", \
    "--default-artifact-root", "/home/data/mlartifacts", \
    "--host", "0.0.0.0", \
    "--port", "5000" \
]