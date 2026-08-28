FROM public.ecr.aws/lambda/python:3.12

COPY requirements-lambda.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r requirements-lambda.txt --target "${LAMBDA_TASK_ROOT}"

COPY api/main.py ${LAMBDA_TASK_ROOT}/api/main.py
COPY 01_pipelines/iceberg_catalog.py ${LAMBDA_TASK_ROOT}/iceberg_catalog.py

CMD ["api.main.handler"]