FROM public.ecr.aws/lambda/python:3.12

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" /build \
    && rm -rf /build /root/.cache

WORKDIR ${LAMBDA_TASK_ROOT}

CMD ["growler.lambdas.metadata_entry.lambda_handler"]
