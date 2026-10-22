# One image for both the API and the worker; Compose picks the command.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY profiles ./profiles

RUN useradd --system --uid 10001 oncall
USER oncall

EXPOSE 8000
CMD ["uvicorn", "oncall.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
