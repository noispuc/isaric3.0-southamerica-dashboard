# Dockerfile
FROM python:3.11-slim
WORKDIR /app

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install --upgrade pip setuptools wheel

# leve apenas o requirements para cachear melhor
COPY requirements.txt ./
RUN pip install -r requirements.txt

# agora o código
COPY . .

EXPOSE 8050

CMD ["gunicorn", "--workers", "1","--reload", "--bind", "0.0.0.0:8050", "vertex.descriptive_dashboard:server"]
