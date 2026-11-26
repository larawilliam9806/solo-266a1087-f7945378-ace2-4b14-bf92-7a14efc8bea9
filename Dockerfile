FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dns_forwarder ./dns_forwarder
COPY mock_upstream ./mock_upstream

# Web 页面服务
EXPOSE 8000
# mock 上游的 DNS/控制口（compose 中按服务暴露）
EXPOSE 53/udp 8080

CMD ["uvicorn", "dns_forwarder.main:app", "--host", "0.0.0.0", "--port", "8000"]
