FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN apt-get update && apt-get upgrade -y && pip install --no-cache-dir -r requirements.txt
EXPOSE 8080

CMD ["fastapi", "run", "main.py", "--port", "8080"]
