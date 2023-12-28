# Dockerfile
FROM python:3.9
WORKDIR /app

RUN ["mkfifo", "/pipe"]

# CMD ["python", "-u", "server.py", "-v"]
CMD ["python", "-u", "server.py"]

