FROM python:3.11.2-slim
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./app.py
COPY rf_model.pkl ./rf_model.pkl
COPY vectorizer_rf.pkl ./vectorizer_rf.pkl
CMD ["python", "app.py"]
