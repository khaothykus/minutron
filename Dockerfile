FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instala dependências do sistema, Firefox ESR e geckodriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-dejavu \
    fonts-liberation \
    fonts-freefont-ttf \
    libicu-dev \
    firefox-esr \
    wget \
    gnupg \
    ca-certificates \
    bzip2 \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libxt6 \
    libx11-xcb1 \
    libasound2 \
    libnss3 \
    libxss1 \
    libxrandr2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libpango-1.0-0 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    **libicu-dev** \
    && rm -rf /var/lib/apt/lists/*

# Instala geckodriver versão fixa (v0.33.0)
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz \
    && tar -xzf geckodriver-v0.33.0-linux64.tar.gz -C /usr/local/bin \
    && rm geckodriver-v0.33.0-linux64.tar.gz

# Copia e instala dependências Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia código da aplicação
#COPY app/ /app/

# Garante estrutura de dados
RUN mkdir -p /app/data/users /app/data/logs

CMD ["python", "bot.py"]
