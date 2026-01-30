FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY pixi.toml pixi.lock* ./
RUN pixi install

COPY . .

CMD ["pixi", "run", "python", "receiver.py"]

