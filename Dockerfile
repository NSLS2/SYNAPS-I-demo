# if it complains about sertificates and if sudo powers, uncomment the two lines below in order to install the certificates. Otherwise, set the certificates while running the container . If no sudo powers, certificates might need to be added during "podman run .."

 
#FROM ubuntu:22.04 AS ca
#RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY pixi.toml pixi.lock* ./
RUN pixi install

COPY . .


CMD ["pixi", "run", "python", "receiver.py"]

