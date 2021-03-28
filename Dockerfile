FROM continuumio/anaconda3

RUN mkdir /home/fable
RUN mkdir /home/fable/deps
COPY . /home/fable
WORKDIR /home/fable

# Prepare
RUN mkdir /usr/share/man/man1
RUN conda config --set changeps1 false 
# Install Java and other basic tools
RUN apt update && apt install -y wget \
    curl \
    openjdk-11-jdk \
    gcc g++ \
    net-tools


RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt install -y nodejs

# Install npm packages
RUN npm install chrome-remote-interface chrome-launcher
RUN npm install -g http-server

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt install -y ./google-chrome-stable_current_amd64.deb && rm google-chrome-stable_current_amd64.deb

# Install python dependencies
RUN pip install -r requirements.txt
# Install boilerpipe
RUN git clone https://github.com/misja/python-boilerpipe.git deps/python-boilerpipe
RUN pip install -e deps/python-boilerpipe

ENTRYPOINT /bin/sh -c /bin/bash

# To run: sudo docker run -e FABLE_CONFIG_KEYVAULT=1 -e FABLE_CONFIG_VAULTNAME=fabletestdockerkeyvault -e FABLE_CONFIG_SECRETNAME=fable-config --rm -it --name fable $IMAGE_NAME