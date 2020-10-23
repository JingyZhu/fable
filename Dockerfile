FROM continuumio/anaconda3

# Prepare
RUN mkdir /usr/share/man/man1
RUN conda config --set changeps1 false 
# Install Java and other basic tools
RUN apt update && apt install -y wget \
    curl \
    openjdk-11-jdk \
    gcc g++


RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt install -y nodejs

# Install npm packages
RUN npm install chrome-remote-interface chrome-launcher
RUN npm install -g http-server

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt install -y ./google-chrome-stable_current_amd64.deb && rm google-chrome-stable_current_amd64.deb

ENTRYPOINT /bin/sh -c /bin/bash