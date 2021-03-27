FROM ubuntu:21.04

WORKDIR /example

RUN  \
    apt update && \
    apt upgrade -y && \
    DEBIAN_FRONTEND="noninteractive" \
    apt-get -y install \
        tzdata \
        python3 \
        python3-pip \
        wget \
    && \
    python3 -m pip install \
        yfinance \
        pymongo \
        dnspython \
        requests \
        ta

COPY . /example

#ENTRYPOINT [ "/bin/bash" ]
#CMD [ "pipeline.sh" ]
