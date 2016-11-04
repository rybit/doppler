FROM calavera/go-glide:v0.12.2

ADD . /go/src/github.com/rybit/doppler

RUN useradd -m netlify && \
        cd /go/src/github.com/rybit/doppler && \
        make deps build && \
        mv doppler /usr/local/bin/

USER netlify
CMD ["doppler"]
