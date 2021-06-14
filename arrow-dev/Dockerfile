FROM dlang2/ldc-ubuntu:1.21.0 as ldc

RUN apt-get update && apt-get install -y clang-9 libclang-9-dev
RUN ln -s /usr/bin/clang-9 /usr/bin/clang
RUN apt-get install -y libglib2.0-dev
RUN apt-get install -y gobject-introspection

ADD ./arrow-glib /tmp/arrow-glib

COPY ./arrow-glib/arrow-glib.h /tmp/arrow.dpp

RUN DFLAGS="-L=-L/usr/lib/llvm-9/lib/" dub run dpp -- /tmp/arrow.dpp \
    --include-path /usr/include/glib-2.0 \
    --include-path /usr/lib/x86_64-linux-gnu/glib-2.0/include \
    --include-path /tmp/arrow-glib \
    --preprocess-only
