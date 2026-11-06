FROM ubuntu:24.04

# Everything needed to configure/build/test this project, installed once so
# it doesn't need to be re-installed every time a fresh container starts.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      git \
      gdb \
      vim \
      && rm -rf /var/lib/apt/lists/*

WORKDIR /work
CMD ["bash"]
