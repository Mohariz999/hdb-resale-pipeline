FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

# The base image ships a Yarn apt repo whose signing key is no longer valid, which
# makes apt-get update fail (and with it the docker-in-docker feature install).
# This project doesn't use Yarn, so drop the repo.
RUN grep -rl dl.yarnpkg.com /etc/apt/sources.list.d/ | xargs -r rm -f
