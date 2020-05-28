ARG ALPINE_VERSION=3.11

FROM elixir:1.10-alpine as init


# This step installs all the build tools we'll need
RUN apk update && \
  apk upgrade --no-cache && \
  apk add --no-cache \
  git \
  build-base \
  bash

RUN mix local.hex --force && mix local.rebar --force

# By convention, /opt is typically used for applications
WORKDIR /opt/app

