#!/usr/bin/env bash

REPO=restream/reindexer-py
USERNAME=reindexer-bot
CHECK_TIMEOUT=10

curl \
  -u "$USERNAME:$PYRX_GH_TOKEN" \
  -X POST \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/$REPO/actions/workflows/test-specified-rx.yml/dispatches \
  -d "{\"ref\":\"gh_actions\", \"inputs\":{\"rx_commit\":\"$GITHUB_SHA\"}}"

sleep $CHECK_TIMEOUT

curl \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/$REPO/actions/workflows/test-specified-rx.yml
