#!/usr/bin/env bash

REPO=restream/reindexer-py
USERNAME=reindexer-bot
CHECK_TIMEOUT=10

total_count=$(curl -s -H "Accept: application/vnd.github.v3+json" https://api.github.com/repos/$REPO/actions/workflows/test-specified-rx.yml/runs | jq '.total_count')

# workflow triggering
curl \
  -u "$USERNAME:$PYRX_GH_TOKEN" \
  -X POST \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/$REPO/actions/workflows/test-specified-rx.yml/dispatches \
  -d "{\"ref\":\"gh_actions\", \"inputs\":{\"rx_commit\":\"$GITHUB_SHA\"}}"


# find out workflow run id
for i in {0..30}; do
	all_runs="$(curl -s -H "Accept: application/vnd.github.v3+json" https://api.github.com/repos/$REPO/actions/workflows/test-specified-rx.yml/runs)"
	new_total_count=$(echo $all_runs | jq '.total_count')
	echo $new_total_count
	if [[ $new_total_count != $total_count ]]; then
		run_id=$(echo $all_runs | jq '.workflow_runs[-1].id')
		echo $run_id
		break
	fi
	sleep 1
done

if [[ -z "$run_id" ]]; then
    echo "Workflow wasn't created"
	exit 1
fi

# workflow monitoring
while true; do
  sleep ${CHECK_TIMEOUT}
	run="$(curl -s -H "Accept: application/vnd.github.v3+json" https://api.github.com/repos/$REPO/actions/runs/$run_id)"
	echo $run
	run_status="$(echo $run | jq '.status')"
	echo $run_status
	if [[ $run_status == 'completed' ]]; then
		run_conclusion="$(echo $run | jq '.conclusion')"
		echo $run_conclusion
		if [[ $run_conclusion == 'success' ]]; then
			echo "Success"
			exit 0
		else
			echo "$run_conclusion"
			exit 1
		fi
	else
		echo "$run_status"
	fi
done

exit 1
