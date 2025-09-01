#!/bin/bash

while getopts "v:i:" opt; do
  case "$opt" in
    v) version="$OPTARG" ;;
    i) image="$OPTARG" ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
  esac
done

command="true"
command="$command && podman pull bellsoft/liberica-runtime-container:jre-24-musl "
command="$command && podman build -f Dockerfile -t $image:$version . "
command="$command && podman push $image:$version "

echo "Executing command: $command"
eval "$command"
