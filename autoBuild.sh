#!/bin/bash
# filename="$1"

# m1=$(md5sum "$filename")

while true; do

  # md5sum is computationally expensive, so check only once every 10 seconds
  sleep 3

  # m2=$(md5sum "$filename")

  # if [ "$m1" != "$m2" ] ; then
  #   echo "ERROR: File has changed!" >&2 
  #   exit 1
  # fi
  changed=false
  for file in src/*
  do
      sum1="$(md5sum "$file")"
      sleep 2
      sum2="$(md5sum "$file")"
      if [ "$sum1" = "$sum2" ];
      then
          let $changed=false
      else
          let $changed=true
          echo "## " $file "changed ##"
      fi
  done
  if [ "$changed" == true ] ; then
    echo "### Building docker ###"
    docker build --pull --rm -f "Dockerfile" -t mongometadatamonitor:0.0.1 "."
  fi
done