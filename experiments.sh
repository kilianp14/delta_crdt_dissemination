#!/bin/bash

config_files=("network_full.toml" "network_ring.toml" "network_super_nodes.toml" "network_sparse.toml")
update_intervals=(50 100 200)
fan_outs=("one" "log" "full")
strategies=("push" "pull" "pushpull")
experiments=1 #todo change this

# Compute total runs
total_runs=0
for config in "${config_files[@]}"; do
  for interval in "${update_intervals[@]}"; do
    for strat in "${strategies[@]}"; do
      if [ "$strat" == "push" ]; then
        fan_count=1
      else
        fan_count=${#fan_outs[@]}
      fi
      total_runs=$((total_runs + fan_count * experiments))
    done
  done
done
run=0

for config in "${config_files[@]}"; do
  for interval in "${update_intervals[@]}"; do
    for strat in "${strategies[@]}"; do
      if [ "$strat" == "push" ]; then
        current_fan_outs=("one")
      else
        current_fan_outs=("${fan_outs[@]}")
      fi

      for exp in $(seq 1 $experiments); do
        for fan in "${current_fan_outs[@]}"; do
          run=$((run+1))
          echo ">>> [$run/$total_runs] Experiment $exp | config=$config | interval=$interval | strategy=$strat | fan_out=$fan"

          CONFIG_FILE=$config \
          DATA_DIR="exp_${config%.toml}_${interval}_${strat}_${fan}_run$exp" \
          UPDATE_INTERVAL=$interval \
          DISSEMINATION_STRATEGY=$strat \
          FAN_OUT=$fan \
          docker compose up

          echo ">>> Finished [$run/$total_runs]"
          echo
        done
      done
    done
  done
done