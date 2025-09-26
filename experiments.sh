#!/bin/bash

config_files=("network_full.toml" "network_ring.toml" "network_chord.toml" "network_light.toml")
update_intervals=(50 100 200)
strategies=("push" "pull" "pushpull")
experiments=5

for config in "${config_files[@]}"; do
  for interval in "${update_intervals[@]}"; do
    for strat in "${strategies[@]}"; do
      for exp in $(seq 1 $experiments); do
        echo "=== Running experiment $exp with $config, interval=$interval, strategy=$strat ==="

        CONFIG_FILE=$config \
        DATA_DIR="exp_${config%.toml}_${interval}_${strat}_run$exp" \
        UPDATE_INTERVAL=$interval \
        DISSEMINATION_STRATEGY=$strat \
        docker compose up

        echo "=== Finished experiment $exp with $config, interval=$interval, strategy=$strat ==="
        echo
      done
    done
  done
done
