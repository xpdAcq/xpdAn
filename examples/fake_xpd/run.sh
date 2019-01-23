#!/usr/bin/env bash

#xterm -e "source activate dp_dev; bluesky-0MQ-proxy 5567 5568" &
xterm -e "source activate dp_dev; viz_server" &
source activate dp_dev; python analysis.py
