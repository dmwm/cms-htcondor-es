#!/bin/bash
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>

for i in $(pgrep -u 492 -f "python spider_cms.py"); do
    TIME=$(ps --no-headers -o etimes "$i")
    if [ "$TIME" -ge 3600 ]; then
        ps --no-headers --sort=start_time -o pid,ppid,user,lstart,etime,rss,drs,pmem,cmd "$i"
        read -p "kill? " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            kill -KILL "$i"
        fi
    fi
done
