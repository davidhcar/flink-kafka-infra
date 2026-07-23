#!/usr/bin/env bash
spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
i=0
{ sleep 2; echo "Hello!"; sleep 1; echo "World!"; } | {
    first_line=1
    while true; do
        if read -t 0.1 -r line; then
            if [ $first_line -eq 1 ]; then
                printf "\033[2K\r"
                first_line=0
            fi
            echo "$line"
        else
            ret=$?
            if [ $ret -gt 128 ]; then
                if [ $first_line -eq 1 ]; then
                    i=$(( (i+1) % 10 ))
                    printf "\033[2K\r${spin:$i:1} Running Flinkflow..."
                fi
            else
                break
            fi
        fi
    done
}
