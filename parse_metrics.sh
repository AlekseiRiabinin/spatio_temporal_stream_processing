#!/bin/bash

LOG_FILE="$1"

gawk '
BEGIN {
    event_sum=0; event_count=0;
    wm_sum=0; wm_count=0;
    win_sum=0; win_count=0;
    first_emit=-1; last_emit=-1;
}

/\[TIMESTAMP\]/ {
    match($0, /lag=([0-9]+)/, a)
    if (a[1] != "") {
        event_sum += a[1]
        event_count++
    }
}

/\[WATERMARK\]/ {
    getline; # watermark
    match($0, /([0-9]+)/, w)
    watermark = w[1]

    getline; # maxTs (skip)

    getline; # nextWindowEnd (skip)

    getline; # currentTime
    match($0, /([0-9]+)/, c)
    current = c[1]

    if (watermark > 0 && current > 0) {
        wm_sum += (current - watermark)
        wm_count++
    }
}

/\[WINDOW RESULT\]/ {
    getline; # key (skip)

    getline; # windowStart (skip)

    getline; # windowEnd
    match($0, /([0-9]+)/, wend)
    windowEnd = wend[1]

    getline; # count (skip)

    getline; # timestamp
    match($0, /([0-9]+)/, t)
    emit = t[1]

    if (emit > 0 && windowEnd > 0) {
        win_sum += (emit - windowEnd)
        win_count++

        if (first_emit == -1) first_emit = emit
        last_emit = emit
    }
}

END {
    avg_event = (event_count > 0) ? event_sum/event_count : 0
    avg_wm    = (wm_count > 0) ? wm_sum/wm_count : 0
    avg_win   = (win_count > 0) ? win_sum/win_count : 0

    dt = (last_emit - first_emit) / 1000.0
    throughput = (dt > 0) ? win_count / dt : 0

    printf("Event latency (ms): %.2f\n", avg_event)
    printf("Watermark lag (ms): %.2f\n", avg_wm)
    printf("Window latency (ms): %.2f\n", avg_win)
    printf("Throughput (windows/sec): %.2f\n", throughput)
}
' "$LOG_FILE"
