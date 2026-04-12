#!/bin/bash

LOG_FILE="$1"

gawk '
BEGIN {
    # Accumulators
    ts_sum=0; ts_count=0;
    traj_sum=0; traj_count=0;
    dens_sum=0; dens_count=0;
    si_sum=0; si_count=0;
    knn_sum=0; knn_count=0;
    prox_count=0;
    coll_count=0;
    conf_count=0;
    swarm_count=0;
    win_sum=0; win_count=0;
    thr_sum=0; thr_count=0;

    first_emit=-1; last_emit=-1;
}

# -------------------------------
# [TIMESTAMP]
# -------------------------------
/

\[TIMESTAMP\]

/ {
    match($0, /lag=([0-9]+)/, a)
    if (a[1] != "") {
        ts_sum += a[1]
        ts_count++
    }
}

# -------------------------------
# [TRAJECTORY]
# -------------------------------
/

\[TRAJECTORY\]

/ {
    match($0, /dt=([0-9]+)/, a)
    if (a[1] != "") {
        traj_sum += a[1]
        traj_count++
    }
}

# -------------------------------
# [DENSITY]
# -------------------------------
/

\[DENSITY\]

/ {
    match($0, /ms=([0-9]+)/, a)
    if (a[1] != "") {
        dens_sum += a[1]
        dens_count++
    }
}

# -------------------------------
# [SPATIAL INDEX]
# -------------------------------
/

\[SPATIAL INDEX\]

/ {
    match($0, /timeMs=([0-9.]+)/, a)
    if (a[1] != "") {
        si_sum += a[1]
        si_count++
    }
}

# -------------------------------
# [KNN]
# -------------------------------
/

\[KNN\]

/ {
    match($0, /timeMs=([0-9.]+)/, a)
    if (a[1] != "") {
        knn_sum += a[1]
        knn_count++
    }
}

# -------------------------------
# [PROXIMITY]
# -------------------------------
/

\[PROXIMITY\]

/ {
    prox_count++
}

# -------------------------------
# [COLLISION]
# -------------------------------
/

\[COLLISION\]

/ {
    coll_count++
}

# -------------------------------
# [CONFLICT]
# -------------------------------
/

\[CONFLICT\]

/ {
    conf_count++
}

# -------------------------------
# [SWARM]
# -------------------------------
/

\[SWARM\]

/ {
    swarm_count++
}

# -------------------------------
# [WINDOW RESULT]
# -------------------------------
/

\[WINDOW RESULT\]

/ {
    getline; # key
    getline; # windowStart
    getline; # windowEnd
    match($0, /([0-9]+)/, wend)
    windowEnd = wend[1]

    getline; # count
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

# -------------------------------
# [THROUGHPUT]
# -------------------------------
/

\[THROUGHPUT\]

/ {
    match($0, /eps=([0-9.]+)/, a)
    if (a[1] != "") {
        thr_sum += a[1]
        thr_count++
    }
}

END {
    avg_ts = (ts_count > 0) ? ts_sum/ts_count : 0
    avg_traj = (traj_count > 0) ? traj_sum/traj_count : 0
    avg_dens = (dens_count > 0) ? dens_sum/dens_count : 0
    avg_si = (si_count > 0) ? si_sum/si_count : 0
    avg_knn = (knn_count > 0) ? knn_sum/knn_count : 0
    avg_win = (win_count > 0) ? win_sum/win_count : 0
    avg_thr = (thr_count > 0) ? thr_sum/thr_count : 0

    dt = (last_emit - first_emit) / 1000.0
    throughput = (dt > 0) ? win_count / dt : 0

    printf("Event timestamp lag (ms): %.2f\n", avg_ts)
    printf("Trajectory update time (ms): %.2f\n", avg_traj)
    printf("Density estimator time (ms): %.2f\n", avg_dens)
    printf("Spatial index query time (ms): %.2f\n", avg_si)
    printf("KNN time (ms): %.2f\n", avg_knn)
    printf("Window latency (ms): %.2f\n", avg_win)
    printf("Throughput (windows/sec): %.2f\n", throughput)
    printf("Reported throughput (eps): %.2f\n", avg_thr)

    printf("Proximity detections: %d\n", prox_count)
    printf("Collision detections: %d\n", coll_count)
    printf("Conflict predictions: %d\n", conf_count)
    printf("Swarm clusters: %d\n", swarm_count)
}
' "$LOG_FILE"
