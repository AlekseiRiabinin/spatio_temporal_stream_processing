CREATE TABLE IF NOT EXISTS public.athlete_telemetry (
    id                                       BIGSERIAL PRIMARY KEY,
    record_id                                TEXT             NOT NULL,
    sample_idx                               BIGINT           NOT NULL,
    time_sec                                 DOUBLE PRECISION NOT NULL,
    wallclock_sec                            DOUBLE PRECISION,          -- sample_times_for_all_signals_apart_from_ecg
    is_r_peak                                BOOLEAN          NOT NULL DEFAULT FALSE,
    chest_ecg                                REAL,
    wrist_ppg                                REAL,
    wrist_gyro_x                             REAL,
    wrist_gyro_y                             REAL,
    wrist_gyro_z                             REAL,
    wrist_low_noise_accelerometer_x          REAL,
    wrist_low_noise_accelerometer_y          REAL,
    wrist_low_noise_accelerometer_z          REAL,
    wrist_wide_range_accelerometer_x         REAL,
    wrist_wide_range_accelerometer_y         REAL,
    wrist_wide_range_accelerometer_z         REAL,
    wrist_mag_x                              REAL,
    wrist_mag_y                              REAL,
    wrist_mag_z                              REAL
);

CREATE INDEX IF NOT EXISTS idx_athlete_telemetry_record_sample
    ON public.athlete_telemetry (record_id, sample_idx);
CREATE INDEX IF NOT EXISTS idx_athlete_telemetry_id
    ON public.athlete_telemetry (id);
