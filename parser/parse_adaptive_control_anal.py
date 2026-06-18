#!/usr/bin/env python3

import re
from pathlib import Path
from typing import Optional, Dict

import pandas as pd


LOG_DIR = Path("../logs")
OUTPUT_DIR = Path("../data/article_4/parsed")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRIC_PATTERN = re.compile(r"(\w+)=([^\s]+)")

# All valid values from experimental matrix
PROFILES = {
    "realtime",
    "skewed",
    "late",
    "out_of_order",
    "mixed"
}

RATES = {
    "constant",
    "burst",
    "wave"
}

MOTIONS = {
    "straight",
    "random_walk",
    "swarm",
    "collision",
    "corridor"
}

MODES = {
    "fixed",
    "adaptive"
}


def parse_filename(log_file: Path) -> Optional[Dict]:
    """
    Parse log filename with all supported formats:
    
    Format: {mode}_{profile}_{rate}_{motion}_{window}_{watermark}_{timestamp}.log
    
    Examples:
    - fixed_realtime_constant_straight_1000_500_20260603_203825.log
    - adaptive_skewed_wave_corridor_10000_5000_20260609_213353.log
    - fixed_late_burst_collision_5000_5000_20260604_001620.log
    - adaptive_out_of_order_wave_collision_5000_3000_20260604_211629.log
    - fixed_mixed_burst_random_walk_5000_5000_20260604_001620.log
    
    Also supports legacy format (without mode prefix):
    - realtime_constant_straight_1000_500_20260603_203825.log
    """
    name = log_file.stem
    parts = name.split("_")
    
    # Need at least: profile + rate + motion + window + watermark + date + time
    if len(parts) < 7:
        return None
    
    try:
        date_part = parts[-2]
        time_part = parts[-1]
        watermark_delay_ms = int(parts[-3])
        window_size_ms = int(parts[-4])
    except ValueError:
        return None
    
    # Determine if mode is present (first part is 'fixed' or 'adaptive')
    mode = None
    start_idx = 0
    
    if parts[0] in MODES:
        mode = parts[0]
        start_idx = 1
    
    experiment_parts = parts[start_idx:-4]
    experiment = "_".join(experiment_parts)
    
    profile = None
    rate_pattern = None
    motion_mode = None
    
    # Find rate pattern first (it's the most distinct marker)
    for rate in RATES:
        marker = f"_{rate}_"
        if marker in experiment:
            left, right = experiment.split(marker, 1)
            
            # Left part should be profile
            if left in PROFILES:
                profile = left
            else:
                # Try to extract profile from left part
                for prof in PROFILES:
                    if left == prof or left.endswith(prof):
                        profile = prof
                        break
            
            rate_pattern = rate
            
            # Right part should be motion mode
            if right in MOTIONS:
                motion_mode = right
            else:
                # Try to extract motion from right part
                for motion in MOTIONS:
                    if right == motion or right.startswith(motion):
                        motion_mode = motion
                        break
            
            break
    
    # If not found with marker approach, try direct parsing
    if profile is None or rate_pattern is None or motion_mode is None:
        # Try parsing as: profile_rate_motion
        for prof in PROFILES:
            if experiment.startswith(prof):
                profile = prof
                remaining = experiment[len(prof):].lstrip("_")
                
                for rate in RATES:
                    if remaining.startswith(rate):
                        rate_pattern = rate
                        motion_part = remaining[len(rate):].lstrip("_")
                        
                        for motion in MOTIONS:
                            if motion_part.startswith(motion):
                                motion_mode = motion
                                break
                        break
                break
    
    if profile is None or rate_pattern is None or motion_mode is None:
        return None
    
    return {
        "mode": mode if mode else "unknown",
        "profile": profile,
        "rate_pattern": rate_pattern,
        "motion_mode": motion_mode,
        "window_size_ms": window_size_ms,
        "watermark_delay_ms": watermark_delay_ms,
        "run_timestamp": f"{date_part}_{time_part}"
    }


def parse_metric_line(line: str) -> Dict:
    """Parse a single METRIC line from logs."""
    matches = METRIC_PATTERN.findall(line)
    row = {}
    
    for key, value in matches:
        try:
            if "." in value:
                row[key] = float(value)
            else:
                row[key] = int(value)
        except ValueError:
            row[key] = value
    
    return row


def parse_log(log_file: Path) -> pd.DataFrame:
    """Parse entire log file and extract all METRIC lines."""
    metadata = parse_filename(log_file)
    
    if metadata is None:
        return pd.DataFrame()
    
    rows = []
    
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            if "[METRIC]" not in line:
                continue
            
            row = metadata.copy()
            row.update(parse_metric_line(line))
            rows.append(row)
    
    return pd.DataFrame(rows)


def aggregate_by_experiment(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate metrics by experiment configuration."""
    # Group by configuration
    group_cols = [
        "mode", "profile", "rate_pattern", "motion_mode",
        "window_size_ms", "watermark_delay_ms"
    ]
    
    # Metrics to aggregate
    agg_dict = {
        # Performance metrics
        "event_rate": ["mean", "std", "min", "max"],
        "avg_latency_ms": ["mean", "std", "min", "max"],
        "processing_latency_ms": ["mean", "std", "min", "max"],
        "ml_inference_ms": ["mean", "std", "max"],
        
        # Quality metrics
        "disorder_ratio": ["mean", "std"],
        "late_event_ratio": ["mean", "std"],
        "window_fill_ratio": ["mean", "std", "min", "max"],
        "avg_events_per_window": ["mean", "std"],
        
        # Interaction metrics
        "interaction_rate": ["mean", "std"],
        "collision_rate": ["mean", "std"],
        "proximity_rate": ["mean", "std"],
        "swarm_rate": ["mean", "std"],
        "conflict_rate": ["mean", "std"],
        
        # Counts (cumulative)
        "total_events": ["max"],
        "total_interactions": ["max"],
        "collision_count": ["max"],
        "proximity_count": ["max"],
        "swarm_count": ["max"],
        "conflict_count": ["max"],
        
        # Adaptive metrics
        "adaptation_count": ["max", "mean"],
        "window_change_rate": ["mean", "std"],
        "watermark_change_rate": ["mean", "std"],
        "window_oscillation": ["max", "mean"],
        "watermark_oscillation": ["max", "mean"],
        "window_oscillation_rate": ["mean", "std"],
        "watermark_oscillation_rate": ["mean", "std"],
        "avg_adaptation_interval_ms": ["mean", "std"],
        
        # Range metrics
        "min_window_ms": ["min"],
        "max_window_ms": ["max"],
        "min_watermark_ms": ["min"],
        "max_watermark_ms": ["max"],
        
        # Lag metrics
        "watermark_lag_ms": ["mean", "std", "min", "max"],
        "ingestion_lag_ms": ["mean", "std", "max"],
    }
    
    # Create aggregated dataframe
    agg_df = df.groupby(group_cols).agg(agg_dict)
    
    # Flatten multi-level columns
    agg_df.columns = [f"{col}_{agg}" for col, agg in agg_df.columns]
    
    # Reset index to make group columns accessible
    agg_df = agg_df.reset_index()
    
    return agg_df


def generate_experiment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate high-level summary for each experiment."""
    summary_cols = [
        "mode", "profile", "rate_pattern", "motion_mode",
        "window_size_ms", "watermark_delay_ms"
    ]
    
    # Key metrics for summary
    summary_metrics = {
        "total_events_max": "Total Events",
        "total_interactions_max": "Total Interactions",
        "event_rate_mean": "Avg Event Rate",
        "avg_latency_ms_mean": "Avg Latency (ms)",
        "processing_latency_ms_mean": "Avg Processing (ms)",
        "ml_inference_ms_mean": "Avg ML Inference (ms)",
        "interaction_rate_mean": "Interaction Rate",
        "collision_count_max": "Collisions",
        "proximity_count_max": "Proximity Events",
        "swarm_count_max": "Swarms",
        "conflict_count_max": "Conflicts",
        "adaptation_count_max": "Adaptations",
        "window_oscillation_max": "Window Oscillations",
        "watermark_oscillation_max": "Watermark Oscillations",
        "window_change_rate_mean": "Window Change Rate",
        "watermark_change_rate_mean": "Watermark Change Rate",
    }
    
    # Select only columns that exist
    existing_cols = [col for col in summary_metrics.keys() if col in df.columns]
    
    summary = df[summary_cols + existing_cols].copy()
    
    # Rename columns
    rename_map = {col: label for col, label in summary_metrics.items() if col in df.columns}
    summary = summary.rename(columns=rename_map)
    
    return summary


def main():
    """Main parsing and analysis pipeline."""
    
    # Find all log files
    log_files = sorted(LOG_DIR.glob("*.log"))
    
    print(f"Found {len(log_files)} log files")
    print("=" * 60)
    
    all_dfs = []
    processed_files = 0
    skipped_files = 0
    
    for log_file in log_files:
        metadata = parse_filename(log_file)
        
        if metadata is None:
            print(f"⚠️  Skipping {log_file.name} (unrecognized format)")
            skipped_files += 1
            continue
        
        print(f"📊 Parsing {log_file.name}")
        print(f"   Mode: {metadata['mode']}, Profile: {metadata['profile']}, "
              f"Rate: {metadata['rate_pattern']}, Motion: {metadata['motion_mode']}, "
              f"Window: {metadata['window_size_ms']}ms, Watermark: {metadata['watermark_delay_ms']}ms")
        
        df = parse_log(log_file)
        
        if df.empty:
            print(f"   ⚠️  No METRIC lines found")
            skipped_files += 1
            continue
        
        # Save individual parsed file
        output_file = OUTPUT_DIR / f"{log_file.stem}.csv"
        df.to_csv(output_file, index=False)
        
        all_dfs.append(df)
        processed_files += 1
        
        print(f"   ✅ Saved {len(df)} rows -> {output_file.name}")
        print()
    
    if not all_dfs:
        print("❌ No data to process")
        return
    
    # Combine all data
    print("=" * 60)
    print("📈 Combining all parsed data...")
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by timestamp
    if "run_timestamp" in combined_df.columns:
        combined_df = combined_df.sort_values(["profile", "run_timestamp"])
    
    # Save combined dataset
    combined_output = OUTPUT_DIR / "all_experiments.csv"
    combined_df.to_csv(combined_output, index=False)
    print(f"✅ Saved combined dataset: {combined_output} ({len(combined_df)} rows)")
    
    # Generate aggregated view
    print("\n📊 Generating aggregated statistics...")
    agg_df = aggregate_by_experiment(combined_df)
    agg_output = OUTPUT_DIR / "aggregated_experiments.csv"
    agg_df.to_csv(agg_output, index=False)
    print(f"✅ Saved aggregated data: {agg_output} ({len(agg_df)} experiments)")
    
    # Generate summary
    print("\n📋 Generating experiment summary...")
    summary_df = generate_experiment_summary(agg_df)
    summary_output = OUTPUT_DIR / "experiment_summary.csv"
    summary_df.to_csv(summary_output, index=False)
    print(f"✅ Saved summary: {summary_output} ({len(summary_df)} experiments)")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("📊 SUMMARY STATISTICS")
    print("=" * 60)
    
    # Count by mode
    print(f"\n📌 Experiments by Mode:")
    mode_counts = combined_df['mode'].value_counts()
    for mode, count in mode_counts.items():
        print(f"   {mode}: {count} metric rows")
    
    # Count by profile
    print(f"\n📌 Experiments by Profile:")
    profile_counts = combined_df['profile'].value_counts()
    for profile, count in profile_counts.items():
        print(f"   {profile}: {count} metric rows")
    
    # Count by motion mode
    print(f"\n📌 Experiments by Motion:")
    motion_counts = combined_df['motion_mode'].value_counts()
    for motion, count in motion_counts.items():
        print(f"   {motion}: {count} metric rows")
    
    # Calculate coverage of experimental matrix
    print(f"\n📌 Experimental Matrix Coverage:")
    expected_combinations = len(PROFILES) * len(RATES) * len(MOTIONS)
    actual_configs = len(agg_df)
    coverage = (actual_configs / expected_combinations) * 100
    
    print(f"   Expected configurations: {expected_combinations}")
    print(f"   Actual configurations: {actual_configs}")
    print(f"   Coverage: {coverage:.1f}%")
    
    # Performance comparison between fixed and adaptive
    print(f"\n📌 Fixed vs Adaptive Comparison (overall):")
    mode_comparison = combined_df.groupby('mode').agg({
        'event_rate': 'mean',
        'interaction_rate': 'mean',
        'processing_latency_ms': 'mean',
        'ml_inference_ms': 'mean',
        'total_interactions': 'max',
        'adaptation_count': 'max',
        'window_change_rate': 'mean',
        'watermark_change_rate': 'mean'
    }).round(3)
    print(mode_comparison)
    
    # Save summary stats
    stats_output = OUTPUT_DIR / "summary_statistics.txt"
    with open(stats_output, "w") as f:
        f.write("ADAPTIVE CONTROL EXPERIMENT SUMMARY\n")
        f.write("=" * 60 + "\n")
        f.write(f"Total log files processed: {processed_files}\n")
        f.write(f"Total metric rows parsed: {len(combined_df)}\n")
        f.write(f"Total experiments: {len(agg_df)}\n")
        f.write(f"Expected configurations: {expected_combinations}\n")
        f.write(f"Coverage: {coverage:.1f}%\n")
        f.write("\nMode counts:\n")
        for mode, count in mode_counts.items():
            f.write(f"  {mode}: {count}\n")
        f.write("\nProfile counts:\n")
        for profile, count in profile_counts.items():
            f.write(f"  {profile}: {count}\n")
        f.write("\nMotion counts:\n")
        for motion, count in motion_counts.items():
            f.write(f"  {motion}: {count}\n")
    
    print(f"\n✅ Summary statistics saved to: {stats_output}")
    print(f"\n✅ All outputs saved to: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
