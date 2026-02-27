import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import sys
from pathlib import Path


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        required_keys = ["seed", "window", "version"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing config key: {key}")

        return config

    except Exception as e:
        raise ValueError(f"Config error: {str(e)}")


def load_dataset(input_path):
    if not Path(input_path).exists():
        raise FileNotFoundError("Input file not found")

    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("Input CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    args = parser.parse_args()

    start_time = time.time()

    try:
        setup_logging(args.log_file)
        logging.info("Job started")

        config = load_config(args.config)
        logging.info(f"Config loaded: {config}")

        np.random.seed(config["seed"])

        df = load_dataset(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        window = config["window"]

        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        df["signal"] = np.where(
            df["close"] > df["rolling_mean"],
            1,
            0
        )

        valid_signals = df["signal"].dropna()

        signal_rate = float(valid_signals.mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": config["version"],
            "rows_processed": int(len(df)),
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success"
        }

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        with open(args.output, "w") as f:
            json.dump(error_metrics, f, indent=2)

        logging.error(str(e))
        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()