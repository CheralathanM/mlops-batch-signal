# mlops-batch-signal
Minimal MLOps-style batch job computing trading signals with rolling mean, metrics, and logging.


# MLOps Task — Batch Signal Pipeline

## Local Run
pip install -r requirements.txt
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Docker Build & Run
docker build -t mlops-task .
docker run --rm mlops-task

## Example metrics.json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4989,
  "latency_ms": 98,
  "seed": 42,
  "status": "success"
}
