import { JobMetricsResp, Metric } from '../gen/api_pb';

export function durationFormat(micros: number): string {
  const units = [
    ['μs', 1000],
    ['ms', 1000],
    ['s', 60],
    ['m', 60],
    ['h', 24],
    ['d', 1],
  ];

  let cur = micros;
  let curUnit = units[0][0];
  units.some(u => {
    let next = cur / (u[1] as number);
    curUnit = u[0];
    if (next < 1) {
      return true;
    } else {
      cur = next;
      return false;
    }
  });

  return String(Math.round(cur)) + curUnit;
}

export function datarateFormat(bps: number): string {
  const units = [
    ['B/s', 1024],
    ['KB/s', 1024],
    ['MB/s', 1024],
    ['GB/s', 1024],
    ['TB/s', 1024],
  ];

  let cur = bps;
  let curUnit = units[0][0];
  units.some(u => {
    let next = cur / (u[1] as number);
    curUnit = u[0];
    if (next < 1) {
      return true;
    } else {
      cur = next;
      return false;
    }
  });

  return String(Math.round(cur)) + curUnit;
}

export function dataFormat(bytes: number): string {
  const units = [
    ['B', 1024],
    ['KB', 1024],
    ['MB', 1024],
    ['GB', 1024],
    ['TB', 1024],
  ];

  let cur = bytes;
  let curUnit = units[0][0];
  units.some(u => {
    let next = cur / (u[1] as number);
    curUnit = u[0];
    if (next < 1) {
      return true;
    } else {
      cur = next;
      return false;
    }
  });

  return String(Math.round(cur)) + curUnit;
}

export function stringifyBigint(data: any, indent: number): string {
  return JSON.stringify(
    data,
    (_, value) => (typeof value === 'bigint' ? value.toString() + 'n' : value),
    indent
  );
}

export function getBackpressureColor(backpressure: number): string {
  // backPressure should be a value between 0 and 1
  const thresholds = [0.33, 0.66];
  const colors = ['#ffffff', '#ffdb5d', '#e06262'];

  // Find the corresponding color based on the number
  for (let i = 0; i < thresholds.length; i++) {
    if (backpressure <= thresholds[i]) {
      return colors[i];
    }
  }

  // If the number is greater than the last threshold, return the last color
  return colors[colors.length - 1];
}

export function getOperatorBackpressure(
  metrics: JobMetricsResp | undefined,
  operator: string
): number {
  if (!metrics) {
    return 0;
  }

  // reduce the subtasks backpressure to an operator-level backpressure

  const nodeMetrics = metrics.metrics[operator];

  if (!nodeMetrics) {
    return 0;
  }

  const backPressureMetrics = Object.entries(nodeMetrics.subtasks)
    .map(kv => {
      return kv[1].backpressure;
    })
    .filter(ml => ml.length);

  if (!backPressureMetrics.length) {
    return 0;
  }

  const recent = backPressureMetrics.map(m =>
    m.reduce((r: Metric, c: Metric) => {
      if (c.time > r.time) {
        return c;
      } else {
        return r;
      }
    }, m[0])
  );

  return recent.map(m => m.value).reduce((max, curr) => Math.max(max, curr));
}

export function formatDate(timestamp: bigint) {
  const date = new Date(Number(timestamp / BigInt(1000)));
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'short',
    timeStyle: 'short',
  }).format(date);
}
