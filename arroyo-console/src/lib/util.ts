import { Metric, MetricGroup } from './data_fetching';

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

export function getCurrentMaxMetric(metricGroup: MetricGroup): number {
  return metricGroup.subtasks
    .map(s => s.metrics)
    .filter(m => m.length)
    .map(m =>
      m.reduce((r: Metric, c: Metric) => {
        if (c.time > r.time) {
          return c;
        } else {
          return r;
        }
      }, m[0])
    )
    .map(m => m.value)
    .reduce((max, curr) => Math.max(max, curr));
}

export function transformMetricGroup(metric_group: MetricGroup) {
  return metric_group.subtasks.map(s =>
    s.metrics.map(m => {
      return {
        label: s.index,
        x: new Date(Number(m.time) / 1000),
        y: m.value + m.value * Math.random() * 0.01,
      };
    })
  );
}

export function formatDate(timestamp: bigint) {
  const date = new Date(Number(timestamp / BigInt(1000)));
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'short',
    timeStyle: 'short',
  }).format(date);
}

export function relativeTime(timestamp: number): string {
  const now = Date.now() * 1000;
  const microsecondsAgo = now - timestamp;

  const secondsAgo = Math.floor(microsecondsAgo / 1_000_000);
  if (secondsAgo < 60) {
    return 'just now';
  }

  const minutesAgo = Math.floor(secondsAgo / 60);
  if (minutesAgo < 60) {
    return `${minutesAgo} minutes ago`;
  }

  const hoursAgo = Math.floor(minutesAgo / 60);
  if (hoursAgo < 24) {
    return `${hoursAgo} hours ago`;
  }

  const daysAgo = Math.floor(hoursAgo / 24);
  if (daysAgo < 30) {
    return `${daysAgo} days ago`;
  }

  const monthsAgo = Math.floor(daysAgo / 30);
  if (monthsAgo < 12) {
    return `${monthsAgo} months ago`;
  }

  const yearsAgo = Math.floor(monthsAgo / 12);
  return `${yearsAgo} years ago`;
}

export const formatError = (error: any) => {
  if (error.error != undefined && typeof error.error === 'string') {
    return error.error;
  } else {
    return 'Something went wrong.';
  }
};

export const generate_udf_id = () => {
  let result = '';
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  let counter = 0;
  while (counter < 5) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
    counter += 1;
  }
  return result;
};
