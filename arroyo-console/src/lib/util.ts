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
