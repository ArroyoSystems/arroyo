import React from 'react';
import * as d3 from 'd3';
import * as MG from 'metrics-graphics';

interface TimeSeries {
  data?: Array<Array<{ x: Date; y: number }>>;
  timeWindowMs: number;
}

export class TimeSeriesGraph extends React.Component<TimeSeries, {}> {
  private id = (Math.random() * 100000).toFixed();

  update() {
    if (this.props.data == null) {
      return;
    }
    const id = 'timeseries-graph-' + this.id;
    const now = new Date();
    const start = new Date(now.getTime() - this.props.timeWindowMs);

    const yMax =
      Math.round(
        this.props.data
          .flat()
          .map(f => f.y)
          .reduce((p, n) => Math.max(p, n), 0) *
          1.25 *
          10
      ) / 10;

    const yMin =
      Math.round(
        this.props.data
          .flat()
          .map(f => f.y)
          .reduce((p, n) => Math.min(p, n), Infinity) *
          0.75 *
          10
      ) / 10;

    // make sure we have a color for every series, otherwise the library defaults to black
    let colors = d3.schemeSet2.concat(
      new Array(Math.max(0, this.props.data.length - d3.schemeSet2.length)).fill('white')
    );

    const data = {
      data: this.props.data,
      target: document.getElementById(id),
      height: 200,
      width: 450,
      colors: colors,
      xAccessor: 'x',
      yAccessor: 'y',
      xScale: {
        minValue: start,
        maxValue: now,
      },
      yScale: {
        minValue: yMin,
        maxValue: yMax,
      },
      yAxis: {
        tickCount: 4,
      },
      xAxis: {
        tickCount: 5,
      },
      tooltipFunction: (data: { x: Date; y: number }) => {
        return `${new Intl.DateTimeFormat('en-US', { timeStyle: 'medium' }).format(data.x)} ${
          Math.round(data.y * 10) / 10
        }`;
      },
    };

    // @ts-ignore types are not general enough in MetricsGraphics
    new MG.LineChart(data);
  }

  componentDidMount() {
    this.update();
  }

  componentDidUpdate() {
    this.update();
  }

  render() {
    return <div className="timeseriesGraph" id={'timeseries-graph-' + this.id}></div>;
  }
}
