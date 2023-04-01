
Arroyo
============

<img src="docs/images/arroyo_logo.png" width="400px">

[Arroyo](https://arroyo.dev) is a distributed stream processing engine written in Rust, designed to efficiently
perform stateful computations on streams of data. Unlike traditional batch processing, streaming engines can operate
on both bounded and unbounded sources, emitting results as soon as they are available.

In short: Arroyo lets you ask complex questions of high-volume real-time data with subsecond results.

![running job](docs/images/header_image.png)

## Features

* SQL and Rust pipelines
* Scales up to millions of events per second
* Stateful operations like windows and joins
* State checkpointing for fault-tolerance and recovery of pipelines
* Timely stream processing via the [Dataflow model](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)

## Use cases

Some example use cases include:

* Detecting fraud and security incidents
* Real-time product and business analytics
* Real-time ingestion into your data warehouse or data lake
* Real-time ML feature generation

## Why Arroyo

There are already a number of existing streaming engines out there, including [Apache Flink](https://flink.apache.org),
[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and
[Kafka Streams](https://kafka.apache.org/documentation/streams/). Why create a new one?

* _Serverless operations_: Arroyo pipelines are designed to run in modern cloud environments, supporting seamless scaling,
    recovery, and rescheduling
* _High performance SQL_: SQL is a first-class concern, with consistently excellent performance
* _Designed for non-experts_: Arroyo cleanly separates the pipeline APIs from its internal implementation. You don't
    need to be a streaming expert to build real-time data pipelines.

## Current state

Arroyo is currently alpha. It is missing featurs and has known bugs. At this stage, it is likely to primarily be of
interest to the Rust data processing community and contributors.

## Getting Started

### With Docker

The easiest way to get started running Arroyo is with the single-node Docker image. This contains all of the services
you need to get a test Arroyo system up and running.

```bash
$ docker run -p 8000:8000 -p 8001:8001 ghcr.io/arroyosystems/arroyo:single
```

This will run all of the core arroyo services and a temporary Postgres database. The web UI will be exposed at
http://localhost:8000.

Note that this mode should not be used for production, as it does not persist configuration state and does not support
any distributed runtimes.

### Locally

Arroyo can easily be run locally as well. Currently Linux and MacOS are well supported as development environments.

To run Arroyo locally first follow the dev setup instructions to get a working dev environment. Then, you can start
the API and Controller services:

```bash
$ target/release/arroyo-controller
# in another terminal
$ target/release/arroyo-api
```

Then you can navigate to the web ui at http://localhost:8000.

### Tutorial

#### Creating a source

Arroyo includes several generated data sources we can use to try out the engine. To create one, go to the Sources tab
on the left menu, and click "Create Source".

From there, you can choose what type of source you would like to create. For this tutorial, we're going to create a
Nexmark source. [Nexmark](https://datalab.cs.pdx.edu/niagara/NEXMark/) is a standard benchmark for streaming systems.
It involves various query patterns over a data set from a simulated auction website.

Once Nexmark is selected, click continue. Then set the desired event rate. For this tutorial, the default of 100/sec
should suffice.

Click continue, and finally give the source a name, like "nexmark_100," and click "Publish" to finish creating the
source.

#### Creating a pipeline

Now that we've created a source to query, we can start to build pipelines by writing SQL. Navigate to the Jobs page
on the left sidebar, then click "Create Pipeline."

This opens up the SQL editor. On the left is a list of tables that we can query (our catalogue). The main UI consists
of a text editor where we can write SQL, and actions we can take with that query, like previewing it on our data set.

Let's start with a simple query:

```sql
SELECT bid FROM nexmark WHERE bid IS NOT NULL;
```

This will simply dump out all of our data from our source that fits the filter criteria, which can be useful to get
a handle on our data before we start to build more sophisticated queries. Note that the first query may take some time
to compile, although subsequent queries should be much faster.

Once the query runs, you should start to see outputs in the Results tab. Since we did a simple pass-through query,
each input event will result in a single output line.

Many streaming pipelines involve working with time in some way. Arroyo supports a few different ways of expressing
computations over the time characteristic of our data. Let's add a sliding window (called `hop` in SQL) to perform
a time-oriented aggregation:

```sql
SELECT avg(bid.price) as avg_price
FROM nexmark
WHERE bid IS NOT NULL
GROUP BY hop(interval '2 seconds', interval '10 seconds');
```

This query computes an aggregate function (avg) over a sliding window with a 10 second width, updating every 2
seconds. You can preview this and see that it does what you would expect: produce a result every 2 seconds for
the average bid price for all bids in the past ten seconds.

Arroyo supports complex analytical SQL functions, like SQL windows. Let's use that capability to implement query 5
of the Nexmark benchmark, which asks us to find the top 5 auctions by the number of bids over a sliding window. In
Arroyo, that query looks like this:

```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window
        ORDER BY count DESC) AS row_num
    FROM (SELECT count(*) AS count, bid.auction AS auction,
        hop(interval '2 seconds', interval '60 seconds') AS window
            FROM nexmark WHERE bid is not null
            GROUP BY 2, window)) WHERE row_num <= 5
```

Plug that into the editor and preview it, validating the results look like we expect.

Once we're happy with it, we can run the pipeline for real. Click the "Start Pipeline" button, give it a name
(for example "top_auctions"). Then we can select a sink, which determines where the results go. The current release
of Arroyo supports streaming results to the browser ("Web" sink), to a log file ("Log"), and to Kafka. For now,
select Web so that we can see the outputs, and click "Start".

This will compile and run the pipeline. Once it's running, we can click nodes on the pipeline dataflow graph and
see metrics for that operator. Clicking into the Outputs tab we can tail the results for the pipeline. The Checkpoints
tab shows metadata for the checkpoints for the pipeline. Arroyo regularly takes consistent checkpoints of the state
of the pipeline (using a variation of asynchronous barrier snapshotting algorithm described in
[this paper](https://arxiv.org/abs/1506.08603)) so we can recover from failure.

We can also control execution of the pipeline, stopping and starting it. Before stopping the pipeline Arroyo takes
a final checkpoint so that we can restart it without any dataloss.

## Dev setup

### Dependencies

To build Arroyo, you will need to install some dependencies.

#### Ubuntu

```bash
$ sudo apt-get install pkg-config build-essential libssl-dev openssl cmake clang postgresql postgresql-client
$ sudo systemctl start postgresql
$ curl https://sh.rustup.rs -sSf | sh -s -- -y
$ cargo install wasm-pack
$ cargo install refinery_cli
$ curl -fsSL https://get.pnpm.io/install.sh | sh -
```

#### MacOS

Coming soon

### Postgres

Developing Arroyo requires having a properly configured postgres instance running. By default,
it expects a database called `arroyo`, a user `arroyo` with password `arroyo`, although that
can be changed by setting the following environment variables:

* `DATABASE_NAME`
* `DATABASE_HOST`
* `DATABASE_USER`
* `DATABASE_PASSWORD`

On Ubuntu, you can setup a compatible database like this:

```bash
$ sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;"
$ sudo -u postgres createdb arroyo
```

Migrations are managed using [refinery](https://github.com/rust-db/refinery). Once Postgres is
set up, you can initalize it with

```bash
$ refinery setup # follow the prompts
$ mv refinery.toml ~/
$ refinery migrate -c ~/refinery.toml -p arroyo-api/migrations
```

We use [cornucopia](https://github.com/cornucopia-rs/cornucopia) for typed-checked SQL queries. Our build is set up
to automatically re-generate the rust code for those queries on build, so you will need a DB set up for compilation
as well.

### Building the services

Arroyo is built via Cargo, the Rust build tool. To build all of the services in release mode, you can run

```
$ cargo build --release
```

### Building the frontend

We use pnpm and vite for frotend development.

Start by installing pnpm by following the instructions here: https://pnpm.io/installation.

Then you should be able to run the console in dev mode like this:

```
$ cd arroyo-console
$ pnpm build
```

This will launch a dev server at http://localhost:5173 (note the controller must
also be running locally for this to be functional).

To build the console (necessary before deploying for the hosted version to work) you
can run

```
$ pnpm build
```

### Ports

Each service we run (arroyo-api, arroyo-node, and arroyo-controller) exposes two ports: a gRPC
port used for RPCs between services and an HTTP port used for metrics and admin control.

By default, these ports are:

```
CONTROLLER_HTTP = 9190
CONTROLLER_GRPC = 9191

NODE_HTTP       = 9290
NODE_GRPC       = 9291

API_HTTP        = 8000
API_GRPC        = 8001
```
