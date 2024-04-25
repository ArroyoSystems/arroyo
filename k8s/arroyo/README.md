# Arroyo

This is the official Helm chart for deploying [Arroyo](https://github.com/ArroyoSystems/arroyo), a distributed stream
processing engine, on Kubernetes.

Arroyo supports Kubernetes as both a _scheduler_ (for running Arroyo pipeline tasks) and as as a deploy target for the
Arroyo control plane. This is the easiest way to get a production quality Arroyo cluster running.

See the [docs](https://doc.arroyo.dev/deployment/kubernetes) for full information on how to use this helm chart.

Each version of the helm chart is associated by default with a particular release of Arroyo. The latest release
is [0.10.0](https://www.arroyo.dev/blog/arroyo-0-10-0).

## Quickstart

To quickly get up and running with Arroyo on a local kubernetes cluster (e.g. minikube), first create the following
configuration file:

_values.yaml_

```yaml
outputDir: "/tmp/arroyo-test"

volumes:
  - name: checkpoints
    hostPath:
      path: /tmp/arroyo-test
      type: DirectoryOrCreate

volumeMounts:
  - name: checkpoints
    mountPath: /tmp/arroyo-test
```

Then you can create the cluster with

```
$ helm install arroyo arroyo/arroyo -f values.yaml
```

Once all of the pods start up (which may take a few minutes if you're deploying Postgres) you should be able to access
the Arroyo UI by running

```
$ open "http://$(kubectl get service/arroyo-api -o jsonpath='{.spec.clusterIP}')"
```

or if that doesn't work, by proxying the service:

```
$ kubectl port-forward service/arroyo-api 8000:80 8001:8001
```

and opening http://localhost:8000.


## Configuration

The Helm chart provides a number of options, which can be inspected by running

```
$ helm show values arroyo/arroyo
```

The most important options are:

- `postgresql.deploy`: Whether to deploy a new Postgres instance. If set to `false`, the chart will expect a Postgres
  instance to be available with the connection settings determined by `postgresql.externalDatabase` configurations
  (by default: postgres://arroyo:arroyo@localhost:5432/arroyo).
- `prometheus.deploy`: Whether to deploy a new Prometheus instance. If set to `false`, the chart will expect a
  Prometheus instance to be availble at the URL determined by `prometheus.endpoint` (by default: http://localhost:9090).
  Prometheus is not required for operation of the system, but is needed for the Arroyo UI metrics to function. By
  default Arroyo expects you to have a scrape interval of 5s. If you have a higher scrape interval (for example, the
  default 1m) you will need to update the `prometheus.queryRate` configuration to at least 4x the scrape interval.
- `s3.bucket` and `s3.region`: Configures the s3 bucket and region that will be used for storing artifacts and
  checkpoints. If using s3, the pods will need to have access to the bucket.
- `outputDir`: Alternatively, you may configure the pods to write artifacts and checkpoints to a local directory when
  running a local Kubernetes cluster. You will additionally need to configure `volumes` and `volumeMounts` to make this
  directory available on all of the pods.


The helm chart can be configured either via a `values.yaml` file or via command line arguments. See the
[Helm documentation](https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing) for more details.

## Help

If you have any questions or need help, please feel free to reach out on our
[Discord server](https://discord.gg/cjCr5rVmyR).
