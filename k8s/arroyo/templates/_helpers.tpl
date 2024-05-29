{{/*
Expand the name of the chart.
*/}}
{{- define "arroyo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "arroyo.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Prometheus endpoint
*/}}
{{- define "arroyo.prometheusEndpoint" -}}
{{- if .Values.prometheus.endpoint }}
{{- .Values.prometheus.endpoint }}
{{- else if .Values.prometheus.deploy -}}
http://{{- template "prometheus.fullname" . }}-prometheus-server.{{- default .Release.Namespace }}.svc.cluster.local
{{- end }}
{{- end }}

{{/*
{{- end }}
{{- end }}

{{/*

{{/*

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "arroyo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "arroyo.labels" -}}
helm.sh/chart: {{ include "arroyo.chart" . }}
{{ include "arroyo.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "arroyo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arroyo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "arroyo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arroyo.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "arroyo.roleName" -}}
{{- if .Values.role.create }}
{{- default (include "arroyo.fullname" .) .Values.role.name }}
{{- else }}
{{- default "default" .Values.role.name }}
{{- end }}
{{- end }}

{{- define "tplvalues.render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}