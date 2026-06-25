{{- define "arroyo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

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

{{- define "arroyo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "arroyo.labels" -}}
helm.sh/chart: {{ include "arroyo.chart" . }}
{{ include "arroyo.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "arroyo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arroyo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "arroyo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arroyo.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "arroyo.roleName" -}}
{{- if .Values.role.create }}
{{- default (include "arroyo.fullname" .) .Values.role.name }}
{{- else }}
{{- default "default" .Values.role.name }}
{{- end }}
{{- end }}

{{- define "arroyo.databasePasswordEnv" -}}
{{- if .Values.postgresql.deploy }}
- name: ARROYO__DATABASE__POSTGRES__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "arroyo.fullname" . }}-postgresql
      key: password
{{- else if .Values.postgresql.externalDatabase.existingSecret }}
- name: ARROYO__DATABASE__POSTGRES__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgresql.externalDatabase.existingSecret }}
      key: {{ .Values.postgresql.externalDatabase.existingSecretPasswordKey | default "password" }}
{{- end }}
{{- end }}

{{- define "tplvalues.render" -}}
{{- if typeIs "string" .value }}
{{- tpl .value .context }}
{{- else }}
{{- tpl (.value | toYaml) .context }}
{{- end }}
{{- end -}}
