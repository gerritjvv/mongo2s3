{{- define "mongo2s3.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "mongo2s3.labels" -}}
app.kubernetes.io/name: {{ include "mongo2s3.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "mongo2s3.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end }}

{{- define "mongo2s3.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mongo2s3.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
