{{- define "kcc.name" -}}kcc-training{{- end -}}
{{- define "kcc.fullname" -}}{{ printf "%s-%s" .Release.Name (include "kcc.name" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "kcc.labels" -}}
app.kubernetes.io/name: {{ include "kcc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

