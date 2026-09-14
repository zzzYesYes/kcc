{{- define "kcc-training.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kcc-training.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "kcc-training.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kcc-training.serviceAccountName" -}}
{{- default (include "kcc-training.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "kcc-training.labels" -}}
app.kubernetes.io/name: {{ include "kcc-training.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
