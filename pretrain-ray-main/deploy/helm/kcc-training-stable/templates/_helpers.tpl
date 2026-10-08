{{- define "kcc.name" -}}kcc-training{{- end -}}
{{- define "kcc.fullname" -}}{{ printf "%s-%s" .Release.Name (include "kcc.name" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "kcc.clusterResourceName" -}}{{ printf "%s-%s" .Release.Namespace (include "kcc.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "kcc.controllerServiceAccountName" -}}
{{- default (include "kcc.fullname" .) .Values.controller.serviceAccount.name -}}
{{- end -}}
{{- define "kcc.runtimeServiceAccountName" -}}
{{- default (printf "%s-runtime" (include "kcc.fullname" .)) .Values.runtimeServiceAccount.name -}}
{{- end -}}
{{- define "kcc.labels" -}}
app.kubernetes.io/name: {{ include "kcc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
