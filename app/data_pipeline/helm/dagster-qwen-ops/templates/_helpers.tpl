{{- define "dagster-qwen-ops.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dagster-qwen-ops.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "dagster-qwen-ops.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dagster-qwen-ops.namespace" -}}
{{- default .Release.Namespace .Values.namespace.nameOverride -}}
{{- end -}}

{{- define "dagster-qwen-ops.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "dagster-qwen-ops.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "dagster-qwen-ops.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dagster-qwen-ops.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "dagster-qwen-ops.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "dagster-qwen-ops.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "dagster-qwen-ops.workerName" -}}
{{- printf "%s-%s" (include "dagster-qwen-ops.fullname" .root) .worker.nameSuffix | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dagster-qwen-ops.deviceAnnotation" -}}
{{- $root := .root -}}
{{- range $index, $device := .devices -}}{{ if $index }},{{ end }}{{ $root.Values.ascend.deviceAnnotation.prefix }}{{ $device }}{{- end -}}
{{- end -}}

{{- define "dagster-qwen-ops.healthCommand" -}}
{{- $worker := .worker -}}
{{- range $index, $endpoint := $worker.endpoints -}}{{ if $index }} && {{ end }}curl --noproxy '*' -fsS http://127.0.0.1:{{ $endpoint.port }}{{ $worker.runtime.healthPath }} >/dev/null{{- end -}}
{{- end -}}

{{- define "dagster-qwen-ops.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{ toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "dagster-qwen-ops.proxyEnv" -}}
{{- if .Values.proxy.httpProxy }}
- {name: HTTP_PROXY, value: {{ .Values.proxy.httpProxy | quote }}}
- {name: http_proxy, value: {{ .Values.proxy.httpProxy | quote }}}
{{- end }}
{{- if .Values.proxy.httpsProxy }}
- {name: HTTPS_PROXY, value: {{ .Values.proxy.httpsProxy | quote }}}
- {name: https_proxy, value: {{ .Values.proxy.httpsProxy | quote }}}
{{- end }}
- {name: NO_PROXY, value: {{ .Values.proxy.noProxy | quote }}}
- {name: no_proxy, value: {{ .Values.proxy.noProxy | quote }}}
{{- end -}}

{{- define "dagster-qwen-ops.modelVolume" -}}
{{- if eq .Values.modelVolume.type "pvc" }}
persistentVolumeClaim:
  claimName: {{ required "modelVolume.existingClaim is required for pvc" .Values.modelVolume.existingClaim | quote }}
{{- else if eq .Values.modelVolume.type "hostPath" }}
hostPath:
  path: {{ required "modelVolume.hostPath is required for hostPath" .Values.modelVolume.hostPath | quote }}
  type: Directory
{{- else }}
{{- fail "modelVolume.type must be pvc or hostPath" }}
{{- end }}
{{- end -}}
