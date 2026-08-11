{{- define "k12-clean-qa.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "k12-clean-qa.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "k12-clean-qa.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "k12-clean-qa.namespace" -}}
{{- default .Release.Namespace .Values.global.namespaceOverride -}}
{{- end }}

{{- define "k12-clean-qa.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "k12-clean-qa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "k12-clean-qa.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k12-clean-qa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "k12-clean-qa.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "k12-clean-qa.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "k12-clean-qa.rayClusterName" -}}
{{- printf "%s-%s" (include "k12-clean-qa.fullname" .) .Values.ray.clusterName | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "k12-clean-qa.rayHeadService" -}}
{{- printf "%s-head-svc" (include "k12-clean-qa.rayClusterName" .) -}}
{{- end }}

{{- define "k12-clean-qa.mineruName" -}}
{{- printf "%s-mineru" (include "k12-clean-qa.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "k12-clean-qa.qwenName" -}}
{{- printf "%s-qwen" (include "k12-clean-qa.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "k12-clean-qa.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{- define "k12-clean-qa.s3Env" -}}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.externalS3.credentialsSecret.name | quote }}
      key: {{ .Values.externalS3.credentialsSecret.accessKeyKey | quote }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.externalS3.credentialsSecret.name | quote }}
      key: {{ .Values.externalS3.credentialsSecret.secretKeyKey | quote }}
- name: AWS_DEFAULT_REGION
  value: {{ .Values.externalS3.region | quote }}
- name: AWS_EC2_METADATA_DISABLED
  value: "true"
- name: S3_ENDPOINT_URL
  value: {{ .Values.externalS3.endpoint | quote }}
{{- end }}

{{- define "k12-clean-qa.proxyEnv" -}}
{{- if .Values.proxy.httpProxy }}
- name: HTTP_PROXY
  value: {{ .Values.proxy.httpProxy | quote }}
- name: http_proxy
  value: {{ .Values.proxy.httpProxy | quote }}
{{- end }}
{{- if .Values.proxy.httpsProxy }}
- name: HTTPS_PROXY
  value: {{ .Values.proxy.httpsProxy | quote }}
- name: https_proxy
  value: {{ .Values.proxy.httpsProxy | quote }}
{{- end }}
- name: NO_PROXY
  value: {{ .Values.proxy.noProxy | quote }}
- name: no_proxy
  value: {{ .Values.proxy.noProxy | quote }}
{{- end }}

{{- define "k12-clean-qa.ascendVolumeMounts" -}}
{{- if .Values.ascend.hostMounts.enabled }}
- name: ascend-driver
  mountPath: /usr/local/Ascend/driver
  readOnly: true
- name: ascend-dcmi
  mountPath: /usr/local/dcmi
  readOnly: true
- name: npu-smi
  mountPath: /usr/local/bin/npu-smi
  readOnly: true
{{- end }}
{{- end }}

{{- define "k12-clean-qa.ascendVolumes" -}}
{{- if .Values.ascend.hostMounts.enabled }}
- name: ascend-driver
  hostPath:
    path: {{ .Values.ascend.hostMounts.driverPath | quote }}
    type: Directory
- name: ascend-dcmi
  hostPath:
    path: {{ .Values.ascend.hostMounts.dcmiPath | quote }}
    type: Directory
- name: npu-smi
  hostPath:
    path: {{ .Values.ascend.hostMounts.npuSmiPath | quote }}
    type: File
{{- end }}
{{- end }}

{{- define "k12-clean-qa.qwenModelVolume" -}}
{{- if eq .Values.models.qwen.volumeType "pvc" }}
persistentVolumeClaim:
  claimName: {{ required "models.qwen.existingClaim is required for PVC model storage" .Values.models.qwen.existingClaim | quote }}
{{- else if eq .Values.models.qwen.volumeType "hostPath" }}
hostPath:
  path: {{ required "models.qwen.hostPath is required for hostPath model storage" .Values.models.qwen.hostPath | quote }}
  type: Directory
{{- else }}
{{- fail "models.qwen.volumeType must be hostPath or pvc" }}
{{- end }}
{{- end }}

{{- define "k12-clean-qa.endpointSpecs" -}}
{{- range $index, $endpoint := . -}}
{{- if $index }};{{ end -}}
{{ $endpoint.id }}|{{ $endpoint.devices }}|{{ $endpoint.port }}|{{ $endpoint.cpuSet }}
{{- end -}}
{{- end }}

{{- define "k12-clean-qa.ascendDeviceAnnotation" -}}
{{- range $index, $device := . -}}
{{- if $index }},{{ end -}}Ascend910-{{ $device }}
{{- end -}}
{{- end }}
