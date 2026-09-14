#!/bin/bash
#  wandb登录
# pip list | grep wandb
# 如果<0.24要升级：
#  pip install -U wandb
# wandb login
# pip install tensorboard

export CUDA_DEVICE_MAX_CONNECTIONS=1
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True

# 加载 Worker 镜像内已有的 CANN 与 NNAL/ATB 环境。
source /usr/local/Ascend/cann/ascend-toolkit/set_env.sh
source /usr/local/Ascend/cann/nnal/atb/set_env.sh
if [[ -z "${ATB_HOME_PATH:-}" || ! -d "${ATB_HOME_PATH}/lib" ]]; then
    echo "ERROR: NNAL/ATB environment is unavailable: ATB_HOME_PATH=${ATB_HOME_PATH:-unset}" >&2
    exit 1
fi

# 多机训练必须设置RANK_TABLE_FILE
export RANK_TABLE_FILE=/mnt/models/CODE/wh/MindSpeed-LLM-v2.3.0/examples/mcore/qwen3/rank_table_generated.json
export HCCL_EXEC_TIMEOUT=300
export HCCL_CONNECT_TIMEOUT=7200
export DISTRIBUTED_BACKEND_TIMEOUT=7200

NPUS_PER_NODE=8
#MASTER_ADDR=localhost
MASTER_ADDR=110.129.0.5
MASTER_PORT=26011
NNODES=6
NODE_RANK=5
WORLD_SIZE=$(($NPUS_PER_NODE*$NNODES))

CKPT_SAVE_DIR="/mnt/models/00_TRAIN_RES/0717"
CKPT_LOAD_DIR="/mnt/models/00_TRAIN_RES/0717"
LOG_FILE="logs/0717.log"

WANDB_PROJECT="0701"
WANDB_EXPERIMENT="cpm_minidata_150M-gbs96-mix"
# W&B 当前未传入训练命令；启用时从 Secret/运行环境提供 WANDB_API_KEY。
export WANDB_INIT_TIMEOUT="${WANDB_INIT_TIMEOUT:-300}"
TOKENIZER_PATH="/mnt/models/MODELS/MiniCPM"
# TOKENIZER_PATH="/mnt/models/TOKENIZER_GEN/tokenizer_sp_200k"


LR=0.0003
MIN_LR=0.00003   # LR * 0.775
# MIN_LR=0.0003875   # LR * 0.55
# MIN_LR=0.0003875   # LR * 0.325
# MIN_LR=0.0003875   # LR * 0.1

# LR=0.0012
# MIN_LR=0.00093   # LR * 0.775
# MIN_LR=0.00066   # LR * 0.55
# MIN_LR=0.00039   # LR * 0.325
# MIN_LR=0.00012   # LR * 0.1

# LR=0.0001
# MIN_LR=0.0000775

# TOKENS_NUM=20000000000
TOKENS_NUM=295937343488
# TOKENS_NUM=100000000000

TP=1
PP=1
MBS=2
GBS=96
SEQ_LENGTH=4096
# TRAIN_ITERS=300000
# LR_WARMUP_ITERS=3000

TRAIN_ITERS=$(( TOKENS_NUM / (GBS * SEQ_LENGTH) ))
echo $TRAIN_ITERS
LR_WARMUP_ITERS=$(( TRAIN_ITERS / 10 ))
WSD_DECAY_ITERS=$(( TRAIN_ITERS / 10 ))
###############################################################
# DATA_PATH=""
#DATA_DIRS=("c4-bin" "ChineseWebText-bin" "zh_baike-bin" "Skypile-bin" "zh_papers-bin" )
#DATA_DIRS=( "zh_baike-bin"  "zh_papers-bin" )
#DATA_DIRS=("c4-bin" "finephrase_turiol_bin" "finepdfs-bin" "ChineseWebText-bin" "zh_baike-bin" "Skypile-bin" "zh_papers-bin" )
# DATA_DIRS=("c4-bin" "finephrase_turiol_bin" "finepdfs-bin-new" "ChineseWebText-bin-new" "zh_baike-bin" "Skypile-bin" "zh_papers-bin" )
# DATA_DIRS=("CPM/zh-cc" "CPM/dclm/global-shard_01_of_10_processed" "CPM/Skypile" "CPM/c4")
# DATA_DIRS=(
#     "CPM/cci4"
#     "CPM/ultrafineweb-en"
#     "CPM/ultrafineweb-zh"
#     "CPM/ultrafineweb-zh-l3-multistyle"
# )

# DATA_DIRS=(
#     "CPM/minimind"
#     "CPM/ultrafineweb-en"
#     "CPM/ultrafineweb-zh-l3-multistyle"
# )

# DATA_DIRS=(
#     "TOKENIZER_GEN/pretrain"
#     "TOKENIZER_GEN/ultrafineweb-en"
#     "TOKENIZER_GEN/ultrafineweb-zh-l3"
# )

# DATA_DIRS=(
#     "CPM/minimind/cpm"
# )

DATA_DIRS=(
    "ultrafineweb-en-l3"
    "ultrafineweb-zh-l3-multistyle"
    "ultrafineweb-zh-l3"
)



# 307 2.4T 477 369 = 3553
# 71.55 561.6 111.488 86.528
# DIR_WEIGHTS=("0.086" "0.675" "0.134" "0.104" )
DIR_WEIGHTS=( "0.675" "0.134" "0.104" )
# #DATA_DIRS=("c4-bin" "finepdfs-bin" "quality-bin" "ChineseWebText-bin" "zh_baike-bin"  "zh_papers-bin" )
BASE_DATA_DIR="/mnt/models/DATA_BIN/GEN"
DATA_PATH=""
for i in "${!DATA_DIRS[@]}"; do
    d="${DATA_DIRS[$i]}"
    w="${DIR_WEIGHTS[$i]}"
    SUB_DIR="${BASE_DATA_DIR}/${d}"
    if [ -d "$SUB_DIR" ]; then
        count=0
        prefixes=()
        while read -r prefix; do
            [ -f "${prefix}.idx" ] && prefixes+=("$prefix") && count=$((count+1))
        done < <(find "$SUB_DIR" -name "*.bin" | sed 's/\.bin$//' | sort)

        if [ $count -gt 0 ]; then
            per_file_weight=$(echo "scale=6; $w / $count" | bc)
            for prefix in "${prefixes[@]}"; do
                echo "  [找到] weight=${per_file_weight} $(basename $prefix)"
                # DATA_PATH="${DATA_PATH} ${per_file_weight} ${prefix}"
                DATA_PATH="${DATA_PATH} ${prefix}"
            done
        else
            echo "  [警告] $d 下无文件"
        fi
    else
        echo "  [跳过] 不存在: $SUB_DIR"
    fi
done

echo "  [data path]: $DATA_PATH"

DISTRIBUTED_ARGS="
    --nproc_per_node $NPUS_PER_NODE \
    --nnodes $NNODES \
    --node_rank $NODE_RANK \
    --master_addr $MASTER_ADDR \
    --master_port $MASTER_PORT
"

OPTIMIZE_ARGS="
    --use-flash-attn \
    --use-fused-rotary-pos-emb \
    --use-rotary-position-embeddings \
    --use-fused-swiglu \
    --use-fused-rmsnorm \
    --no-masked-softmax-fusion \
    --use-distributed-optimizer \
    --overlap-grad-reduce \
"

MODEL_PARALLEL_ARGS="
    --tensor-model-parallel-size ${TP} \
    --pipeline-model-parallel-size ${PP} \
"

TRAIN_ARGS="
    --micro-batch-size ${MBS} \
    --global-batch-size ${GBS} \
    --lr ${LR} \
    --min-lr ${MIN_LR} \
    --lr-decay-style WSD \
    --lr-wsd-decay-iters ${WSD_DECAY_ITERS} \
    --lr-warmup-iters ${LR_WARMUP_ITERS} \
    --weight-decay 1e-5 \
    --attention-dropout 0.0 \
    --hidden-dropout 0.0 \
    --clip-grad 2.0 \
    --adam-beta1 0.9 \
    --adam-beta2 0.92 \
    --initial-loss-scale 4096 \
    --seed 42 \
    --bf16 \
    --train-iters ${TRAIN_ITERS} \
    --seq-length ${SEQ_LENGTH} \
"

GPT_ARGS="
    --use-mcore-models \
    --sequence-parallel \
    --reset-attention-mask \
    --reset-position-ids \
    --spec mindspeed_llm.tasks.models.spec.qwen3_spec layer_spec \
    --kv-channels 64 \
    --qk-layernorm \
    --num-layers 28 \
    --hidden-size 768 \
    --num-attention-heads 12 \
    --ffn-hidden-size 1536 \
    --max-position-embeddings 4096 \
    --make-vocab-size-divisible-by 1 \
    --padded-vocab-size 73440 \
    --rotary-base 1000000 \
    --disable-bias-linear \
    --swiglu \
    --tokenizer-type PretrainedFromHF \
    --tokenizer-name-or-path ${TOKENIZER_PATH} \
    --normalization RMSNorm \
    --position-embedding-type rope \
    --norm-epsilon 1e-6 \
    --attention-softmax-in-fp32 \
    --exit-on-missing-checkpoint \
    --group-query-attention \
    --num-query-groups 4 \
    --override-opt_param-scheduler \
    --seed 42 \
    --bf16 \
"

DATA_ARGS="
    --data-path $DATA_PATH \
    --split 100,0,0 \
    --num-dataset-builder-threads 64 \
    --num-workers 32 \
    --data-cache-path /mnt/models/dataset_cache_final \
"

OUTPUT_ARGS="
    --log-interval 10 \
    --save-interval 1000 \
    --eval-interval 10000 \
    --eval-iters 0 \
"

WANDB_ARGS=""  # W&B 暂时屏蔽，先测试训练与恢复

# nohup torchrun $DISTRIBUTED_ARGS pretrain_gpt.py \
#     $GPT_ARGS \
#     $DATA_ARGS \
#     $OUTPUT_ARGS \
#     $OPTIMIZE_ARGS \
#     $TRAIN_ARGS \
#     $WANDB_ARGS \
#     $MODEL_PARALLEL_ARGS \
#     --distributed-backend nccl \
#     --log-throughput \
#     --load ${CKPT_LOAD_DIR} \
#     --save ${CKPT_SAVE_DIR} \
#     > ${LOG_FILE} 2>&1 &


torchrun $DISTRIBUTED_ARGS pretrain_gpt.py \
    $GPT_ARGS \
    $DATA_ARGS \
    $OUTPUT_ARGS \
    $OPTIMIZE_ARGS \
    $TRAIN_ARGS \
    $WANDB_ARGS \
    $MODEL_PARALLEL_ARGS \
    --distributed-backend nccl \
    --log-throughput \
    --save ${CKPT_SAVE_DIR} \
    --load ${CKPT_LOAD_DIR} \
    | tee ${LOG_FILE}
