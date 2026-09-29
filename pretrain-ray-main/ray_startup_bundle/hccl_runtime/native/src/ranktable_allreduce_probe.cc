#include <acl/acl.h>
#include <hccl/hccl.h>

#include <cerrno>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/prctl.h>
#include <unistd.h>

namespace {

struct Options {
    std::string rankTable;
    uint32_t rankId = 0;
    int32_t deviceId = 0;
    uint32_t worldSize = 0;
    uint64_t count = 4096;
};

uint64_t ParseUnsigned(const char *value, const char *name, uint64_t maximum)
{
    if (value == nullptr || value[0] == '\0' || value[0] == '-') {
        throw std::runtime_error(std::string(name) + " must be a non-negative integer");
    }
    for (const unsigned char *cursor = reinterpret_cast<const unsigned char *>(value);
         *cursor != '\0'; ++cursor) {
        if (!std::isdigit(*cursor)) {
            throw std::runtime_error(std::string(name) + " must contain only decimal digits");
        }
    }
    char *end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed > maximum) {
        throw std::runtime_error(std::string(name) + " is outside the supported range");
    }
    return static_cast<uint64_t>(parsed);
}

Options ParseOptions(int argc, char **argv)
{
    Options options;
    const char *rankTableEnv = std::getenv("RANK_TABLE_FILE");
    if (rankTableEnv != nullptr) {
        options.rankTable = rankTableEnv;
    }

    bool haveRank = false;
    bool haveDevice = false;
    bool haveWorld = false;
    for (int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        if (argument == "--help") {
            std::cout
                << "Usage: ranktable_allreduce_probe --rank-id N --device-id N "
                   "--world-size N [--rank-table PATH] [--count N]\n";
            std::exit(0);
        }
        if (index + 1 >= argc) {
            throw std::runtime_error("missing value after " + argument);
        }
        const char *value = argv[++index];
        if (argument == "--rank-table") {
            options.rankTable = value;
        } else if (argument == "--rank-id") {
            options.rankId = static_cast<uint32_t>(
                ParseUnsigned(value, "--rank-id", std::numeric_limits<uint32_t>::max()));
            haveRank = true;
        } else if (argument == "--device-id") {
            options.deviceId = static_cast<int32_t>(
                ParseUnsigned(value, "--device-id", std::numeric_limits<int32_t>::max()));
            haveDevice = true;
        } else if (argument == "--world-size") {
            options.worldSize = static_cast<uint32_t>(ParseUnsigned(value, "--world-size", 65535));
            haveWorld = true;
        } else if (argument == "--count") {
            options.count = ParseUnsigned(value, "--count", 1024ULL * 1024ULL);
        } else {
            throw std::runtime_error("unsupported argument: " + argument);
        }
    }

    if (!haveRank || !haveDevice || !haveWorld) {
        throw std::runtime_error("--rank-id, --device-id, and --world-size are required");
    }
    if (options.rankTable.empty()) {
        throw std::runtime_error("RANK_TABLE_FILE or --rank-table is required");
    }
    if (options.rankTable.size() + 1U > 4096U) {
        throw std::runtime_error("rank-table path exceeds CANN's 4096-byte limit");
    }
    if (options.worldSize == 0 || options.rankId >= options.worldSize) {
        throw std::runtime_error("rank ID must be smaller than the positive world size");
    }
    if (options.count == 0) {
        throw std::runtime_error("--count must be greater than zero");
    }
    const uint64_t expected =
        static_cast<uint64_t>(options.worldSize) * (options.worldSize + 1ULL) / 2ULL;
    if (expected > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
        throw std::runtime_error("world size makes the INT32 expected sum overflow");
    }
    std::ifstream rankTable(options.rankTable.c_str(), std::ios::binary);
    if (!rankTable.good()) {
        throw std::runtime_error("rank table is not readable: " + options.rankTable);
    }
    return options;
}

class Resources {
public:
    explicit Resources(int32_t deviceId) : deviceId_(deviceId) {}

    Resources(const Resources &) = delete;
    Resources &operator=(const Resources &) = delete;

    ~Resources()
    {
        Cleanup();
    }

    bool Cleanup()
    {
        if (cleaned_) {
            return cleanupOk_;
        }
        cleaned_ = true;
        if (stream_ != nullptr && streamMayHaveWork_) {
            if (aclrtSynchronizeStream(stream_) != ACL_SUCCESS) {
                std::cerr << "aclrtSynchronizeStream during cleanup failed; "
                             "leaving ACL buffers and stream to process teardown\n";
                cleanupOk_ = false;
                if (comm_ != nullptr) {
                    const HcclResult result = HcclCommDestroy(comm_);
                    if (result != HCCL_SUCCESS) {
                        std::cerr << "HcclCommDestroy failed: "
                                  << static_cast<int>(result) << '\n';
                    }
                    comm_ = nullptr;
                }
                return false;
            }
            streamMayHaveWork_ = false;
        }
        if (comm_ != nullptr) {
            const HcclResult result = HcclCommDestroy(comm_);
            if (result != HCCL_SUCCESS) {
                std::cerr << "HcclCommDestroy failed: " << static_cast<int>(result) << '\n';
                cleanupOk_ = false;
            }
            comm_ = nullptr;
        }
        if (sendBuffer_ != nullptr && aclrtFree(sendBuffer_) != ACL_SUCCESS) {
            std::cerr << "aclrtFree(sendBuffer) failed\n";
            cleanupOk_ = false;
        }
        sendBuffer_ = nullptr;
        if (receiveBuffer_ != nullptr && aclrtFree(receiveBuffer_) != ACL_SUCCESS) {
            std::cerr << "aclrtFree(receiveBuffer) failed\n";
            cleanupOk_ = false;
        }
        receiveBuffer_ = nullptr;
        if (hostBuffer_ != nullptr && aclrtFreeHost(hostBuffer_) != ACL_SUCCESS) {
            std::cerr << "aclrtFreeHost failed\n";
            cleanupOk_ = false;
        }
        hostBuffer_ = nullptr;
        if (stream_ != nullptr && aclrtDestroyStream(stream_) != ACL_SUCCESS) {
            std::cerr << "aclrtDestroyStream failed\n";
            cleanupOk_ = false;
        }
        stream_ = nullptr;
        if (deviceSet_ && aclrtResetDevice(deviceId_) != ACL_SUCCESS) {
            std::cerr << "aclrtResetDevice failed\n";
            cleanupOk_ = false;
        }
        deviceSet_ = false;
        if (aclInitialized_ && aclFinalize() != ACL_SUCCESS) {
            std::cerr << "aclFinalize failed\n";
            cleanupOk_ = false;
        }
        aclInitialized_ = false;
        return cleanupOk_;
    }

    bool aclInitialized_ = false;
    bool deviceSet_ = false;
    aclrtStream stream_ = nullptr;
    HcclComm comm_ = nullptr;
    void *sendBuffer_ = nullptr;
    void *receiveBuffer_ = nullptr;
    void *hostBuffer_ = nullptr;
    bool streamMayHaveWork_ = false;

private:
    int32_t deviceId_;
    bool cleaned_ = false;
    bool cleanupOk_ = true;
};

bool CheckAcl(aclError result, const char *operation)
{
    if (result == ACL_SUCCESS) {
        return true;
    }
    std::cerr << operation << " failed: " << static_cast<int>(result) << '\n';
    return false;
}

bool CheckHccl(HcclResult result, const char *operation)
{
    if (result == HCCL_SUCCESS) {
        return true;
    }
    std::cerr << operation << " failed: " << static_cast<int>(result) << '\n';
    return false;
}

int Run(const Options &options)
{
    Resources resources(options.deviceId);
    if (!CheckAcl(aclInit(nullptr), "aclInit")) {
        return 1;
    }
    resources.aclInitialized_ = true;

    uint32_t deviceCount = 0;
    if (!CheckAcl(aclrtGetDeviceCount(&deviceCount), "aclrtGetDeviceCount")) {
        return 1;
    }
    if (options.deviceId < 0 || static_cast<uint32_t>(options.deviceId) >= deviceCount) {
        std::cerr << "device " << options.deviceId << " is outside visible device count "
                  << deviceCount << '\n';
        return 1;
    }
    if (!CheckAcl(aclrtSetDevice(options.deviceId), "aclrtSetDevice")) {
        return 1;
    }
    resources.deviceSet_ = true;

    if (!CheckHccl(
            HcclCommInitClusterInfo(options.rankTable.c_str(), options.rankId, &resources.comm_),
            "HcclCommInitClusterInfo")) {
        return 1;
    }
    uint32_t actualWorldSize = 0;
    uint32_t actualRankId = 0;
    if (!CheckHccl(
            HcclGetRankSize(resources.comm_, &actualWorldSize), "HcclGetRankSize")) {
        return 1;
    }
    if (!CheckHccl(HcclGetRankId(resources.comm_, &actualRankId), "HcclGetRankId")) {
        return 1;
    }
    if (actualWorldSize != options.worldSize || actualRankId != options.rankId) {
        std::cerr << "communicator identity mismatch: expected rank=" << options.rankId
                  << " world_size=" << options.worldSize << " actual rank=" << actualRankId
                  << " world_size=" << actualWorldSize << '\n';
        return 1;
    }
    if (!CheckAcl(aclrtCreateStream(&resources.stream_), "aclrtCreateStream")) {
        return 1;
    }
    resources.streamMayHaveWork_ = true;

    const size_t bytes = static_cast<size_t>(options.count) * sizeof(int32_t);
    if (!CheckAcl(
            aclrtMalloc(&resources.sendBuffer_, bytes, ACL_MEM_MALLOC_HUGE_FIRST),
            "aclrtMalloc(sendBuffer)")) {
        return 1;
    }
    if (!CheckAcl(
            aclrtMalloc(&resources.receiveBuffer_, bytes, ACL_MEM_MALLOC_HUGE_FIRST),
            "aclrtMalloc(receiveBuffer)")) {
        return 1;
    }
    if (!CheckAcl(aclrtMallocHost(&resources.hostBuffer_, bytes), "aclrtMallocHost")) {
        return 1;
    }

    int32_t *host = static_cast<int32_t *>(resources.hostBuffer_);
    const int32_t inputValue = static_cast<int32_t>(options.rankId + 1U);
    for (uint64_t index = 0; index < options.count; ++index) {
        host[index] = inputValue;
    }
    if (!CheckAcl(
            aclrtMemcpy(
                resources.sendBuffer_,
                bytes,
                resources.hostBuffer_,
                bytes,
                ACL_MEMCPY_HOST_TO_DEVICE),
            "aclrtMemcpy(host-to-device)")) {
        return 1;
    }

    resources.streamMayHaveWork_ = true;
    if (!CheckHccl(
            HcclAllReduce(
                resources.sendBuffer_,
                resources.receiveBuffer_,
                options.count,
                HCCL_DATA_TYPE_INT32,
                HCCL_REDUCE_SUM,
                resources.comm_,
                resources.stream_),
            "HcclAllReduce")) {
        return 1;
    }
    if (!CheckAcl(aclrtSynchronizeStream(resources.stream_), "aclrtSynchronizeStream")) {
        return 1;
    }
    resources.streamMayHaveWork_ = false;
    if (!CheckAcl(
            aclrtMemcpy(
                resources.hostBuffer_,
                bytes,
                resources.receiveBuffer_,
                bytes,
                ACL_MEMCPY_DEVICE_TO_HOST),
            "aclrtMemcpy(device-to-host)")) {
        return 1;
    }

    const int32_t expected = static_cast<int32_t>(
        static_cast<uint64_t>(options.worldSize) * (options.worldSize + 1ULL) / 2ULL);
    for (uint64_t index = 0; index < options.count; ++index) {
        if (host[index] != expected) {
            std::cerr << "AllReduce mismatch rank=" << options.rankId
                      << " device=" << options.deviceId << " index=" << index
                      << " expected=" << expected << " actual=" << host[index] << '\n';
            return 1;
        }
    }

    if (!resources.Cleanup()) {
        return 1;
    }
    std::cout << "RANKTABLE_HCCL_PASS rank=" << options.rankId
              << " device=" << options.deviceId << " world_size=" << options.worldSize
              << " count=" << options.count << " expected=" << expected << '\n';
    return 0;
}

}  // namespace

int main(int argc, char **argv)
{
    try {
        if (prctl(
                PR_SET_PDEATHSIG,
                static_cast<unsigned long>(SIGKILL),
                0UL,
                0UL,
                0UL) != 0) {
            throw std::runtime_error("could not install parent-death signal");
        }
        if (getppid() == 1) {
            throw std::runtime_error("Ray supervisor exited before probe initialization");
        }
        return Run(ParseOptions(argc, argv));
    } catch (const std::exception &error) {
        std::cerr << "ranktable_allreduce_probe: " << error.what() << '\n';
        return 2;
    }
}
