#include "utils/diag_event_publisher.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <system_error>

namespace mooncake {
namespace {

std::string GetEnvString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : std::string(value);
}

size_t GetEnvSize(const char* name, size_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) {
        return fallback;
    }
    return static_cast<size_t>(parsed);
}

void AppendEscaped(std::string& out, const std::string& value) {
    out.push_back('"');
    for (unsigned char c : value) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (c < 0x20) {
                    char buffer[7];
                    std::snprintf(buffer, sizeof(buffer), "\\u%04x", c);
                    out += buffer;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    out.push_back('"');
}

class JsonLine {
   public:
    JsonLine(const DiagConfig& config, const char* event, uint64_t seq) {
        line_.reserve(256);
        line_.push_back('{');
        AddString("event", event);
        line_ += ",\"ts_ns\":";
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        line_ += std::to_string(static_cast<long long>(now));
        line_ += ",\"seq\":";
        line_ += std::to_string(static_cast<unsigned long long>(seq));
        line_ += ",\"pid\":";
        line_ += std::to_string(static_cast<long long>(::getpid()));
        AddString("node", config.node_name);
    }

    JsonLine& AddString(const char* name, const std::string& value) {
        Separator();
        AppendEscaped(line_, name);
        line_.push_back(':');
        AppendEscaped(line_, value);
        return *this;
    }

    JsonLine& AddInt(const char* name, long long value) {
        Separator();
        AppendEscaped(line_, name);
        line_.push_back(':');
        line_ += std::to_string(value);
        return *this;
    }

    JsonLine& AddReplicas(const char* name,
                          const std::vector<DiagReplicaInfo>& replicas) {
        Separator();
        AppendEscaped(line_, name);
        line_ += ":[";
        for (size_t i = 0; i < replicas.size(); ++i) {
            if (i > 0) {
                line_.push_back(',');
            }
            line_.push_back('{');
            const auto& replica = replicas[i];
            AppendEscaped(line_, "replica_id");
            line_.push_back(':');
            AppendEscaped(line_, replica.replica_id);
            line_.push_back(',');
            AppendEscaped(line_, "segment_id");
            line_.push_back(':');
            AppendEscaped(line_, replica.segment_id);
            line_.push_back(',');
            AppendEscaped(line_, "segment_name");
            line_.push_back(':');
            AppendEscaped(line_, replica.segment_name);
            line_.push_back(',');
            AppendEscaped(line_, "kind");
            line_.push_back(':');
            AppendEscaped(line_, replica.kind);
            line_.push_back(',');
            AppendEscaped(line_, "status");
            line_.push_back(':');
            AppendEscaped(line_, replica.status);
            line_.push_back('}');
        }
        line_.push_back(']');
        return *this;
    }

    std::string Finish() {
        line_.push_back('}');
        line_.push_back('\n');
        return std::move(line_);
    }

   private:
    void Separator() { line_.push_back(','); }

    std::string line_;
};

}  // namespace

DiagConfig DiagConfig::FromEnv() {
    DiagConfig config;
    config.directory = GetEnvString("MOONCAKE_DIAG_DIR", "");
    config.enabled = !config.directory.empty();
    config.node_name = GetEnvString("MOONCAKE_DIAG_NODE", "");
    config.overflow_policy =
        GetEnvString("MOONCAKE_DIAG_OVERFLOW_POLICY", "drop");
    config.queue_events = GetEnvSize("MOONCAKE_DIAG_QUEUE_EVENTS", 65536);
    config.queue_bytes = GetEnvSize("MOONCAKE_DIAG_QUEUE_BYTES", 64ull << 20);
    config.max_file_bytes =
        GetEnvSize("MOONCAKE_DIAG_MAX_FILE_BYTES", 512ull << 20);
    config.keep_files = GetEnvSize("MOONCAKE_DIAG_KEEP_FILES", 8);
    config.block_timeout_ms =
        GetEnvSize("MOONCAKE_DIAG_BLOCK_TIMEOUT_MS", 5000);
    return config;
}

DiagEventPublisher::DiagEventPublisher(DiagConfig config)
    : config_(std::move(config)) {
    if (!config_.enabled) {
        return;
    }
    if (config_.overflow_policy != "drop" &&
        config_.overflow_policy != "block") {
        config_.overflow_policy = "drop";
    }
    if (config_.keep_files == 0) {
        config_.keep_files = 1;
    }
    std::error_code error;
    std::filesystem::create_directories(config_.directory, error);
    if (error) {
        config_.enabled = false;
        return;
    }
    worker_ = std::thread(&DiagEventPublisher::WorkerLoop, this);
}

DiagEventPublisher::~DiagEventPublisher() {
    if (!config_.enabled) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    queue_cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_ != nullptr) {
        std::fflush(file_);
        std::fclose(file_);
        file_ = nullptr;
    }
}

void DiagEventPublisher::PublishObjectStored(
    const std::string& key, const std::string& tenant, uint64_t size,
    const std::vector<DiagReplicaInfo>& replicas) {
    if (!config_.enabled) {
        return;
    }
    JsonLine line(config_, "object_stored", seq_.fetch_add(1) + 1);
    line.AddString("key", key)
        .AddString("tenant", tenant)
        .AddInt("size", static_cast<long long>(size))
        .AddInt("replica_count", static_cast<long long>(replicas.size()))
        .AddReplicas("replicas", replicas);
    Enqueue(line.Finish());
}

void DiagEventPublisher::PublishReplicasRemoved(
    const std::string& key, const std::string& tenant,
    const std::vector<DiagReplicaInfo>& removed, size_t remaining_replicas,
    const std::string& reason, const std::string& detail) {
    if (!config_.enabled) {
        return;
    }
    JsonLine line(config_, "replicas_removed", seq_.fetch_add(1) + 1);
    line.AddString("key", key)
        .AddString("tenant", tenant)
        .AddString("reason", reason)
        .AddString("detail", detail)
        .AddInt("removed_count", static_cast<long long>(removed.size()))
        .AddInt("remaining_replicas",
                static_cast<long long>(remaining_replicas))
        .AddReplicas("replicas", removed);
    Enqueue(line.Finish());
}

void DiagEventPublisher::PublishObjectRemoved(
    const std::string& key, const std::string& tenant,
    const std::vector<DiagReplicaInfo>& replicas,
    const std::string& reason) {
    if (!config_.enabled) {
        return;
    }
    JsonLine line(config_, "object_removed", seq_.fetch_add(1) + 1);
    line.AddString("key", key)
        .AddString("tenant", tenant)
        .AddString("reason", reason)
        .AddInt("removed_count", static_cast<long long>(replicas.size()))
        .AddInt("remaining_replicas", 0)
        .AddReplicas("replicas", replicas);
    Enqueue(line.Finish());
}

void DiagEventPublisher::PublishSegmentEvent(
    const std::string& segment_id, const std::string& segment_name,
    const std::string& endpoint, const std::string& phase,
    const std::string& reason) {
    if (!config_.enabled) {
        return;
    }
    JsonLine line(config_, "segment_event", seq_.fetch_add(1) + 1);
    line.AddString("segment_id", segment_id)
        .AddString("segment_name", segment_name)
        .AddString("endpoint", endpoint)
        .AddString("phase", phase)
        .AddString("reason", reason);
    Enqueue(line.Finish());
}

void DiagEventPublisher::Enqueue(std::string line) {
    const size_t size = line.size();
    std::unique_lock<std::mutex> lock(mutex_);
    if (stop_) {
        stats_.dropped++;
        return;
    }
    if (!HasRoomLocked(size)) {
        if (config_.overflow_policy == "drop") {
            stats_.dropped++;
            return;
        }
        stats_.blocked++;
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(config_.block_timeout_ms);
        while (!HasRoomLocked(size) && !stop_) {
            if (queue_cv_.wait_until(lock, deadline) ==
                std::cv_status::timeout) {
                break;
            }
        }
        if (!HasRoomLocked(size)) {
            stats_.dropped++;
            stats_.block_timeouts++;
            return;
        }
    }
    queue_.push_back(std::move(line));
    queued_bytes_ += size;
    pending_++;
    lock.unlock();
    queue_cv_.notify_all();
}

bool DiagEventPublisher::HasRoomLocked(size_t size) const {
    return queue_.size() < config_.queue_events &&
           queued_bytes_ + size <= config_.queue_bytes;
}

void DiagEventPublisher::WorkerLoop() {
    while (true) {
        std::string line;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
            if (queue_.empty()) {
                if (stop_) {
                    break;
                }
                continue;
            }
            line = std::move(queue_.front());
            queue_.pop_front();
            queued_bytes_ -= line.size();
        }
        queue_cv_.notify_all();

        bool ok = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            try {
                if (file_ == nullptr ||
                    (file_bytes_ > 0 &&
                     file_bytes_ + line.size() > config_.max_file_bytes)) {
                    OpenNewFileLocked();
                }
                if (file_ == nullptr ||
                    std::fwrite(line.data(), 1, line.size(), file_) !=
                        line.size()) {
                    ok = false;
                } else {
                    file_bytes_ += line.size();
                }
            } catch (...) {
                ok = false;
            }
            if (ok) {
                stats_.written++;
            } else {
                stats_.write_errors++;
            }
            pending_--;
        }
        drain_cv_.notify_all();
    }
}

void DiagEventPublisher::OpenNewFileLocked() {
    if (file_ != nullptr) {
        std::fflush(file_);
        std::fclose(file_);
        file_ = nullptr;
        ShiftHistoryLocked();
    }
    std::string base = config_.directory + "/master_diag";
    if (!config_.node_name.empty()) {
        base += "_" + config_.node_name;
    }
    base += "_" + std::to_string(static_cast<long long>(::getpid())) + ".jsonl";
    current_path_ = base;
    file_ = std::fopen(base.c_str(), "a");
    file_bytes_ = 0;
    if (file_ != nullptr) {
        if (std::fseek(file_, 0, SEEK_END) == 0) {
            long size = std::ftell(file_);
            if (size > 0) {
                file_bytes_ = static_cast<size_t>(size);
            }
        }
    }
    PruneHistoryLocked();
}

void DiagEventPublisher::ShiftHistoryLocked() {
    if (current_path_.empty()) {
        return;
    }
    for (size_t index = config_.keep_files - 1; index > 0; --index) {
        std::string source = current_path_ + "." + std::to_string(index);
        std::string target = current_path_ + "." + std::to_string(index + 1);
        std::error_code error;
        if (std::filesystem::exists(source, error)) {
            std::filesystem::rename(source, target, error);
        }
    }
    std::error_code error;
    std::filesystem::rename(current_path_, current_path_ + ".1", error);
}

void DiagEventPublisher::PruneHistoryLocked() {
    if (current_path_.empty()) {
        return;
    }
    for (size_t index = config_.keep_files;
         index < config_.keep_files + 100; ++index) {
        std::string stale = current_path_ + "." + std::to_string(index);
        std::error_code error;
        std::filesystem::remove(stale, error);
    }
}

bool DiagEventPublisher::Flush(uint64_t timeout_ms) {
    if (!config_.enabled) {
        return true;
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lock(mutex_);
    while (pending_ > 0) {
        if (drain_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            return false;
        }
    }
    if (file_ != nullptr) {
        std::fflush(file_);
    }
    return true;
}

DiagEventPublisher::Stats DiagEventPublisher::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

std::string DiagEventPublisher::current_path() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_path_;
}

}  // namespace mooncake
