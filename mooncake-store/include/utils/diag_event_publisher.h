#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

/// One replica of an object, copied out while the metadata lock is held.
struct DiagReplicaInfo {
    std::string replica_id;
    std::string segment_id;
    std::string segment_name;
    std::string kind;
    std::string status;
};

/// Diagnostic sink configuration, read once from the environment.
struct DiagConfig {
    bool enabled = false;
    std::string directory;
    std::string node_name;
    /// "drop" protects the data path; "block" protects evidence completeness.
    std::string overflow_policy = "drop";
    size_t queue_events = 65536;
    size_t queue_bytes = 64ull << 20;
    size_t max_file_bytes = 256ull << 20;
    size_t keep_files = 8;
    uint64_t block_timeout_ms = 5000;

    static DiagConfig FromEnv();
};

/// Append-only JSONL sink for object/segment lifecycle events.
///
/// Publish* only formats a line and enqueues it; file IO and rotation run on a
/// background thread. Call Publish* outside metadata locks when using the
/// "block" overflow policy, otherwise a full queue serializes the data path.
class DiagEventPublisher {
   public:
    explicit DiagEventPublisher(DiagConfig config);
    ~DiagEventPublisher();

    DiagEventPublisher(const DiagEventPublisher&) = delete;
    DiagEventPublisher& operator=(const DiagEventPublisher&) = delete;

    bool enabled() const { return config_.enabled; }

    /// Master-side publishers run under metadata/client locks. Turning blocking
    /// off makes a full queue degrade to "drop" instead of parking the data
    /// path while it waits for the writer thread.
    void set_block_on_full(bool value) { block_on_full_.store(value); }

    void PublishObjectStored(const std::string& key, const std::string& tenant,
                             uint64_t size,
                             const std::vector<DiagReplicaInfo>& replicas);
    void PublishReplicasRemoved(const std::string& key,
                                const std::string& tenant,
                                const std::vector<DiagReplicaInfo>& removed,
                                size_t remaining_replicas,
                                const std::string& reason,
                                const std::string& detail = "");
    void PublishObjectRemoved(const std::string& key, const std::string& tenant,
                              const std::vector<DiagReplicaInfo>& replicas,
                              const std::string& reason);
    void PublishSegmentEvent(const std::string& segment_id,
                             const std::string& segment_name,
                             const std::string& endpoint,
                             const std::string& phase,
                             const std::string& reason);

    /// Wait until every enqueued line is written and flushed.
    bool Flush(uint64_t timeout_ms = 5000);

    struct Stats {
        uint64_t written = 0;
        uint64_t dropped = 0;
        uint64_t blocked = 0;
        uint64_t block_timeouts = 0;
        uint64_t write_errors = 0;
    };
    Stats GetStats() const;

    /// Current file path; empty until the first write.
    std::string current_path() const;

   private:
    void Enqueue(std::string line);
    void WorkerLoop();
    void OpenNewFileLocked();
    void ShiftHistoryLocked();
    void PruneHistoryLocked();
    bool HasRoomLocked(size_t size) const;

    DiagConfig config_;
    mutable std::mutex mutex_;
    std::condition_variable queue_cv_;
    std::condition_variable drain_cv_;
    std::deque<std::string> queue_;
    size_t queued_bytes_ = 0;
    uint64_t pending_ = 0;
    bool stop_ = false;
    std::thread worker_;

    Stats stats_;
    std::string current_path_;
    FILE* file_ = nullptr;
    size_t file_bytes_ = 0;
    std::atomic<uint64_t> seq_{0};
    std::atomic<bool> block_on_full_{true};
    // Drops taken without acquiring mutex_ on the master-side fast path.
    std::atomic<uint64_t> fast_drops_{0};
};

}  // namespace mooncake
