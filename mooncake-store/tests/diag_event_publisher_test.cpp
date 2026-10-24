#include "utils/diag_event_publisher.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace mooncake {
namespace {

std::string TempDir(const std::string& name) {
    auto path = std::filesystem::temp_directory_path() / ("mooncake_diag_" + name);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

std::string ReadAll(const std::string& path) {
    std::ifstream input(path);
    std::stringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

size_t CountLines(const std::string& text) {
    size_t lines = 0;
    for (char c : text) {
        if (c == '\n') {
            ++lines;
        }
    }
    return lines;
}

DiagReplicaInfo MakeReplica(const std::string& id, const std::string& segment) {
    DiagReplicaInfo info;
    info.replica_id = id;
    info.segment_id = segment;
    info.segment_name = "seg-" + segment;
    info.kind = "memory";
    info.status = "complete";
    return info;
}

TEST(DiagEventPublisherTest, DisabledWritesNothing) {
    DiagConfig config;
    config.enabled = false;
    config.directory = TempDir("disabled");
    DiagEventPublisher publisher(config);
    publisher.PublishObjectStored("key", "tenant", 1, {});
    EXPECT_FALSE(publisher.enabled());
    EXPECT_TRUE(publisher.current_path().empty());
}

TEST(DiagEventPublisherTest, WritesObjectAndSegmentEvents) {
    DiagConfig config;
    config.enabled = true;
    config.directory = TempDir("enabled");
    config.node_name = "master-test";
    DiagEventPublisher publisher(config);
    publisher.PublishObjectStored(
        "key-1", "tenant", 4096,
        {MakeReplica("r1", "s1"), MakeReplica("r2", "s2")});
    publisher.PublishReplicasRemoved("key-1", "tenant", {MakeReplica("r1", "s1")},
                                     1, "capacity_eviction");
    publisher.PublishObjectRemoved("key-2", "tenant", {}, "explicit_delete");
    publisher.PublishSegmentEvent("s1", "seg-s1", "10.0.0.1:1234", "unreadable",
                                  "unmount");
    ASSERT_TRUE(publisher.Flush(5000));

    const auto text = ReadAll(publisher.current_path());
    EXPECT_EQ(CountLines(text), 4u);
    EXPECT_NE(text.find("\"event\":\"object_stored\""), std::string::npos);
    EXPECT_NE(text.find("\"key\":\"key-1\""), std::string::npos);
    EXPECT_NE(text.find("\"replica_count\":2"), std::string::npos);
    EXPECT_NE(text.find("\"reason\":\"capacity_eviction\""), std::string::npos);
    EXPECT_NE(text.find("\"remaining_replicas\":1"), std::string::npos);
    EXPECT_NE(text.find("\"event\":\"segment_event\""), std::string::npos);
    EXPECT_NE(text.find("\"node\":\"master-test\""), std::string::npos);
}

TEST(DiagEventPublisherTest, FlushWritesEveryLine) {
    DiagConfig config;
    config.enabled = true;
    config.directory = TempDir("flush");
    DiagEventPublisher publisher(config);
    for (int i = 0; i < 500; ++i) {
        publisher.PublishObjectStored("key-" + std::to_string(i), "tenant", 1,
                                      {});
    }
    ASSERT_TRUE(publisher.Flush(5000));
    EXPECT_EQ(CountLines(ReadAll(publisher.current_path())), 500u);
}

TEST(DiagEventPublisherTest, EscapesKeys) {
    DiagConfig config;
    config.enabled = true;
    config.directory = TempDir("escape");
    DiagEventPublisher publisher(config);
    publisher.PublishObjectStored("weird\"key\\with\nnewline", "tenant", 1, {});
    ASSERT_TRUE(publisher.Flush(5000));
    const auto text = ReadAll(publisher.current_path());
    EXPECT_NE(text.find("\\\"key\\\\with\\nnewline"), std::string::npos);
}

TEST(DiagEventPublisherTest, RotatesAndKeepsBoundedHistory) {
    DiagConfig config;
    config.enabled = true;
    config.directory = TempDir("rotate");
    config.max_file_bytes = 400;
    config.keep_files = 3;
    DiagEventPublisher publisher(config);
    for (int i = 0; i < 200; ++i) {
        publisher.PublishObjectStored("key-" + std::to_string(i), "tenant", 1,
                                      {});
    }
    ASSERT_TRUE(publisher.Flush(5000));
    size_t files = 0;
    for (const auto& entry : std::filesystem::directory_iterator(config.directory)) {
        if (entry.is_regular_file()) {
            ++files;
        }
    }
    EXPECT_LE(files, config.keep_files);
}

}  // namespace
}  // namespace mooncake
