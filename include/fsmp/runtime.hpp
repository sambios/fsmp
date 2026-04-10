#ifndef CPPFSMP_RUNTIME_HPP
#define CPPFSMP_RUNTIME_HPP

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace fsmp {

using InstanceId = std::uint64_t;
using NodeId = std::uint32_t;
using ContainerId = std::uint32_t;
using TimerId = std::uint32_t;
using NameCode = std::uint32_t;

enum class EventType : std::uint32_t {
    NodeConnected = 1000,
    NodeDisconnected = 1001,
    NameQuery = 1002,
    NameQueryReply = 1003,
    Timer = 1004,
    UserBase = 2000,
};

enum class TimerMode : std::uint8_t {
    Once = 0,
    Cycle = 1,
};

struct QueueStats {
    std::uint32_t currentMessageCount {0};
    std::uint32_t receivedMessageCount {0};
    std::uint32_t lostMessageCount {0};
    std::uint32_t processedMessageCount {0};
    std::uint32_t peakMessageCount {0};
    std::uint32_t peakMessageSize {0};
};

struct ContainerStats {
    QueueStats fsmQueue {};
};

struct InstanceOptions {
    std::uint16_t localPort {0};
    std::uint16_t telnetPort {0};
    std::uint32_t maxNodeCount {32};
    std::uint32_t maxClientNodeCount {32};
    std::uint32_t maxContainerCount {32};
    std::uint32_t applicationData {0};
};

enum class LogLevel : std::uint8_t {
    Debug = 0,
    Info = 1,
    Error = 2,
};

struct NodeOptions {
    std::uint16_t sendQueueSize {128};
    std::uint16_t receiveQueueSize {128};
    std::uint16_t heartbeatIntervalSeconds {5};
    std::uint16_t heartbeatRetryCount {3};
};

struct RemoteContainerInfo {
    NodeId remoteNodeId {0};
    ContainerId remoteContainerId {0};
    NameCode nameCode {0};
};

struct Message {
    NodeId sourceNodeId {0};
    ContainerId sourceContainerId {0};
    std::uint32_t sourceObjectId {0};
    NodeId destinationNodeId {0};
    ContainerId destinationContainerId {0};
    std::uint32_t destinationObjectId {0};
    EventType eventType {EventType::UserBase};
    TimerId timerId {0};
    std::uint32_t timerContext {0};
    NameCode destinationNameCode {0};
    std::vector<std::uint8_t> payload {};

    std::string payloadAsString() const;
};

struct ContainerOptions {
    NameCode nameCode {0};
    std::uint8_t priority {90};
    std::uint32_t stackSize {0};
    std::uint32_t objectCount {0};
    std::uint32_t objectSize {0};
    std::uint32_t messageQueueCapacityHint {64};
    std::function<void(const Message &)> onMessage;
};

class Instance;

class Runtime {
public:
    static Runtime &shared();

    void registerServer(std::uint16_t port, Instance *instance);
    void unregisterServer(std::uint16_t port, Instance *instance);
    Instance *findServer(std::uint16_t port);

private:
    Runtime() = default;

    std::mutex mutex_;
    std::unordered_map<std::uint16_t, Instance *> servers_;
};

class Instance {
public:
    using TelnetCommandHandler =
        std::function<int(Instance &, const std::vector<std::string> &, std::string &)>;

    explicit Instance(InstanceOptions options);
    ~Instance();

    Instance(const Instance &) = delete;
    Instance &operator=(const Instance &) = delete;

    InstanceId id() const noexcept;
    std::uint32_t applicationData() const noexcept;

    void startServer(const NodeOptions &options);
    void stopServer();
    bool isServerRunning() const noexcept;

    ContainerId createContainer(const ContainerOptions &options);
    void deleteContainer(ContainerId containerId);

    template <typename T>
    T *getObject(ContainerId containerId, std::uint32_t objectId) {
        return static_cast<T *>(getObjectMemory(containerId, objectId));
    }

    void *getObjectMemory(ContainerId containerId, std::uint32_t objectId);

    NodeId createNode(const NodeOptions &options);
    void deleteNode(NodeId nodeId);
    void connectNode(NodeId nodeId, const std::string &ipAddress, std::uint16_t serverPort);
    void disconnectNode(NodeId nodeId);

    void sendMessage(const Message &message);
    std::optional<RemoteContainerInfo> queryRemoteContainer(NodeId nodeId, NameCode nameCode);

    TimerId setTimer(ContainerId containerId, std::uint32_t delayMs, TimerMode mode, std::uint32_t context);
    void killTimer(ContainerId containerId, TimerId timerId);

    ContainerStats getContainerStats(ContainerId containerId) const;

    void startTelnetServer();
    void stopTelnetServer();
    bool isTelnetServerRunning() const noexcept;
    std::uint16_t telnetPort() const noexcept;
    void registerTelnetCommand(std::string name, std::string usage, TelnetCommandHandler handler);
    void setTelnetPrompt(std::string prompt);
    std::string telnetPrompt() const;
    void setTelnetShellCommandEnabled(bool enabled);
    bool isTelnetShellCommandEnabled() const noexcept;
    void setTelnetShellCommandTimeout(std::chrono::milliseconds timeout);
    std::chrono::milliseconds telnetShellCommandTimeout() const noexcept;
    void log(LogLevel level, std::string_view message);

private:
    struct TelnetState;
    struct PendingNameQuery {
        bool completed {false};
        std::optional<RemoteContainerInfo> result;
    };

    struct NodeRecord {
        NodeOptions options {};
        bool isAcceptedPeer {false};
        bool isConnected {false};
        bool localCloseRequested {false};
        std::string peerIpAddress {"127.0.0.1"};
        std::uint16_t peerPort {0};
        int socketFd {-1};
        std::thread readerThread;
        std::uint32_t nextQueryId {1};
        std::unordered_map<std::uint32_t, PendingNameQuery> pendingQueries;
    };

    struct ContainerRecord {
        ContainerOptions options {};
        std::vector<std::uint8_t> objectMemory {};
        ContainerStats stats {};
    };

    struct TimerRecord {
        ContainerId containerId {0};
        TimerMode mode {TimerMode::Once};
        std::uint32_t delayMs {0};
        std::uint32_t context {0};
        std::shared_ptr<std::atomic<bool>> cancelled;
    };

    struct QueuedMessage {
        ContainerId containerId {0};
        Message message {};
    };

    void workerLoop();
    void enqueueLocalMessage(QueuedMessage message);
    void dispatchRemoteMessage(NodeId destinationNodeId, const Message &message);
    void broadcastSystemEvent(EventType eventType, NodeId nodeId);
    ContainerId findContainerByName(NameCode nameCode) const;
    NodeId allocateNodeIdLocked(const NodeOptions &options, bool isAcceptedPeer);
    void startNodeReader(NodeId nodeId);
    void readerLoop(NodeId nodeId);
    void handleSocketClosed(NodeId nodeId);
    void sendUserPacket(NodeId nodeId, const Message &message);
    void sendNameQueryPacket(NodeId nodeId, std::uint32_t requestId, NameCode nameCode);
    void sendNameQueryReplyPacket(NodeId nodeId, std::uint32_t requestId, NameCode nameCode, ContainerId containerId);
    void shutdownNodeSocketLocked(NodeRecord &node);
    void joinNodeThread(std::thread &thread);

    NodeRecord &requireNode(NodeId nodeId);
    const NodeRecord &requireNode(NodeId nodeId) const;
    ContainerRecord &requireContainer(ContainerId containerId);
    const ContainerRecord &requireContainer(ContainerId containerId) const;

    InstanceId id_ {0};
    InstanceOptions options_ {};

    mutable std::mutex mutex_;
    std::condition_variable queueCondition_;
    std::condition_variable stateCondition_;
    std::thread workerThread_;
    std::thread acceptThread_;
    bool exiting_ {false};

    std::uint16_t serverPort_ {0};
    int serverListenSocket_ {-1};

    std::vector<std::optional<NodeRecord>> nodes_;
    std::vector<std::optional<ContainerRecord>> containers_;
    std::unordered_map<TimerId, TimerRecord> timers_;
    std::deque<QueuedMessage> queue_;

    std::atomic<TimerId> nextTimerId_ {1};
    std::unique_ptr<TelnetState> telnet_;
};

NameCode makeNameCode(char a, char b, char c, char d) noexcept;

}  // namespace fsmp

#endif
