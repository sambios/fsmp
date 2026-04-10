#include "fsmp/runtime.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fsmp {

namespace {

std::atomic<InstanceId> g_nextInstanceId {1};

std::runtime_error makeError(const std::string &message) {
    return std::runtime_error("fsmp: " + message);
}

bool isReservedEventType(EventType eventType) {
    return static_cast<std::uint32_t>(eventType) < static_cast<std::uint32_t>(EventType::UserBase);
}

constexpr std::uint16_t kMinTelnetPort = 5000;
constexpr std::uint16_t kMaxTelnetPort = 6000;
constexpr int kInvalidSocket = -1;

enum class PacketType : std::uint32_t {
    UserMessage = 1,
    NameQuery = 2,
    NameQueryReply = 3,
};

struct PacketHeader {
    std::uint32_t packetType {0};
    std::uint32_t requestId {0};
    std::uint32_t sourceContainerId {0};
    std::uint32_t sourceObjectId {0};
    std::uint32_t destinationContainerId {0};
    std::uint32_t destinationObjectId {0};
    std::uint32_t eventType {0};
    std::uint32_t timerId {0};
    std::uint32_t timerContext {0};
    std::uint32_t nameCode {0};
    std::uint32_t payloadSize {0};
    std::uint32_t found {0};
};

std::uint32_t toNetwork(std::uint32_t value) {
    return htonl(value);
}

std::uint32_t fromNetwork(std::uint32_t value) {
    return ntohl(value);
}

std::string defaultTelnetPrompt() {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    const char *programName = ::getprogname();
    if (programName != nullptr && programName[0] != '\0') {
        return std::string(programName) + "$";
    }
#elif defined(__GLIBC__)
    if (::program_invocation_short_name != nullptr && ::program_invocation_short_name[0] != '\0') {
        return std::string(::program_invocation_short_name) + "$";
    }
#endif
    return "fsmp$";
}

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::vector<std::string> parseCommandLine(const std::string &line) {
    std::vector<std::string> tokens;
    std::string current;
    bool inQuotes = false;
    char quoteChar = '\0';

    for (char ch : line) {
        if ((ch == '"' || ch == '\'') && (!inQuotes || ch == quoteChar)) {
            if (inQuotes && ch == quoteChar) {
                inQuotes = false;
                quoteChar = '\0';
            } else if (!inQuotes) {
                inQuotes = true;
                quoteChar = ch;
            }
            continue;
        }
        if (!inQuotes && std::isspace(static_cast<unsigned char>(ch))) {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }
    return tokens;
}

std::string joinLines(const std::vector<std::string> &lines) {
    std::ostringstream stream;
    for (const auto &line : lines) {
        stream << line << '\n';
    }
    return stream.str();
}

void sendSocketText(int socketFd, std::string_view text) {
    if (socketFd == kInvalidSocket || text.empty()) {
        return;
    }

    std::size_t totalSent = 0;
    while (totalSent < text.size()) {
        const auto sent = ::send(socketFd, text.data() + totalSent, text.size() - totalSent, 0);
        if (sent <= 0) {
            return;
        }
        totalSent += static_cast<std::size_t>(sent);
    }
}

bool sendAll(int socketFd, const void *data, std::size_t size) {
    const auto *bytes = static_cast<const std::uint8_t *>(data);
    std::size_t totalSent = 0;
    while (totalSent < size) {
        const auto sent = ::send(socketFd, bytes + totalSent, size - totalSent, 0);
        if (sent <= 0) {
            return false;
        }
        totalSent += static_cast<std::size_t>(sent);
    }
    return true;
}

bool recvAll(int socketFd, void *data, std::size_t size) {
    auto *bytes = static_cast<std::uint8_t *>(data);
    std::size_t totalRead = 0;
    while (totalRead < size) {
        const auto readBytes = ::recv(socketFd, bytes + totalRead, size - totalRead, 0);
        if (readBytes <= 0) {
            return false;
        }
        totalRead += static_cast<std::size_t>(readBytes);
    }
    return true;
}

int runShellCommand(const std::string &command, std::chrono::milliseconds timeout, std::string &output) {
    int pipefd[2] {-1, -1};
    if (::pipe(pipefd) != 0) {
        output = "failed to create shell pipe\n";
        return -1;
    }

    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        output = "failed to fork shell process\n";
        return -1;
    }

    if (pid == 0) {
        ::dup2(pipefd[1], STDOUT_FILENO);
        ::dup2(pipefd[1], STDERR_FILENO);
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        ::execl("/bin/sh", "sh", "-lc", command.c_str(), static_cast<char *>(nullptr));
        _exit(127);
    }

    ::close(pipefd[1]);
    const int flags = ::fcntl(pipefd[0], F_GETFL, 0);
    if (flags >= 0) {
        ::fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);
    }

    std::array<char, 256> buffer {};
    int status = 0;
    bool childExited = false;
    bool timedOut = false;
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (true) {
        const auto bytes = ::read(pipefd[0], buffer.data(), buffer.size());
        if (bytes > 0) {
            output.append(buffer.data(), static_cast<std::size_t>(bytes));
        }

        const pid_t waitResult = ::waitpid(pid, &status, WNOHANG);
        if (waitResult == pid) {
            childExited = true;
        }

        if (childExited && bytes <= 0) {
            break;
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            timedOut = true;
            ::kill(pid, SIGKILL);
            ::waitpid(pid, &status, 0);
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    while (true) {
        const auto bytes = ::read(pipefd[0], buffer.data(), buffer.size());
        if (bytes <= 0) {
            break;
        }
        output.append(buffer.data(), static_cast<std::size_t>(bytes));
    }
    ::close(pipefd[0]);

    if (timedOut) {
        output += "command timed out\n";
        return -2;
    }
    if (output.empty()) {
        output = '\n';
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return -1;
}

}  // namespace

struct Instance::TelnetState {
    struct Command {
        std::string usage;
        TelnetCommandHandler handler;
    };

    mutable std::mutex mutex;
    int listenSocket {kInvalidSocket};
    int clientSocket {kInvalidSocket};
    std::thread acceptThread;
    std::thread clientThread;
    bool running {false};
    std::uint16_t requestedPort {0};
    std::uint16_t actualPort {0};
    std::string prompt {defaultTelnetPrompt()};
    bool shellCommandEnabled {true};
    std::chrono::milliseconds shellCommandTimeout {std::chrono::seconds(5)};
    std::unordered_map<std::string, Command> commands;
};

std::string Message::payloadAsString() const {
    return std::string(payload.begin(), payload.end());
}

Runtime &Runtime::shared() {
    static Runtime runtime;
    return runtime;
}

void Runtime::registerServer(std::uint16_t port, Instance *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (servers_.count(port) > 0) {
        throw makeError("server port already registered");
    }
    servers_[port] = instance;
}

void Runtime::unregisterServer(std::uint16_t port, Instance *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = servers_.find(port);
    if (it != servers_.end() && it->second == instance) {
        servers_.erase(it);
    }
}

Instance *Runtime::findServer(std::uint16_t port) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = servers_.find(port);
    return it == servers_.end() ? nullptr : it->second;
}

Instance::Instance(InstanceOptions options)
    : id_(g_nextInstanceId.fetch_add(1)),
      options_(options),
      nodes_(options.maxNodeCount + 1),
      containers_(options.maxContainerCount + 1),
      telnet_(std::make_unique<TelnetState>()) {
    telnet_->requestedPort = options_.telnetPort;
    workerThread_ = std::thread([this] { workerLoop(); });
    registerTelnetCommand(
        "help",
        "help - list available telnet commands",
        [](Instance &instance, const std::vector<std::string> &, std::string &output) {
            std::vector<std::string> lines;
            {
                std::lock_guard<std::mutex> lock(instance.telnet_->mutex);
                lines.emplace_back("available commands:");
                for (const auto &[name, command] : instance.telnet_->commands) {
                    lines.push_back("  " + name + " - " + command.usage);
                }
            }
            output = joinLines(lines);
            return 0;
        });
    registerTelnetCommand(
        "status",
        "status - show instance, server, telnet, node and container counts",
        [](Instance &instance, const std::vector<std::string> &, std::string &output) {
            std::size_t connectedNodes = 0;
            std::size_t acceptedNodes = 0;
            std::size_t containers = 0;
            std::size_t timers = 0;
            {
                std::lock_guard<std::mutex> lock(instance.mutex_);
                for (std::size_t index = 1; index < instance.nodes_.size(); ++index) {
                    if (instance.nodes_[index].has_value()) {
                        if (instance.nodes_[index]->isConnected) {
                            ++connectedNodes;
                        }
                        if (instance.nodes_[index]->isAcceptedPeer) {
                            ++acceptedNodes;
                        }
                    }
                }
                for (std::size_t index = 1; index < instance.containers_.size(); ++index) {
                    if (instance.containers_[index].has_value()) {
                        ++containers;
                    }
                }
                timers = instance.timers_.size();
            }
            std::ostringstream stream;
            stream << "instance_id=" << instance.id() << '\n'
                   << "app_data=" << instance.applicationData() << '\n'
                   << "server_running=" << (instance.isServerRunning() ? 1 : 0) << '\n'
                   << "server_port=" << instance.options_.localPort << '\n'
                   << "telnet_running=" << (instance.isTelnetServerRunning() ? 1 : 0) << '\n'
                   << "telnet_port=" << instance.telnetPort() << '\n'
                   << "connected_nodes=" << connectedNodes << '\n'
                   << "accepted_nodes=" << acceptedNodes << '\n'
                   << "containers=" << containers << '\n'
                   << "timers=" << timers << '\n';
            output = stream.str();
            return 0;
        });
    registerTelnetCommand(
        "nodes",
        "nodes - list current node records",
        [](Instance &instance, const std::vector<std::string> &, std::string &output) {
            std::ostringstream stream;
            std::lock_guard<std::mutex> lock(instance.mutex_);
            for (std::size_t index = 1; index < instance.nodes_.size(); ++index) {
                if (!instance.nodes_[index].has_value()) {
                    continue;
                }
                const auto &node = *instance.nodes_[index];
                stream << "node id=" << index
                       << " connected=" << (node.isConnected ? 1 : 0)
                       << " accepted=" << (node.isAcceptedPeer ? 1 : 0)
                       << " peer_port=" << node.peerPort
                       << " socket_fd=" << node.socketFd
                       << '\n';
            }
            output = stream.str();
            return 0;
        });
    registerTelnetCommand(
        "containers",
        "containers - list current containers and queue stats",
        [](Instance &instance, const std::vector<std::string> &, std::string &output) {
            std::ostringstream stream;
            std::lock_guard<std::mutex> lock(instance.mutex_);
            for (std::size_t index = 1; index < instance.containers_.size(); ++index) {
                if (!instance.containers_[index].has_value()) {
                    continue;
                }
                const auto &container = *instance.containers_[index];
                const auto &stats = container.stats.fsmQueue;
                stream << "container id=" << index
                       << " name=" << container.options.nameCode
                       << " objects=" << container.options.objectCount
                       << " object_size=" << container.options.objectSize
                       << " queue_current=" << stats.currentMessageCount
                       << " queue_received=" << stats.receivedMessageCount
                       << " queue_processed=" << stats.processedMessageCount
                       << '\n';
            }
            output = stream.str();
            return 0;
        });
    registerTelnetCommand(
        "timers",
        "timers - list active timers",
        [](Instance &instance, const std::vector<std::string> &, std::string &output) {
            std::ostringstream stream;
            std::lock_guard<std::mutex> lock(instance.mutex_);
            for (const auto &[timerId, timer] : instance.timers_) {
                stream << "timer id=" << timerId
                       << " container=" << timer.containerId
                       << " mode=" << (timer.mode == TimerMode::Once ? "once" : "cycle")
                       << " delay_ms=" << timer.delayMs
                       << " context=" << timer.context
                       << '\n';
            }
            output = stream.str();
            return 0;
        });
    registerTelnetCommand(
        "echo",
        "echo <args...> - echo arguments back to the telnet client",
        [](Instance &, const std::vector<std::string> &args, std::string &output) {
            std::ostringstream stream;
            for (std::size_t index = 1; index < args.size(); ++index) {
                if (index > 1) {
                    stream << ' ';
                }
                stream << args[index];
            }
            stream << '\n';
            output = stream.str();
            return 0;
        });
}

Instance::~Instance() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        exiting_ = true;
    }
    queueCondition_.notify_all();
    stateCondition_.notify_all();
    stopTelnetServer();
    stopServer();
    std::vector<std::thread> nodeThreads;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &entry : nodes_) {
            if (!entry.has_value()) {
                continue;
            }
            entry->localCloseRequested = true;
            entry->isConnected = false;
            shutdownNodeSocketLocked(*entry);
            nodeThreads.push_back(std::move(entry->readerThread));
        }
    }
    for (auto &thread : nodeThreads) {
        joinNodeThread(thread);
    }
    for (auto &entry : timers_) {
        if (entry.second.cancelled) {
            *entry.second.cancelled = true;
        }
    }
    if (workerThread_.joinable()) {
        workerThread_.join();
    }
}

InstanceId Instance::id() const noexcept {
    return id_;
}

std::uint32_t Instance::applicationData() const noexcept {
    return options_.applicationData;
}

bool Instance::isServerRunning() const noexcept {
    return serverPort_ != 0;
}

void Instance::startServer(const NodeOptions &) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (serverPort_ != 0) {
        throw makeError("server already started");
    }
    if (telnetPort() != 0 && telnetPort() == options_.localPort) {
        throw makeError("server port conflicts with telnet port");
    }

    const int listenSocket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenSocket == kInvalidSocket) {
        throw makeError("failed to create server socket");
    }

    int reuse = 1;
    ::setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(options_.localPort);
    if (::bind(listenSocket, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listenSocket, 20) != 0) {
        ::close(listenSocket);
        throw makeError("failed to start server listener");
    }

    serverPort_ = options_.localPort;
    serverListenSocket_ = listenSocket;
    acceptThread_ = std::thread([this] {
        while (true) {
            sockaddr_in clientAddress {};
            socklen_t clientLength = sizeof(clientAddress);
            const int acceptedSocket = ::accept(
                serverListenSocket_,
                reinterpret_cast<sockaddr *>(&clientAddress),
                &clientLength);

            if (acceptedSocket == kInvalidSocket) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (exiting_ || serverListenSocket_ == kInvalidSocket) {
                    return;
                }
                continue;
            }

            NodeId acceptedNodeId = 0;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (exiting_ || serverListenSocket_ == kInvalidSocket) {
                    ::close(acceptedSocket);
                    return;
                }
                acceptedNodeId = allocateNodeIdLocked(NodeOptions {}, true);
                auto &node = requireNode(acceptedNodeId);
                node.isConnected = true;
                node.localCloseRequested = false;
                node.peerIpAddress = inet_ntoa(clientAddress.sin_addr);
                node.peerPort = ntohs(clientAddress.sin_port);
                node.socketFd = acceptedSocket;
            }
            startNodeReader(acceptedNodeId);
            broadcastSystemEvent(EventType::NodeConnected, acceptedNodeId);
        }
    });
}

void Instance::stopServer() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (serverPort_ == 0) {
            return;
        }
        if (serverListenSocket_ != kInvalidSocket) {
            ::shutdown(serverListenSocket_, SHUT_RDWR);
            ::close(serverListenSocket_);
            serverListenSocket_ = kInvalidSocket;
        }
        serverPort_ = 0;
    }
    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
}

ContainerId Instance::createContainer(const ContainerOptions &options) {
    if (options.nameCode == 0) {
        throw makeError("container nameCode must not be zero");
    }
    if (!options.onMessage) {
        throw makeError("container callback must not be empty");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (findContainerByName(options.nameCode) != 0) {
        throw makeError("container name already exists");
    }

    for (ContainerId index = 1; index < containers_.size(); ++index) {
        if (!containers_[index].has_value()) {
            ContainerRecord record;
            record.options = options;
            if (options.objectCount > 0 && options.objectSize > 0) {
                record.objectMemory.resize(options.objectCount * options.objectSize, 0);
            }
            containers_[index] = std::move(record);
            return index;
        }
    }

    throw makeError("no free container slot");
}

void Instance::deleteContainer(ContainerId containerId) {
    std::lock_guard<std::mutex> lock(mutex_);
    requireContainer(containerId);
    containers_[containerId].reset();
    queue_.erase(
        std::remove_if(queue_.begin(), queue_.end(), [containerId](const QueuedMessage &queued) {
            return queued.containerId == containerId;
        }),
        queue_.end());
}

void *Instance::getObjectMemory(ContainerId containerId, std::uint32_t objectId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &container = requireContainer(containerId);
    if (objectId == 0 || objectId > container.options.objectCount || container.options.objectSize == 0) {
        throw makeError("invalid object id");
    }
    auto offset = static_cast<std::size_t>((objectId - 1) * container.options.objectSize);
    return container.objectMemory.data() + offset;
}

NodeId Instance::createNode(const NodeOptions &options) {
    std::lock_guard<std::mutex> lock(mutex_);
    return allocateNodeIdLocked(options, false);
}

void Instance::deleteNode(NodeId nodeId) {
    std::thread readerThread;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &node = requireNode(nodeId);
        if (node.isConnected) {
            throw makeError("node must be disconnected before delete");
        }
        readerThread = std::move(node.readerThread);
        nodes_[nodeId].reset();
    }
    joinNodeThread(readerThread);
}

void Instance::connectNode(NodeId nodeId, const std::string &ipAddress, std::uint16_t serverPort) {
    int socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd == kInvalidSocket) {
        throw makeError("failed to create client socket");
    }

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_port = htons(serverPort);
    if (::inet_pton(AF_INET, ipAddress.c_str(), &address.sin_addr) != 1) {
        ::close(socketFd);
        throw makeError("invalid remote server address");
    }

    if (::connect(socketFd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(socketFd);
        throw makeError("remote server not found");
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &clientNode = requireNode(nodeId);
        clientNode.isConnected = true;
        clientNode.localCloseRequested = false;
        clientNode.peerIpAddress = ipAddress;
        clientNode.peerPort = serverPort;
        clientNode.socketFd = socketFd;
    }
    startNodeReader(nodeId);
}

void Instance::disconnectNode(NodeId nodeId) {
    std::thread readerThread;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &node = requireNode(nodeId);
        if (!node.isConnected) {
            throw makeError("node is not connected");
        }
        node.localCloseRequested = true;
        node.isConnected = false;
        shutdownNodeSocketLocked(node);
        readerThread = std::move(node.readerThread);
    }
    joinNodeThread(readerThread);
    stateCondition_.notify_all();
}

void Instance::sendMessage(const Message &message) {
    if (isReservedEventType(message.eventType)) {
        throw makeError("system event types are reserved");
    }

    if (message.destinationNodeId == 0) {
        ContainerId targetContainerId = message.destinationContainerId;
        if (targetContainerId == 0 && message.destinationNameCode != 0) {
            targetContainerId = findContainerByName(message.destinationNameCode);
            if (targetContainerId == 0) {
                throw makeError("local destination container not found");
            }
        }
        enqueueLocalMessage(QueuedMessage {targetContainerId, message});
        return;
    }

    dispatchRemoteMessage(message.destinationNodeId, message);
}

std::optional<RemoteContainerInfo> Instance::queryRemoteContainer(NodeId nodeId, NameCode nameCode) {
    std::uint32_t requestId = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &node = requireNode(nodeId);
        if (!node.isConnected || node.socketFd == kInvalidSocket) {
            throw makeError("name query requires a connected node");
        }
        requestId = node.nextQueryId++;
        node.pendingQueries.insert_or_assign(requestId, PendingNameQuery {});
    }

    sendNameQueryPacket(nodeId, requestId, nameCode);

    std::unique_lock<std::mutex> lock(mutex_);
    stateCondition_.wait(lock, [&] {
        if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
            return true;
        }
        auto &node = *nodes_[nodeId];
        auto iterator = node.pendingQueries.find(requestId);
        return iterator != node.pendingQueries.end() && iterator->second.completed;
    });

    if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
        return std::nullopt;
    }
    auto &node = *nodes_[nodeId];
    auto iterator = node.pendingQueries.find(requestId);
    if (iterator == node.pendingQueries.end()) {
        return std::nullopt;
    }
    const auto result = iterator->second.result;
    node.pendingQueries.erase(iterator);
    return result;
}

TimerId Instance::setTimer(ContainerId containerId, std::uint32_t delayMs, TimerMode mode, std::uint32_t context) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        requireContainer(containerId);
    }

    auto timerId = nextTimerId_.fetch_add(1);
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_.insert_or_assign(timerId, TimerRecord {
            .containerId = containerId,
            .mode = mode,
            .delayMs = delayMs,
            .context = context,
            .cancelled = cancelled,
        });
    }

    std::thread([this, containerId, delayMs, mode, context, timerId, cancelled] {
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            if (*cancelled) {
                return;
            }
            Message message;
            message.destinationContainerId = containerId;
            message.eventType = EventType::Timer;
            message.timerId = timerId;
            message.timerContext = context;
            enqueueLocalMessage(QueuedMessage {containerId, std::move(message)});
        } while (mode == TimerMode::Cycle && !(*cancelled));
    }).detach();

    return timerId;
}

void Instance::killTimer(ContainerId containerId, TimerId timerId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iterator = timers_.find(timerId);
    if (iterator == timers_.end() || iterator->second.containerId != containerId) {
        throw makeError("timer not found");
    }
    *iterator->second.cancelled = true;
    queue_.erase(
        std::remove_if(queue_.begin(), queue_.end(), [containerId, timerId](const QueuedMessage &queued) {
            return queued.containerId == containerId &&
                   queued.message.eventType == EventType::Timer &&
                   queued.message.timerId == timerId;
        }),
        queue_.end());
    timers_.erase(iterator);
}

ContainerStats Instance::getContainerStats(ContainerId containerId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return requireContainer(containerId).stats;
}

NodeId Instance::allocateNodeIdLocked(const NodeOptions &options, bool isAcceptedPeer) {
    for (NodeId index = 1; index < nodes_.size(); ++index) {
        if (!nodes_[index].has_value()) {
            NodeRecord record;
            record.options = options;
            record.isAcceptedPeer = isAcceptedPeer;
            nodes_[index] = std::move(record);
            return index;
        }
    }
    throw makeError("no free node slot");
}

void Instance::startNodeReader(NodeId nodeId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &node = requireNode(nodeId);
    node.readerThread = std::thread([this, nodeId] { readerLoop(nodeId); });
}

void Instance::readerLoop(NodeId nodeId) {
    while (true) {
        int socketFd = kInvalidSocket;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
                return;
            }
            socketFd = nodes_[nodeId]->socketFd;
            if (socketFd == kInvalidSocket) {
                return;
            }
        }

        PacketHeader networkHeader {};
        if (!recvAll(socketFd, &networkHeader, sizeof(networkHeader))) {
            handleSocketClosed(nodeId);
            return;
        }

        PacketHeader header {
            .packetType = fromNetwork(networkHeader.packetType),
            .requestId = fromNetwork(networkHeader.requestId),
            .sourceContainerId = fromNetwork(networkHeader.sourceContainerId),
            .sourceObjectId = fromNetwork(networkHeader.sourceObjectId),
            .destinationContainerId = fromNetwork(networkHeader.destinationContainerId),
            .destinationObjectId = fromNetwork(networkHeader.destinationObjectId),
            .eventType = fromNetwork(networkHeader.eventType),
            .timerId = fromNetwork(networkHeader.timerId),
            .timerContext = fromNetwork(networkHeader.timerContext),
            .nameCode = fromNetwork(networkHeader.nameCode),
            .payloadSize = fromNetwork(networkHeader.payloadSize),
            .found = fromNetwork(networkHeader.found),
        };

        std::vector<std::uint8_t> payload(header.payloadSize);
        if (header.payloadSize > 0 && !recvAll(socketFd, payload.data(), payload.size())) {
            handleSocketClosed(nodeId);
            return;
        }

        try {
            if (header.packetType == static_cast<std::uint32_t>(PacketType::UserMessage)) {
                Message message;
                message.sourceNodeId = 0;
                message.sourceContainerId = header.sourceContainerId;
                message.sourceObjectId = header.sourceObjectId;
                message.destinationNodeId = nodeId;
                message.destinationContainerId = header.destinationContainerId;
                message.destinationObjectId = header.destinationObjectId;
                message.eventType = static_cast<EventType>(header.eventType);
                message.timerId = header.timerId;
                message.timerContext = header.timerContext;
                message.destinationNameCode = header.nameCode;
                message.payload = std::move(payload);

                ContainerId targetContainerId = message.destinationContainerId;
                if (targetContainerId == 0 && message.destinationNameCode != 0) {
                    targetContainerId = findContainerByName(message.destinationNameCode);
                    if (targetContainerId == 0) {
                        continue;
                    }
                    message.destinationContainerId = targetContainerId;
                }
                enqueueLocalMessage(QueuedMessage {targetContainerId, std::move(message)});
                continue;
            }

            if (header.packetType == static_cast<std::uint32_t>(PacketType::NameQuery)) {
                const auto containerId = findContainerByName(header.nameCode);
                sendNameQueryReplyPacket(nodeId, header.requestId, header.nameCode, containerId);
                continue;
            }

            if (header.packetType == static_cast<std::uint32_t>(PacketType::NameQueryReply)) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
                    return;
                }
                auto &node = *nodes_[nodeId];
                auto &pending = node.pendingQueries[header.requestId];
                pending.completed = true;
                if (header.found != 0 && header.destinationContainerId != 0) {
                    pending.result = RemoteContainerInfo {
                        .remoteNodeId = nodeId,
                        .remoteContainerId = header.destinationContainerId,
                        .nameCode = header.nameCode,
                    };
                } else {
                    pending.result = std::nullopt;
                }
                stateCondition_.notify_all();
                continue;
            }
        } catch (const std::exception &) {
            continue;
        }
    }
}

void Instance::handleSocketClosed(NodeId nodeId) {
    bool eraseNode = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
            return;
        }
        auto &node = *nodes_[nodeId];
        const bool localCloseRequested = node.localCloseRequested;
        node.isConnected = false;
        shutdownNodeSocketLocked(node);
        for (auto &[requestId, pending] : node.pendingQueries) {
            pending.completed = true;
            pending.result = std::nullopt;
        }
        stateCondition_.notify_all();

        if (node.readerThread.joinable() && node.readerThread.get_id() == std::this_thread::get_id()) {
            node.readerThread.detach();
        }
        eraseNode = !localCloseRequested;
        node.localCloseRequested = false;
    }

    if (eraseNode) {
        broadcastSystemEvent(EventType::NodeDisconnected, nodeId);
        std::lock_guard<std::mutex> lock(mutex_);
        if (nodeId < nodes_.size() && nodes_[nodeId].has_value() && !nodes_[nodeId]->isConnected) {
            nodes_[nodeId].reset();
        }
    }
}

void Instance::sendUserPacket(NodeId nodeId, const Message &message) {
    int socketFd = kInvalidSocket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &node = requireNode(nodeId);
        if (!node.isConnected || node.socketFd == kInvalidSocket) {
            throw makeError("destination node is not connected");
        }
        socketFd = node.socketFd;
    }

    PacketHeader header {};
    header.packetType = toNetwork(static_cast<std::uint32_t>(PacketType::UserMessage));
    header.sourceContainerId = toNetwork(message.sourceContainerId);
    header.sourceObjectId = toNetwork(message.sourceObjectId);
    header.destinationContainerId = toNetwork(message.destinationContainerId);
    header.destinationObjectId = toNetwork(message.destinationObjectId);
    header.eventType = toNetwork(static_cast<std::uint32_t>(message.eventType));
    header.timerId = toNetwork(message.timerId);
    header.timerContext = toNetwork(message.timerContext);
    header.nameCode = toNetwork(message.destinationNameCode);
    header.payloadSize = toNetwork(static_cast<std::uint32_t>(message.payload.size()));

    if (!sendAll(socketFd, &header, sizeof(header)) ||
        (!message.payload.empty() && !sendAll(socketFd, message.payload.data(), message.payload.size()))) {
        throw makeError("failed to send remote message");
    }
}

void Instance::sendNameQueryPacket(NodeId nodeId, std::uint32_t requestId, NameCode nameCode) {
    int socketFd = kInvalidSocket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &node = requireNode(nodeId);
        if (!node.isConnected || node.socketFd == kInvalidSocket) {
            throw makeError("name query requires a connected node");
        }
        socketFd = node.socketFd;
    }

    PacketHeader header {};
    header.packetType = toNetwork(static_cast<std::uint32_t>(PacketType::NameQuery));
    header.requestId = toNetwork(requestId);
    header.nameCode = toNetwork(nameCode);
    if (!sendAll(socketFd, &header, sizeof(header))) {
        throw makeError("failed to send name query");
    }
}

void Instance::sendNameQueryReplyPacket(NodeId nodeId, std::uint32_t requestId, NameCode nameCode, ContainerId containerId) {
    int socketFd = kInvalidSocket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
            return;
        }
        auto &node = *nodes_[nodeId];
        if (!node.isConnected || node.socketFd == kInvalidSocket) {
            return;
        }
        socketFd = node.socketFd;
    }

    PacketHeader header {};
    header.packetType = toNetwork(static_cast<std::uint32_t>(PacketType::NameQueryReply));
    header.requestId = toNetwork(requestId);
    header.destinationContainerId = toNetwork(containerId);
    header.nameCode = toNetwork(nameCode);
    header.found = toNetwork(containerId != 0 ? 1u : 0u);
    sendAll(socketFd, &header, sizeof(header));
}

void Instance::shutdownNodeSocketLocked(NodeRecord &node) {
    if (node.socketFd != kInvalidSocket) {
        ::shutdown(node.socketFd, SHUT_RDWR);
        ::close(node.socketFd);
        node.socketFd = kInvalidSocket;
    }
}

void Instance::joinNodeThread(std::thread &thread) {
    if (thread.joinable()) {
        if (thread.get_id() == std::this_thread::get_id()) {
            thread.detach();
        } else {
            thread.join();
        }
    }
}

void Instance::startTelnetServer() {
    std::lock_guard<std::mutex> telnetLock(telnet_->mutex);
    if (telnet_->running) {
        throw makeError("telnet server already started");
    }
    if (serverPort_ != 0 && options_.localPort == telnet_->requestedPort) {
        throw makeError("telnet port conflicts with server port");
    }

    const auto bindPort = [this](int sock) -> std::uint16_t {
        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);

        const auto tryBind = [&](std::uint16_t port) -> bool {
            address.sin_port = htons(port);
            return ::bind(sock, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0;
        };

        if (telnet_->requestedPort != 0 && tryBind(telnet_->requestedPort)) {
            return telnet_->requestedPort;
        }
        if (telnet_->requestedPort == 0 && tryBind(0)) {
            sockaddr_in boundAddress {};
            socklen_t boundLength = sizeof(boundAddress);
            if (::getsockname(sock, reinterpret_cast<sockaddr *>(&boundAddress), &boundLength) == 0) {
                return ntohs(boundAddress.sin_port);
            }
        }
        for (std::uint16_t port = kMinTelnetPort; port <= kMaxTelnetPort; ++port) {
            if (tryBind(port)) {
                return port;
            }
        }
        return 0;
    };

    const auto listenSocket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenSocket == kInvalidSocket) {
        throw makeError("failed to create telnet socket");
    }

    int reuse = 1;
    ::setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    const auto actualPort = bindPort(listenSocket);
    if (actualPort == 0 || ::listen(listenSocket, 20) != 0) {
        ::close(listenSocket);
        throw makeError("failed to start telnet listener");
    }

    telnet_->listenSocket = listenSocket;
    telnet_->actualPort = actualPort;
    telnet_->running = true;

    telnet_->acceptThread = std::thread([this] {
        while (true) {
            sockaddr_in clientAddress {};
            socklen_t clientLength = sizeof(clientAddress);
            const int accepted = ::accept(
                telnet_->listenSocket,
                reinterpret_cast<sockaddr *>(&clientAddress),
                &clientLength);

            std::lock_guard<std::mutex> lock(telnet_->mutex);
            if (!telnet_->running) {
                if (accepted != kInvalidSocket) {
                    ::close(accepted);
                }
                return;
            }
            if (accepted == kInvalidSocket) {
                continue;
            }
            if (telnet_->clientSocket != kInvalidSocket) {
                ::close(telnet_->clientSocket);
            }
            telnet_->clientSocket = accepted;
            std::string promptLine;
            {
                promptLine = telnet_->prompt;
            }
            const std::array<std::string, 4> welcome {
                "***************************************\r\n",
                "welcome to fsmp telnet server\r\n",
                "***************************************\r\n",
                "",
            };
            for (const auto &line : welcome) {
                sendSocketText(telnet_->clientSocket, line);
            }
            sendSocketText(telnet_->clientSocket, promptLine);
        }
    });

    telnet_->clientThread = std::thread([this] {
        std::string buffer;
        while (true) {
            int clientSocket = kInvalidSocket;
            {
                std::lock_guard<std::mutex> lock(telnet_->mutex);
                if (!telnet_->running) {
                    return;
                }
                clientSocket = telnet_->clientSocket;
            }
            if (clientSocket == kInvalidSocket) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }

            fd_set readSet;
            FD_ZERO(&readSet);
            FD_SET(clientSocket, &readSet);
            timeval timeout {};
            timeout.tv_sec = 0;
            timeout.tv_usec = 200000;
            const int ready = ::select(clientSocket + 1, &readSet, nullptr, nullptr, &timeout);
            if (ready <= 0) {
                continue;
            }

            char ch = '\0';
            const auto bytes = ::recv(clientSocket, &ch, 1, 0);
            if (bytes <= 0) {
                std::lock_guard<std::mutex> lock(telnet_->mutex);
                if (telnet_->clientSocket != kInvalidSocket) {
                    ::close(telnet_->clientSocket);
                    telnet_->clientSocket = kInvalidSocket;
                }
                buffer.clear();
                continue;
            }

            if (ch == '\r') {
                continue;
            }
            if (ch == '\n') {
                const auto command = trim(buffer);
                buffer.clear();
                if (command.empty()) {
                    sendSocketText(clientSocket, telnetPrompt());
                    continue;
                }
                if (command == "bye") {
                    log(LogLevel::Info, "bye......");
                    std::lock_guard<std::mutex> lock(telnet_->mutex);
                    if (telnet_->clientSocket != kInvalidSocket) {
                        ::close(telnet_->clientSocket);
                        telnet_->clientSocket = kInvalidSocket;
                    }
                    continue;
                }
                const auto args = parseCommandLine(command);
                std::string output;
                int result = -1;
                TelnetCommandHandler handler;
                {
                    std::lock_guard<std::mutex> lock(telnet_->mutex);
                    auto iterator = telnet_->commands.find(args.front());
                    if (iterator != telnet_->commands.end()) {
                        handler = iterator->second.handler;
                    }
                }
                if (handler) {
                    result = handler(*this, args, output);
                } else if (!command.empty() && command.front() == '!') {
                    const auto shellCommand = trim(command.substr(1));
                    bool shellEnabled = false;
                    std::chrono::milliseconds shellTimeout {0};
                    {
                        std::lock_guard<std::mutex> lock(telnet_->mutex);
                        shellEnabled = telnet_->shellCommandEnabled;
                        shellTimeout = telnet_->shellCommandTimeout;
                    }
                    if (!shellEnabled) {
                        output = "shell command mode is disabled\n";
                        result = -1;
                    } else if (shellCommand.empty()) {
                        output = "empty shell command\n";
                        result = -1;
                    } else {
                        result = runShellCommand(shellCommand, shellTimeout, output);
                    }
                } else {
                    output = "function '" + command + "' doesn't exist!\n";
                }
                if (!output.empty()) {
                    log(result == 0 ? LogLevel::Info : LogLevel::Error, output);
                }
                sendSocketText(clientSocket, telnetPrompt());
                continue;
            }
            if (ch == 0x08 || ch == 0x7f) {
                if (!buffer.empty()) {
                    buffer.pop_back();
                }
                continue;
            }
            buffer.push_back(ch);
        }
    });
}

void Instance::stopTelnetServer() {
    {
        std::lock_guard<std::mutex> lock(telnet_->mutex);
        if (!telnet_->running) {
            return;
        }
        telnet_->running = false;
        if (telnet_->clientSocket != kInvalidSocket) {
            ::shutdown(telnet_->clientSocket, SHUT_RDWR);
            ::close(telnet_->clientSocket);
            telnet_->clientSocket = kInvalidSocket;
        }
        if (telnet_->listenSocket != kInvalidSocket) {
            ::shutdown(telnet_->listenSocket, SHUT_RDWR);
            ::close(telnet_->listenSocket);
            telnet_->listenSocket = kInvalidSocket;
        }
    }
    if (telnet_->acceptThread.joinable()) {
        telnet_->acceptThread.join();
    }
    if (telnet_->clientThread.joinable()) {
        telnet_->clientThread.join();
    }
    telnet_->actualPort = 0;
}

bool Instance::isTelnetServerRunning() const noexcept {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    return telnet_->running;
}

std::uint16_t Instance::telnetPort() const noexcept {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    return telnet_->actualPort;
}

void Instance::registerTelnetCommand(std::string name, std::string usage, TelnetCommandHandler handler) {
    if (name.empty() || !handler) {
        throw makeError("invalid telnet command");
    }
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    telnet_->commands.insert_or_assign(std::move(name), TelnetState::Command {
                                                        .usage = std::move(usage),
                                                        .handler = std::move(handler),
                                                    });
}

void Instance::setTelnetPrompt(std::string prompt) {
    if (prompt.empty()) {
        throw makeError("telnet prompt must not be empty");
    }
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    telnet_->prompt = std::move(prompt);
}

std::string Instance::telnetPrompt() const {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    return telnet_->prompt;
}

void Instance::setTelnetShellCommandEnabled(bool enabled) {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    telnet_->shellCommandEnabled = enabled;
}

bool Instance::isTelnetShellCommandEnabled() const noexcept {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    return telnet_->shellCommandEnabled;
}

void Instance::setTelnetShellCommandTimeout(std::chrono::milliseconds timeout) {
    if (timeout <= std::chrono::milliseconds::zero()) {
        throw makeError("telnet shell command timeout must be positive");
    }
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    telnet_->shellCommandTimeout = timeout;
}

std::chrono::milliseconds Instance::telnetShellCommandTimeout() const noexcept {
    std::lock_guard<std::mutex> lock(telnet_->mutex);
    return telnet_->shellCommandTimeout;
}

void Instance::log(LogLevel, std::string_view message) {
    std::string normalized;
    normalized.reserve(message.size() + 2);
    for (char ch : message) {
        if (ch == '\n') {
            normalized += "\r\n";
        } else {
            normalized.push_back(ch);
        }
    }
    if (!normalized.empty() && normalized.back() != '\n') {
        normalized += "\r\n";
    }

    int clientSocket = kInvalidSocket;
    {
        std::lock_guard<std::mutex> lock(telnet_->mutex);
        clientSocket = telnet_->clientSocket;
    }
    if (clientSocket != kInvalidSocket) {
        ::send(clientSocket, normalized.data(), normalized.size(), 0);
        return;
    }
    std::cout << normalized;
}

void Instance::workerLoop() {
    while (true) {
        QueuedMessage queued;
        ContainerRecord container;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queueCondition_.wait(lock, [this] { return exiting_ || !queue_.empty(); });
            if (exiting_ && queue_.empty()) {
                return;
            }
            queued = std::move(queue_.front());
            queue_.pop_front();
            container = requireContainer(queued.containerId);
        }

        container.options.onMessage(queued.message);

        std::lock_guard<std::mutex> lock(mutex_);
        if (queued.containerId < containers_.size() && containers_[queued.containerId].has_value()) {
            auto &stats = containers_[queued.containerId]->stats.fsmQueue;
            if (stats.currentMessageCount > 0) {
                --stats.currentMessageCount;
            }
            ++stats.processedMessageCount;
        }
    }
}

void Instance::enqueueLocalMessage(QueuedMessage message) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &container = requireContainer(message.containerId);
    auto &stats = container.stats.fsmQueue;
    ++stats.receivedMessageCount;
    ++stats.currentMessageCount;
    stats.peakMessageCount = std::max(stats.peakMessageCount, stats.currentMessageCount);
    stats.peakMessageSize = std::max(stats.peakMessageSize, static_cast<std::uint32_t>(message.message.payload.size()));
    queue_.push_back(std::move(message));
    queueCondition_.notify_one();
}

void Instance::dispatchRemoteMessage(NodeId destinationNodeId, const Message &message) {
    sendUserPacket(destinationNodeId, message);
}

void Instance::broadcastSystemEvent(EventType eventType, NodeId nodeId) {
    Message message;
    message.eventType = eventType;
    message.payload.resize(sizeof(NodeId), 0);
    std::memcpy(message.payload.data(), &nodeId, sizeof(NodeId));

    std::lock_guard<std::mutex> lock(mutex_);
    for (ContainerId containerId = 1; containerId < containers_.size(); ++containerId) {
        if (containers_[containerId].has_value()) {
            auto &stats = containers_[containerId]->stats.fsmQueue;
            ++stats.receivedMessageCount;
            ++stats.currentMessageCount;
            stats.peakMessageCount = std::max(stats.peakMessageCount, stats.currentMessageCount);
            stats.peakMessageSize = std::max(stats.peakMessageSize, static_cast<std::uint32_t>(message.payload.size()));
            queue_.push_back(QueuedMessage {containerId, message});
        }
    }
    queueCondition_.notify_one();
}

ContainerId Instance::findContainerByName(NameCode nameCode) const {
    for (ContainerId containerId = 1; containerId < containers_.size(); ++containerId) {
        if (containers_[containerId].has_value() && containers_[containerId]->options.nameCode == nameCode) {
            return containerId;
        }
    }
    return 0;
}

Instance::NodeRecord &Instance::requireNode(NodeId nodeId) {
    if (nodeId == 0 || nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
        throw makeError("invalid node id");
    }
    return *nodes_[nodeId];
}

const Instance::NodeRecord &Instance::requireNode(NodeId nodeId) const {
    if (nodeId == 0 || nodeId >= nodes_.size() || !nodes_[nodeId].has_value()) {
        throw makeError("invalid node id");
    }
    return *nodes_[nodeId];
}

Instance::ContainerRecord &Instance::requireContainer(ContainerId containerId) {
    if (containerId == 0 || containerId >= containers_.size() || !containers_[containerId].has_value()) {
        throw makeError("invalid container id");
    }
    return *containers_[containerId];
}

const Instance::ContainerRecord &Instance::requireContainer(ContainerId containerId) const {
    if (containerId == 0 || containerId >= containers_.size() || !containers_[containerId].has_value()) {
        throw makeError("invalid container id");
    }
    return *containers_[containerId];
}

NameCode makeNameCode(char a, char b, char c, char d) noexcept {
    return (static_cast<NameCode>(static_cast<std::uint8_t>(a)) << 24) |
           (static_cast<NameCode>(static_cast<std::uint8_t>(b)) << 16) |
           (static_cast<NameCode>(static_cast<std::uint8_t>(c)) << 8) |
           static_cast<NameCode>(static_cast<std::uint8_t>(d));
}

}  // namespace fsmp
