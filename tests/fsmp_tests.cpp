#include "fsmp/runtime.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace fsmp;

namespace {

struct TestFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

void expect(bool condition, const std::string &message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename Fn>
void expectThrows(Fn &&fn, const std::string &messagePart) {
    try {
        fn();
    } catch (const std::exception &error) {
        if (std::string(error.what()).find(messagePart) != std::string::npos) {
            return;
        }
        throw TestFailure("unexpected exception: " + std::string(error.what()));
    }
    throw TestFailure("expected exception containing: " + messagePart);
}

void waitFor(std::function<bool()> predicate, std::chrono::milliseconds timeout, const std::string &message) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw TestFailure(message);
}

struct Lamp {
    std::uint32_t lampId {0};
    bool isOn {false};
};

std::string recvUntil(int socketFd, const std::string &needle, std::chrono::milliseconds timeout) {
    std::string output;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(socketFd, &readSet);
        timeval tv {};
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        const auto ready = ::select(socketFd + 1, &readSet, nullptr, nullptr, &tv);
        if (ready > 0) {
            char buffer[256];
            const auto bytes = ::recv(socketFd, buffer, sizeof(buffer), 0);
            if (bytes <= 0) {
                break;
            }
            output.append(buffer, static_cast<std::size_t>(bytes));
            if (output.find(needle) != std::string::npos) {
                return output;
            }
        }
    }
    throw TestFailure("timed out waiting for telnet data: " + needle);
}

int connectToLocalPort(std::uint16_t port) {
    const auto socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0) {
        throw TestFailure("failed to create telnet client socket");
    }

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(socketFd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(socketFd);
        throw TestFailure("failed to connect to local telnet port");
    }
    return socketFd;
}

void testInstanceLifecycle() {
    Instance first({.localPort = 12000, .maxNodeCount = 8, .maxContainerCount = 8});
    Instance second({.localPort = 12001, .maxNodeCount = 8, .maxContainerCount = 8});
    expect(first.id() != second.id(), "instance id should be unique");
    expect(first.applicationData() == 0, "default application data should be zero");
}

void testRepeatedInstanceConstruction() {
    for (std::uint32_t index = 0; index < 20; ++index) {
        Instance instance({
            .localPort = static_cast<std::uint16_t>(12100 + index),
            .maxNodeCount = 4,
            .maxClientNodeCount = 4,
            .maxContainerCount = 4,
            .applicationData = index,
        });
        expect(instance.applicationData() == index, "application data should round-trip");
    }
}

void testServerLifecycleAndDoubleStart() {
    Instance server({.localPort = 12200, .maxNodeCount = 4, .maxContainerCount = 4});
    server.startServer(NodeOptions {});
    expectThrows([&] { server.startServer(NodeOptions {}); }, "server already started");
    server.stopServer();
    server.startServer(NodeOptions {});
    server.stopServer();
}

void testContainerLifecycleAndObjectAccess() {
    Instance instance({.localPort = 12300, .maxNodeCount = 8, .maxContainerCount = 8});

    const auto containerId = instance.createContainer({
        .nameCode = makeNameCode('l', 'a', 'm', 'p'),
        .objectCount = 4,
        .objectSize = sizeof(Lamp),
        .onMessage = [](const Message &) {},
    });

    auto *lamp = instance.getObject<Lamp>(containerId, 1);
    lamp->lampId = 7;
    lamp->isOn = true;

    auto *sameLamp = instance.getObject<Lamp>(containerId, 1);
    expect(sameLamp->lampId == 7 && sameLamp->isOn, "object access should be stable");

    instance.deleteContainer(containerId);
    expectThrows([&] { instance.getObject<Lamp>(containerId, 1); }, "invalid container id");
}

void testContainerValidationAndCapacity() {
    Instance instance({.localPort = 12400, .maxNodeCount = 4, .maxContainerCount = 2});

    expectThrows(
        [&] {
            instance.createContainer({
                .nameCode = 0,
                .objectCount = 1,
                .objectSize = 1,
                .onMessage = [](const Message &) {},
            });
        },
        "nameCode must not be zero");

    expectThrows(
        [&] {
            instance.createContainer({
                .nameCode = makeNameCode('n', 'u', 'l', 'l'),
                .objectCount = 1,
                .objectSize = 1,
            });
        },
        "callback must not be empty");

    const auto first = instance.createContainer({
        .nameCode = makeNameCode('c', '0', '0', '1'),
        .objectCount = 2,
        .objectSize = sizeof(Lamp),
        .onMessage = [](const Message &) {},
    });

    expectThrows(
        [&] {
            instance.createContainer({
                .nameCode = makeNameCode('c', '0', '0', '1'),
                .objectCount = 2,
                .objectSize = sizeof(Lamp),
                .onMessage = [](const Message &) {},
            });
        },
        "name already exists");

    const auto second = instance.createContainer({
        .nameCode = makeNameCode('c', '0', '0', '2'),
        .objectCount = 0,
        .objectSize = sizeof(Lamp),
        .messageQueueCapacityHint = 0,
        .onMessage = [](const Message &) {},
    });

    expect(first == 1 && second == 2, "container ids should be sequential");
    expectThrows(
        [&] {
            instance.createContainer({
                .nameCode = makeNameCode('c', '0', '0', '3'),
                .objectCount = 1,
                .objectSize = sizeof(Lamp),
                .onMessage = [](const Message &) {},
            });
        },
        "no free container slot");
    expectThrows([&] { instance.deleteContainer(99); }, "invalid container id");
}

void testObjectAccessValidation() {
    Instance instance({.localPort = 12500, .maxNodeCount = 4, .maxContainerCount = 4});

    const auto normalContainer = instance.createContainer({
        .nameCode = makeNameCode('o', 'b', 'j', '1'),
        .objectCount = 2,
        .objectSize = sizeof(Lamp),
        .onMessage = [](const Message &) {},
    });

    const auto zeroSizedContainer = instance.createContainer({
        .nameCode = makeNameCode('o', 'b', 'j', '0'),
        .objectCount = 4,
        .objectSize = 0,
        .onMessage = [](const Message &) {},
    });

    expectThrows([&] { instance.getObject<Lamp>(99, 1); }, "invalid container id");
    expectThrows([&] { instance.getObject<Lamp>(normalContainer, 0); }, "invalid object id");
    expectThrows([&] { instance.getObject<Lamp>(normalContainer, 3); }, "invalid object id");
    expectThrows([&] { instance.getObject<Lamp>(zeroSizedContainer, 1); }, "invalid object id");
}

void testNodeLifecycleAndValidation() {
    Instance server({.localPort = 12600, .maxNodeCount = 4, .maxContainerCount = 4});
    Instance client({.localPort = 12601, .maxNodeCount = 2, .maxContainerCount = 4});
    server.startServer(NodeOptions {});

    const auto firstNode = client.createNode(NodeOptions {});
    const auto secondNode = client.createNode(NodeOptions {});
    expectThrows([&] { client.createNode(NodeOptions {}); }, "no free node slot");

    expectThrows([&] { client.connectNode(99, "127.0.0.1", 12600); }, "invalid node id");
    expectThrows([&] { client.connectNode(firstNode, "127.0.0.1", 12999); }, "remote server not found");
    expectThrows([&] { client.disconnectNode(firstNode); }, "node is not connected");
    expectThrows([&] { client.deleteNode(99); }, "invalid node id");

    client.connectNode(firstNode, "127.0.0.1", 12600);
    expectThrows([&] { client.deleteNode(firstNode); }, "must be disconnected before delete");

    client.disconnectNode(firstNode);
    client.deleteNode(firstNode);
    client.deleteNode(secondNode);
}

void testNodeSystemEvents() {
    Instance server({.localPort = 12700, .maxNodeCount = 4, .maxContainerCount = 4});
    Instance client({.localPort = 12701, .maxNodeCount = 4, .maxContainerCount = 4});
    std::atomic<int> connectedEvents {0};
    std::atomic<int> disconnectedEvents {0};

    server.createContainer({
        .nameCode = makeNameCode('s', 'y', 's', '1'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == EventType::NodeConnected) {
                ++connectedEvents;
            } else if (message.eventType == EventType::NodeDisconnected) {
                ++disconnectedEvents;
            }
        },
    });

    server.startServer(NodeOptions {});
    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, "127.0.0.1", 12700);
    waitFor([&] { return connectedEvents.load() == 1; }, std::chrono::milliseconds(500), "connect event should be broadcast");

    client.disconnectNode(nodeId);
    waitFor([&] { return disconnectedEvents.load() == 1; }, std::chrono::milliseconds(500), "disconnect event should be broadcast");
    client.deleteNode(nodeId);
}

void testLocalMessageDelivery() {
    Instance instance({.localPort = 12800, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> localReceived {0};

    const auto containerId = instance.createContainer({
        .nameCode = makeNameCode('l', 'o', 'c', 'l'),
        .objectCount = 2,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2001)) {
                ++localReceived;
                expect(message.payloadAsString() == "openlamp", "local message payload should match");
            }
        },
    });

    Message message;
    message.destinationContainerId = containerId;
    message.destinationObjectId = 1;
    message.eventType = static_cast<EventType>(2001);
    message.payload.assign({'o', 'p', 'e', 'n', 'l', 'a', 'm', 'p'});
    instance.sendMessage(message);

    waitFor([&] { return localReceived.load() == 1; }, std::chrono::milliseconds(500), "local message should be delivered");
}

void testLocalMessageByNameAndMissingName() {
    Instance instance({.localPort = 12900, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> byNameReceived {0};

    instance.createContainer({
        .nameCode = makeNameCode('n', 'a', 'm', 'e'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2006)) {
                ++byNameReceived;
            }
        },
    });

    Message message;
    message.destinationNameCode = makeNameCode('n', 'a', 'm', 'e');
    message.eventType = static_cast<EventType>(2006);
    instance.sendMessage(message);

    waitFor([&] { return byNameReceived.load() == 1; }, std::chrono::milliseconds(500), "local name send should resolve");

    Message missing;
    missing.destinationNameCode = makeNameCode('m', 'i', 's', 's');
    missing.eventType = static_cast<EventType>(2007);
    expectThrows([&] { instance.sendMessage(missing); }, "local destination container not found");
}

void testRemoteRoundTrip() {
    Instance server({.localPort = 13000, .maxNodeCount = 8, .maxContainerCount = 8});
    Instance client({.localPort = 13001, .maxNodeCount = 8, .maxContainerCount = 8});
    server.startServer(NodeOptions {});

    std::atomic<int> serverReceived {0};
    std::atomic<int> clientReceived {0};

    const auto serverContainerId = server.createContainer({
        .nameCode = makeNameCode('s', 'r', 'v', 'r'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2002)) {
                ++serverReceived;
                Message reply = message;
                reply.destinationNodeId = message.destinationNodeId;
                reply.destinationContainerId = message.sourceContainerId;
                reply.eventType = static_cast<EventType>(2003);
                server.sendMessage(reply);
            }
        },
    });

    ContainerId clientContainerId = 0;
    clientContainerId = client.createContainer({
        .nameCode = makeNameCode('c', 'l', 'n', 't'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2003)) {
                ++clientReceived;
                expect(message.destinationContainerId == clientContainerId, "reply should target original client container");
            }
        },
    });

    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, "127.0.0.1", 13000);

    Message request;
    request.sourceContainerId = clientContainerId;
    request.destinationNodeId = nodeId;
    request.destinationContainerId = serverContainerId;
    request.destinationObjectId = 1;
    request.eventType = static_cast<EventType>(2002);
    request.payload.assign({'p', 'i', 'n', 'g'});
    client.sendMessage(request);

    waitFor([&] { return serverReceived.load() == 1; }, std::chrono::milliseconds(500), "server should receive remote message");
    waitFor([&] { return clientReceived.load() == 1; }, std::chrono::milliseconds(500), "client should receive reply");

    client.disconnectNode(nodeId);
    client.deleteNode(nodeId);
}

void testRemoteMessageByNameAndMissingName() {
    Instance server({.localPort = 13100, .maxNodeCount = 8, .maxContainerCount = 8});
    Instance client({.localPort = 13101, .maxNodeCount = 8, .maxContainerCount = 8});
    server.startServer(NodeOptions {});

    std::atomic<int> received {0};

    server.createContainer({
        .nameCode = makeNameCode('r', 'n', 'a', 'm'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2008)) {
                ++received;
            }
        },
    });

    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, "127.0.0.1", 13100);

    Message byName;
    byName.destinationNodeId = nodeId;
    byName.destinationNameCode = makeNameCode('r', 'n', 'a', 'm');
    byName.eventType = static_cast<EventType>(2008);
    client.sendMessage(byName);

    waitFor([&] { return received.load() == 1; }, std::chrono::milliseconds(500), "remote name send should resolve");

    Message missing;
    missing.destinationNodeId = nodeId;
    missing.destinationNameCode = makeNameCode('n', 'o', 'n', 'e');
    missing.eventType = static_cast<EventType>(2009);
    client.sendMessage(missing);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(received.load() == 1, "missing remote name should not deliver");

    client.disconnectNode(nodeId);
    client.deleteNode(nodeId);
}

void testRemoteNameQuery() {
    Instance server({.localPort = 13200, .maxNodeCount = 8, .maxContainerCount = 8});
    Instance client({.localPort = 13201, .maxNodeCount = 8, .maxContainerCount = 8});
    server.startServer(NodeOptions {});

    server.createContainer({
        .nameCode = makeNameCode('a', 'g', 'n', 't'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [](const Message &) {},
    });

    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, "127.0.0.1", 13200);

    const auto remoteInfo = client.queryRemoteContainer(nodeId, makeNameCode('a', 'g', 'n', 't'));
    expect(remoteInfo.has_value(), "remote name query should find existing container");
    expect(remoteInfo->remoteContainerId == 1, "first remote container should have id 1");

    const auto missingInfo = client.queryRemoteContainer(nodeId, makeNameCode('m', 'i', 's', 's'));
    expect(!missingInfo.has_value(), "remote name query should not find missing container");

    client.disconnectNode(nodeId);
    client.deleteNode(nodeId);
}

void testMessageValidationPaths() {
    Instance local({.localPort = 13300, .maxNodeCount = 4, .maxContainerCount = 4});
    const auto containerId = local.createContainer({
        .nameCode = makeNameCode('v', 'a', 'l', '1'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [](const Message &) {},
    });

    Message reserved;
    reserved.destinationContainerId = containerId;
    reserved.eventType = EventType::NodeDisconnected;
    expectThrows([&] { local.sendMessage(reserved); }, "reserved");

    Message badLocal;
    badLocal.destinationContainerId = 99;
    badLocal.eventType = static_cast<EventType>(2010);
    expectThrows([&] { local.sendMessage(badLocal); }, "invalid container id");

    Instance server({.localPort = 13310, .maxNodeCount = 4, .maxContainerCount = 4});
    Instance client({.localPort = 13311, .maxNodeCount = 4, .maxContainerCount = 4});
    server.startServer(NodeOptions {});
    server.createContainer({
        .nameCode = makeNameCode('r', 'm', 't', '1'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [](const Message &) {},
    });

    const auto nodeId = client.createNode(NodeOptions {});
    expectThrows([&] { client.queryRemoteContainer(nodeId, makeNameCode('r', 'm', 't', '1')); }, "requires a connected node");
    client.connectNode(nodeId, "127.0.0.1", 13310);

    Message badRemoteContainer;
    badRemoteContainer.destinationNodeId = nodeId;
    badRemoteContainer.destinationContainerId = 99;
    badRemoteContainer.eventType = static_cast<EventType>(2011);
    client.sendMessage(badRemoteContainer);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    client.disconnectNode(nodeId);
    expectThrows([&] { client.sendMessage(badRemoteContainer); }, "not connected");
    client.deleteNode(nodeId);
}

void testTimerOnceAndCycle() {
    Instance instance({.localPort = 13400, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> onceCount {0};
    std::atomic<int> cycleCount {0};

    const auto containerId = instance.createContainer({
        .nameCode = makeNameCode('t', 'i', 'm', 'r'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == EventType::Timer) {
                if (message.timerContext == 1) {
                    ++onceCount;
                } else if (message.timerContext == 2) {
                    ++cycleCount;
                }
            }
        },
    });

    instance.setTimer(containerId, 20, TimerMode::Once, 1);
    const auto cycleTimerId = instance.setTimer(containerId, 10, TimerMode::Cycle, 2);

    waitFor([&] { return onceCount.load() == 1; }, std::chrono::milliseconds(500), "once timer should fire exactly once");
    waitFor([&] { return cycleCount.load() >= 3; }, std::chrono::milliseconds(500), "cycle timer should fire repeatedly");

    instance.killTimer(containerId, cycleTimerId);
    const auto cycleSnapshot = cycleCount.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(cycleCount.load() == cycleSnapshot, "killed cycle timer should stop firing");
}

void testTimerValidationAndMixedSchedules() {
    Instance instance({.localPort = 13500, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> onceCount {0};
    std::atomic<int> cycleCount {0};

    const auto containerId = instance.createContainer({
        .nameCode = makeNameCode('t', 'm', 'i', 'x'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType != EventType::Timer) {
                return;
            }
            if (message.timerContext >= 10 && message.timerContext < 20) {
                ++onceCount;
            } else if (message.timerContext >= 20) {
                ++cycleCount;
            }
        },
    });

    std::vector<TimerId> onceTimers;
    for (std::uint32_t index = 0; index < 3; ++index) {
        onceTimers.push_back(instance.setTimer(containerId, 10 + index * 5, TimerMode::Once, 10 + index));
    }
    const auto cycleA = instance.setTimer(containerId, 10, TimerMode::Cycle, 20);
    const auto cycleB = instance.setTimer(containerId, 15, TimerMode::Cycle, 21);

    waitFor([&] { return onceCount.load() == 3; }, std::chrono::milliseconds(500), "all one-shot timers should fire");
    waitFor([&] { return cycleCount.load() >= 4; }, std::chrono::milliseconds(500), "mixed cycle timers should fire repeatedly");

    instance.killTimer(containerId, cycleA);
    instance.killTimer(containerId, cycleB);
    expectThrows([&] { instance.killTimer(containerId, cycleA); }, "timer not found");
    expectThrows([&] { instance.killTimer(99, cycleB); }, "timer not found");
}

void testCallbackSelfSendAndContainerMutation() {
    Instance instance({.localPort = 13600, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> pingCount {0};
    std::atomic<int> pongCount {0};
    std::atomic<int> mutationCount {0};
    std::atomic<ContainerId> dynamicContainerId {0};
    ContainerId primaryContainerId = 0;

    primaryContainerId = instance.createContainer({
        .nameCode = makeNameCode('c', 'b', '0', '1'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == static_cast<EventType>(2100)) {
                ++pingCount;

                if (dynamicContainerId.load() == 0) {
                    const auto createdId = instance.createContainer({
                        .nameCode = makeNameCode('c', 'b', '0', '2'),
                        .objectCount = 1,
                        .objectSize = 4,
                        .onMessage = [&](const Message &innerMessage) {
                            if (innerMessage.eventType == static_cast<EventType>(2102)) {
                                ++mutationCount;

                                Message cleanup;
                                cleanup.destinationContainerId = primaryContainerId;
                                cleanup.eventType = static_cast<EventType>(2103);
                                instance.sendMessage(cleanup);
                            }
                        },
                    });
                    dynamicContainerId = createdId;
                }

                Message toDynamic;
                toDynamic.destinationContainerId = dynamicContainerId.load();
                toDynamic.eventType = static_cast<EventType>(2102);
                instance.sendMessage(toDynamic);

                Message selfReply;
                selfReply.destinationContainerId = primaryContainerId;
                selfReply.eventType = static_cast<EventType>(2101);
                instance.sendMessage(selfReply);
            } else if (message.eventType == static_cast<EventType>(2101)) {
                ++pongCount;
            } else if (message.eventType == static_cast<EventType>(2103)) {
                if (dynamicContainerId.load() != 0) {
                    instance.deleteContainer(dynamicContainerId.load());
                    dynamicContainerId = 0;
                }
            }
        },
    });

    Message kick;
    kick.destinationContainerId = primaryContainerId;
    kick.eventType = static_cast<EventType>(2100);
    instance.sendMessage(kick);

    waitFor([&] { return pingCount.load() == 1; }, std::chrono::milliseconds(500), "callback should receive initial message");
    waitFor([&] { return pongCount.load() == 1; }, std::chrono::milliseconds(500), "callback should be able to send to self");
    waitFor([&] { return mutationCount.load() == 1; }, std::chrono::milliseconds(500), "callback should be able to send to created container");
    waitFor([&] { return dynamicContainerId.load() == 0; }, std::chrono::milliseconds(500), "callback should be able to delete container");
}

void testCallbackCanDisconnectAcceptedNode() {
    Instance server({.localPort = 13700, .maxNodeCount = 8, .maxContainerCount = 8});
    Instance client({.localPort = 13701, .maxNodeCount = 8, .maxContainerCount = 8});
    std::atomic<int> connectEvents {0};
    std::atomic<bool> disconnectTriggered {false};
    std::atomic<NodeId> acceptedNodeId {0};

    server.createContainer({
        .nameCode = makeNameCode('a', 'c', 'p', 't'),
        .objectCount = 1,
        .objectSize = 4,
        .onMessage = [&](const Message &message) {
            if (message.eventType == EventType::NodeConnected) {
                ++connectEvents;
                expect(message.payload.size() == sizeof(NodeId), "node event payload size should match NodeId");
                NodeId nodeIdFromEvent = 0;
                std::memcpy(&nodeIdFromEvent, message.payload.data(), sizeof(NodeId));
                acceptedNodeId = nodeIdFromEvent;
                server.disconnectNode(nodeIdFromEvent);
                disconnectTriggered = true;
            }
        },
    });

    server.startServer(NodeOptions {});
    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, "127.0.0.1", 13700);

    waitFor([&] { return connectEvents.load() == 1; }, std::chrono::milliseconds(500), "server should receive accepted-node callback");
    waitFor([&] { return disconnectTriggered.load(); }, std::chrono::milliseconds(500), "callback should disconnect accepted node");
    waitFor([&] { return acceptedNodeId.load() != 0; }, std::chrono::milliseconds(500), "accepted node id should be captured");
    server.deleteNode(acceptedNodeId.load());
    expectThrows([&] { client.disconnectNode(nodeId); }, "invalid node id");
    expectThrows([&] { client.deleteNode(nodeId); }, "invalid node id");
}

void testMultiInstanceMessageFlowAndStats() {
    constexpr std::size_t kClientCount = 6;
    constexpr std::size_t kMessageCount = 30;

    Instance server({.localPort = 13800, .maxNodeCount = 32, .maxContainerCount = 32});
    server.startServer(NodeOptions {});

    std::vector<std::unique_ptr<Instance>> clients;
    std::vector<NodeId> nodeIds;
    std::vector<ContainerId> clientContainers;
    std::vector<std::atomic<int>> received(kClientCount);
    std::atomic<int> serverEchoCount {0};

    for (std::size_t index = 0; index < kClientCount; ++index) {
        server.createContainer({
            .nameCode = makeNameCode('s', '0', static_cast<char>('0' + (index / 10)), static_cast<char>('0' + (index % 10))),
            .objectCount = 1,
            .objectSize = 4,
            .onMessage = [&](const Message &message) {
                if (message.eventType == static_cast<EventType>(2200)) {
                    ++serverEchoCount;
                    Message reply = message;
                    reply.destinationContainerId = message.sourceContainerId;
                    reply.eventType = static_cast<EventType>(2201);
                    server.sendMessage(reply);
                }
            },
        });
    }

    for (std::size_t index = 0; index < kClientCount; ++index) {
        clients.push_back(std::make_unique<Instance>(InstanceOptions {
            .localPort = static_cast<std::uint16_t>(13810 + index),
            .maxNodeCount = 4,
            .maxContainerCount = 4,
            .applicationData = static_cast<std::uint32_t>(index + 1),
        }));
        expect(clients.back()->applicationData() == index + 1, "client application data should be retained");

        const auto containerId = clients.back()->createContainer({
            .nameCode = makeNameCode('c', '0', static_cast<char>('0' + (index / 10)), static_cast<char>('0' + (index % 10))),
            .objectCount = 1,
            .objectSize = 4,
            .onMessage = [&, index](const Message &message) {
                if (message.eventType == static_cast<EventType>(2201)) {
                    ++received[index];
                }
            },
        });
        clientContainers.push_back(containerId);
        const auto nodeId = clients.back()->createNode(NodeOptions {});
        nodeIds.push_back(nodeId);
        clients.back()->connectNode(nodeId, "127.0.0.1", 13800);
    }

    for (std::size_t messageIndex = 0; messageIndex < kMessageCount; ++messageIndex) {
        for (std::size_t clientIndex = 0; clientIndex < kClientCount; ++clientIndex) {
            Message message;
            message.sourceContainerId = clientContainers[clientIndex];
            message.destinationNodeId = nodeIds[clientIndex];
            message.destinationContainerId = static_cast<ContainerId>(clientIndex + 1);
            message.eventType = static_cast<EventType>(2200);
            message.payload.assign({'o', 'k'});
            clients[clientIndex]->sendMessage(message);
        }
    }

    waitFor([&] { return serverEchoCount.load() == static_cast<int>(kClientCount * kMessageCount); },
            std::chrono::milliseconds(1500),
            "server should receive all multi-instance messages");

    for (std::size_t index = 0; index < kClientCount; ++index) {
        waitFor([&, index] { return received[index].load() == static_cast<int>(kMessageCount); },
                std::chrono::milliseconds(1500),
                "each client should receive all echoed replies");
    }

    for (std::size_t index = 0; index < kClientCount; ++index) {
        const auto stats = clients[index]->getContainerStats(clientContainers[index]);
        expect(stats.fsmQueue.receivedMessageCount >= kMessageCount, "client container stats should record inbound messages");
        clients[index]->disconnectNode(nodeIds[index]);
        clients[index]->deleteNode(nodeIds[index]);
    }
}

void testTelnetServerBasics() {
    Instance instance({.localPort = 13900, .telnetPort = 0, .maxNodeCount = 8, .maxContainerCount = 8});
    expect(instance.telnetPrompt() == "cppfsmp_object_tests$", "default telnet prompt should use the program name");
    instance.startTelnetServer();
    expect(instance.isTelnetServerRunning(), "telnet server should be running");
    expect(instance.telnetPort() >= 5000, "telnet server should pick a valid port");

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    const auto welcome = recvUntil(socketFd, "cppfsmp_object_tests$", std::chrono::milliseconds(1000));
    expect(welcome.find("welcome to fsmp telnet server") != std::string::npos, "telnet welcome banner should be shown");

    const std::string helpCommand = "help\n";
    ::send(socketFd, helpCommand.data(), helpCommand.size(), 0);
    const auto helpOutput = recvUntil(socketFd, "cppfsmp_object_tests$", std::chrono::milliseconds(1000));
    expect(helpOutput.find("available commands") != std::string::npos, "help should list commands");
    expect(helpOutput.find("status") != std::string::npos, "help should include built-in commands");

    const std::string byeCommand = "bye\n";
    ::send(socketFd, byeCommand.data(), byeCommand.size(), 0);
    ::close(socketFd);
    instance.stopTelnetServer();
    expect(!instance.isTelnetServerRunning(), "telnet server should stop cleanly");
}

void testTelnetPromptCustomization() {
    Instance instance({.localPort = 13905, .telnetPort = 5600, .maxNodeCount = 8, .maxContainerCount = 8});
    instance.setTelnetPrompt("demo_app$");
    expect(instance.telnetPrompt() == "demo_app$", "telnet prompt should be configurable");
    expectThrows([&] { instance.setTelnetPrompt(""); }, "prompt must not be empty");
    instance.startTelnetServer();

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    const auto welcome = recvUntil(socketFd, "demo_app$", std::chrono::milliseconds(1000));
    expect(welcome.find("demo_app$") != std::string::npos, "custom prompt should be shown to telnet client");

    ::close(socketFd);
}

void testTelnetCustomCommandsAndStatus() {
    Instance instance({.localPort = 13910, .telnetPort = 5601, .maxNodeCount = 8, .maxContainerCount = 8, .applicationData = 77});
    expect(instance.isTelnetShellCommandEnabled(), "shell compatibility mode should be enabled by default");
    const auto prompt = instance.telnetPrompt();
    instance.createContainer({
        .nameCode = makeNameCode('t', 'e', 'l', '1'),
        .objectCount = 2,
        .objectSize = sizeof(Lamp),
        .onMessage = [](const Message &) {},
    });
    instance.registerTelnetCommand(
        "sum",
        "sum <a> <b> - add two integers",
        [](Instance &, const std::vector<std::string> &args, std::string &output) {
            if (args.size() != 3) {
                output = "usage: sum <a> <b>\n";
                return -1;
            }
            output = std::to_string(std::stoi(args[1]) + std::stoi(args[2])) + "\n";
            return 0;
        });
    instance.startTelnetServer();

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    static_cast<void>(recvUntil(socketFd, prompt, std::chrono::milliseconds(1000)));

    const std::string statusCommand = "status\n";
    ::send(socketFd, statusCommand.data(), statusCommand.size(), 0);
    const auto statusOutput = recvUntil(socketFd, prompt, std::chrono::milliseconds(1000));
    expect(statusOutput.find("app_data=77") != std::string::npos, "status should include application data");
    expect(statusOutput.find("containers=1") != std::string::npos, "status should include container count");

    const std::string sumCommand = "sum 7 5\n";
    ::send(socketFd, sumCommand.data(), sumCommand.size(), 0);
    const auto sumOutput = recvUntil(socketFd, prompt, std::chrono::milliseconds(1000));
    expect(sumOutput.find("12") != std::string::npos, "custom command should run");

    const std::string missingCommand = "missing_command\n";
    ::send(socketFd, missingCommand.data(), missingCommand.size(), 0);
    const auto missingOutput = recvUntil(socketFd, prompt, std::chrono::milliseconds(1000));
    expect(missingOutput.find("doesn't exist") != std::string::npos, "unregistered command should still report missing");

    const std::string shellCommand = "!printf shell_mode_ok\n";
    ::send(socketFd, shellCommand.data(), shellCommand.size(), 0);
    const auto shellOutput = recvUntil(socketFd, prompt, std::chrono::milliseconds(1000));
    expect(shellOutput.find("shell_mode_ok") != std::string::npos, "!command should execute through the shell");

    ::close(socketFd);
}

void testTelnetPortConflictAndLogging() {
    Instance instance({.localPort = 5611, .telnetPort = 5611, .maxNodeCount = 8, .maxContainerCount = 8});
    instance.startTelnetServer();
    expectThrows([&] { instance.startServer(NodeOptions {}); }, "conflicts with telnet port");

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    static_cast<void>(recvUntil(socketFd, instance.telnetPrompt(), std::chrono::milliseconds(1000)));
    instance.log(LogLevel::Info, "telnet-log-test");
    const auto logOutput = recvUntil(socketFd, "telnet-log-test", std::chrono::milliseconds(1000));
    expect(logOutput.find("telnet-log-test") != std::string::npos, "log output should be mirrored to telnet client");

    ::close(socketFd);
}

void testTelnetShellCommandToggle() {
    Instance instance({.localPort = 13930, .telnetPort = 5602, .maxNodeCount = 8, .maxContainerCount = 8});
    instance.setTelnetShellCommandEnabled(false);
    expect(!instance.isTelnetShellCommandEnabled(), "shell compatibility mode should be disableable");
    instance.startTelnetServer();

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    static_cast<void>(recvUntil(socketFd, instance.telnetPrompt(), std::chrono::milliseconds(1000)));

    const std::string shellCommand = "!printf should_not_run\n";
    ::send(socketFd, shellCommand.data(), shellCommand.size(), 0);
    const auto shellOutput = recvUntil(socketFd, instance.telnetPrompt(), std::chrono::milliseconds(1000));
    expect(shellOutput.find("shell command mode is disabled") != std::string::npos, "disabled shell mode should block !commands");
    expect(shellOutput.find("should_not_run") == std::string::npos, "disabled shell mode should not execute shell commands");

    ::close(socketFd);
}

void testTelnetShellCommandTimeout() {
    Instance instance({.localPort = 13940, .telnetPort = 5603, .maxNodeCount = 8, .maxContainerCount = 8});
    instance.setTelnetShellCommandTimeout(std::chrono::milliseconds(100));
    expect(instance.telnetShellCommandTimeout() == std::chrono::milliseconds(100), "shell timeout should be configurable");
    expectThrows([&] { instance.setTelnetShellCommandTimeout(std::chrono::milliseconds(0)); }, "timeout must be positive");
    instance.startTelnetServer();

    const auto socketFd = connectToLocalPort(instance.telnetPort());
    static_cast<void>(recvUntil(socketFd, instance.telnetPrompt(), std::chrono::milliseconds(1000)));

    const std::string timeoutCommand = "!sleep 1\n";
    ::send(socketFd, timeoutCommand.data(), timeoutCommand.size(), 0);
    const auto timeoutOutput = recvUntil(socketFd, instance.telnetPrompt(), std::chrono::milliseconds(2000));
    expect(timeoutOutput.find("command timed out") != std::string::npos, "long-running shell command should time out");

    ::close(socketFd);
}

}  // namespace

int main() {
    try {
        const auto run = [](const char *name, const auto &fn) {
            std::cout << "Running " << name << std::endl;
            fn();
        };

        run("testInstanceLifecycle", testInstanceLifecycle);
        run("testRepeatedInstanceConstruction", testRepeatedInstanceConstruction);
        run("testServerLifecycleAndDoubleStart", testServerLifecycleAndDoubleStart);
        run("testContainerLifecycleAndObjectAccess", testContainerLifecycleAndObjectAccess);
        run("testContainerValidationAndCapacity", testContainerValidationAndCapacity);
        run("testObjectAccessValidation", testObjectAccessValidation);
        run("testNodeLifecycleAndValidation", testNodeLifecycleAndValidation);
        run("testNodeSystemEvents", testNodeSystemEvents);
        run("testLocalMessageDelivery", testLocalMessageDelivery);
        run("testLocalMessageByNameAndMissingName", testLocalMessageByNameAndMissingName);
        run("testRemoteRoundTrip", testRemoteRoundTrip);
        run("testRemoteMessageByNameAndMissingName", testRemoteMessageByNameAndMissingName);
        run("testRemoteNameQuery", testRemoteNameQuery);
        run("testMessageValidationPaths", testMessageValidationPaths);
        run("testTimerOnceAndCycle", testTimerOnceAndCycle);
        run("testTimerValidationAndMixedSchedules", testTimerValidationAndMixedSchedules);
        run("testCallbackSelfSendAndContainerMutation", testCallbackSelfSendAndContainerMutation);
        run("testCallbackCanDisconnectAcceptedNode", testCallbackCanDisconnectAcceptedNode);
        run("testMultiInstanceMessageFlowAndStats", testMultiInstanceMessageFlowAndStats);
        run("testTelnetServerBasics", testTelnetServerBasics);
        run("testTelnetPromptCustomization", testTelnetPromptCustomization);
        run("testTelnetCustomCommandsAndStatus", testTelnetCustomCommandsAndStatus);
        run("testTelnetPortConflictAndLogging", testTelnetPortConflictAndLogging);
        run("testTelnetShellCommandToggle", testTelnetShellCommandToggle);
        run("testTelnetShellCommandTimeout", testTelnetShellCommandTimeout);
        std::cout << "All OO FSMP tests passed." << std::endl;
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "Test failed: " << error.what() << std::endl;
        return 1;
    }
}
