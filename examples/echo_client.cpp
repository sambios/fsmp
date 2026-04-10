#include "fsmp/runtime.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

namespace {

using namespace fsmp;

constexpr std::uint16_t kServerPort = 2100;
constexpr std::uint16_t kClientPort = 2101;
const NameCode kServerName = makeNameCode('e', 'c', 'h', 'o');
const NameCode kClientName = makeNameCode('c', 'l', 'n', 't');
constexpr EventType kEchoEvent = static_cast<EventType>(2001);

}  // namespace

int main(int argc, char **argv) {
    const std::string serverIp = argc > 1 ? argv[1] : "127.0.0.1";

    Instance client({
        .localPort = kClientPort,
        .maxNodeCount = 8,
        .maxContainerCount = 8,
    });

    ContainerId clientContainerId = 0;
    clientContainerId = client.createContainer({
        .nameCode = kClientName,
        .objectCount = 1,
        .objectSize = 1,
        .onMessage = [&](const Message &message) {
            if (message.eventType == EventType::NodeConnected) {
                std::cout << "[client] node connected" << std::endl;
                return;
            }
            if (message.eventType == EventType::NodeDisconnected) {
                std::cout << "[client] node disconnected" << std::endl;
                return;
            }
            if (message.eventType == kEchoEvent) {
                std::cout << "[echo] " << message.payloadAsString() << std::endl;
            }
        },
    });

    const auto nodeId = client.createNode(NodeOptions {});
    client.connectNode(nodeId, serverIp, kServerPort);

    auto remoteInfo = client.queryRemoteContainer(nodeId, kServerName);
    if (!remoteInfo.has_value()) {
        std::cerr << "failed to find remote echo container" << std::endl;
        return 1;
    }

    std::cout << "connected to fsmp echo server at " << serverIp << ":" << kServerPort << std::endl;
    std::cout << "type text and press Enter, type quit to exit" << std::endl;

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "quit" || line == "exit") {
            break;
        }

        Message message;
        message.sourceContainerId = clientContainerId;
        message.destinationNodeId = nodeId;
        message.destinationContainerId = remoteInfo->remoteContainerId;
        message.eventType = kEchoEvent;
        message.payload.assign(line.begin(), line.end());
        client.sendMessage(message);

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    client.disconnectNode(nodeId);
    client.deleteNode(nodeId);
    return 0;
}
