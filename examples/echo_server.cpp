#include "fsmp/runtime.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

namespace {

using namespace fsmp;

constexpr std::uint16_t kServerPort = 2100;
constexpr std::uint16_t kTelnetPort = 2000;
const NameCode kServerName = makeNameCode('e', 'c', 'h', 'o');
constexpr EventType kEchoEvent = static_cast<EventType>(2001);

std::atomic<bool> g_running {true};

void handleSignal(int) {
    g_running = false;
}

}  // namespace

int main() {
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    Instance server({
        .localPort = kServerPort,
        .telnetPort = kTelnetPort,
        .maxNodeCount = 16,
        .maxContainerCount = 8,
    });

    std::atomic<int> messageCount {0};
    std::atomic<int> connectedCount {0};
    ContainerId serverContainerId = 0;
    serverContainerId = server.createContainer({
        .nameCode = kServerName,
        .objectCount = 1,
        .objectSize = 1,
        .onMessage = [&](const Message &message) {
            if (message.eventType == EventType::NodeConnected) {
                ++connectedCount;
                server.log(LogLevel::Info, "echo client connected");
                std::cout << "[server] node connected" << std::endl;
                return;
            }
            if (message.eventType == EventType::NodeDisconnected) {
                server.log(LogLevel::Info, "echo client disconnected");
                std::cout << "[server] node disconnected" << std::endl;
                return;
            }
            if (message.eventType != kEchoEvent) {
                return;
            }

            ++messageCount;
            const auto text = message.payloadAsString();
            server.log(LogLevel::Info, "echo payload: " + text);
            std::cout << "[server] received: " << text << std::endl;

            Message reply;
            reply.sourceContainerId = serverContainerId;
            reply.destinationNodeId = message.destinationNodeId;
            reply.destinationContainerId = message.sourceContainerId;
            reply.eventType = kEchoEvent;
            reply.payload = message.payload;
            server.sendMessage(reply);
        },
    });

    server.registerTelnetCommand(
        "echostats",
        "echostats - show fsmp echo server statistics",
        [&](Instance &, const std::vector<std::string> &, std::string &output) {
            output = "fsmp_port=" + std::to_string(kServerPort) +
                     "\ncontainer_name=echo" +
                     "\nconnected_events=" + std::to_string(connectedCount.load()) +
                     "\nmessage_count=" + std::to_string(messageCount.load()) + "\n";
            return 0;
        });

    server.startServer(NodeOptions {});
    server.startTelnetServer();

    std::cout << "fsmp echo server started" << std::endl;
    std::cout << "fsmp port: " << kServerPort << std::endl;
    std::cout << "telnet port: " << server.telnetPort() << std::endl;
    std::cout << "container name: echo" << std::endl;
    std::cout << "telnet command: echostats" << std::endl;
    std::cout << "press Ctrl+C to stop" << std::endl;

    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "fsmp echo server stopping" << std::endl;
    return 0;
}
