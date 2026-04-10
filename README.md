# cppfsmp

`cppfsmp` 是一个基于 C++17 和 CMake 的轻量级 FSMP 运行时工程，提供了容器、节点、消息投递、定时器和内置 telnet 运维入口等能力。

当前工程包含：

- `fsmp_runtime` 静态库
- `fsmp_tests` 测试程序
- `echo_server` / `echo_client` 跨进程回显示例

## 功能特性

- 基于 `Instance` 的运行时实例管理
- 基于 `Container` 的对象与消息处理模型
- 本地与远端节点连接、断连和事件广播
- 远端容器名称查询
- 一次性 / 周期性定时器
- 内置 telnet 服务与自定义命令注册
- 基本队列统计能力

## 目录结构

```text
.
├── CMakeLists.txt
├── include/fsmp/runtime.hpp
├── src/runtime.cpp
├── tests/fsmp_tests.cpp
├── examples/
│   ├── README.md
│   ├── echo_server.cpp
│   └── echo_client.cpp
└── LICENSE
```

## 构建要求

- CMake 3.20 或更高
- 支持 C++17 的编译器
- POSIX 线程与 socket 环境

> 目前实现使用了 `pthread`、`arpa/inet.h`、`unistd.h` 等接口，更适合在 Linux / macOS 这类类 Unix 环境下构建和运行。

## 快速开始

### 配置与编译

在项目根目录执行：

```bash
cmake -S . -B build
cmake --build build
```

编译完成后，默认会生成：

- `build/libfsmp_runtime.a`
- `build/fsmp_tests`
- `build/echo_server`
- `build/echo_client`

### 运行测试

```bash
./build/fsmp_tests
```

测试主要覆盖：

- 实例生命周期
- 容器创建、校验与对象访问
- 节点创建、连接与断连
- 远端容器查询与消息收发
- telnet 服务相关行为

### 运行示例

先启动服务端：

```bash
./build/echo_server
```

再在另一个终端启动客户端：

```bash
./build/echo_client 127.0.0.1
```

客户端输入任意文本后，服务端会收到消息并原样回显。输入 `quit` 或 `exit` 可退出客户端。

更多示例说明见 [`examples/README.md`](/Volumes/wd0/Users/yuan/clients/main/fsmp/examples/README.md)。

## 核心概念

### Instance

`fsmp::Instance` 是运行时核心对象，负责：

- 服务监听与关闭
- 容器创建与销毁
- 节点创建、连接、断连
- 消息发送
- 定时器管理
- telnet 服务管理

### Container

容器通过 `createContainer` 创建，通常需要提供：

- `nameCode`：4 字节名称编码
- `objectCount` / `objectSize`：容器对象存储空间
- `onMessage`：消息处理回调

### Message

`fsmp::Message` 用于描述消息路由信息和负载内容，包含：

- 源 / 目标节点与容器信息
- 事件类型 `eventType`
- 定时器信息
- `payload` 二进制负载

## 最小使用示例

```cpp
#include "fsmp/runtime.hpp"

#include <iostream>

int main() {
    using namespace fsmp;

    Instance instance({
        .localPort = 2100,
        .maxNodeCount = 8,
        .maxContainerCount = 8,
    });

    const auto containerId = instance.createContainer({
        .nameCode = makeNameCode('d', 'e', 'm', 'o'),
        .objectCount = 1,
        .objectSize = 1,
        .onMessage = [](const Message &message) {
            std::cout << "event=" << static_cast<std::uint32_t>(message.eventType)
                      << ", payload=" << message.payloadAsString() << std::endl;
        },
    });

    (void)containerId;
    instance.startServer(NodeOptions {});
    return 0;
}
```

如果你希望链接该库，可在自己的 `CMakeLists.txt` 中按类似方式接入：

```cmake
add_subdirectory(path/to/fsmp)
target_link_libraries(your_target PRIVATE fsmp_runtime pthread)
```

## Telnet 能力

运行时支持启动内置 telnet 服务，并注册自定义命令：

- `startTelnetServer()`
- `registerTelnetCommand(...)`
- `setTelnetPrompt(...)`
- `setTelnetShellCommandEnabled(...)`

在示例 `echo_server` 中，已经注册了 `echostats` 命令用于查看基础统计信息。

## 适用场景

- 多容器消息驱动程序原型
- 进程间节点通信实验
- 嵌入式 / 工业控制风格状态机运行时验证
- 需要简单远程诊断入口的本地服务程序

## 许可

本项目采用 [`LICENSE`](/Volumes/wd0/Users/yuan/clients/main/fsmp/LICENSE) 中声明的许可协议。
