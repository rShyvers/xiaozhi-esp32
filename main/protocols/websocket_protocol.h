#ifndef _WEBSOCKET_PROTOCOL_H_
#define _WEBSOCKET_PROTOCOL_H_


#include "protocol.h"

#include <web_socket.h>
#include <freertos/FreeRTOS.h>
#include <freertos/event_groups.h>
#include <freertos/task.h>
#include <atomic>
#include <mutex>
#include <memory>

#define WEBSOCKET_PROTOCOL_SERVER_HELLO_EVENT (1 << 0)
#define WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT (1 << 1)

class WebsocketProtocol : public Protocol {
public:
    WebsocketProtocol();
    ~WebsocketProtocol();

    bool Start() override;
    bool SendAudio(std::unique_ptr<AudioStreamPacket> packet) override;
    bool OpenAudioChannel() override;
    void CloseAudioChannel(bool send_goodbye = true) override;
    bool IsAudioChannelOpened() const override;

private:
    EventGroupHandle_t event_group_handle_;
    std::unique_ptr<WebSocket> websocket_;
    int version_ = 1;
    std::mutex socket_mutex_;
    TaskHandle_t keepalive_task_ = nullptr;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> close_pending_{false};
    std::shared_ptr<std::atomic<bool>> alive_ = std::make_shared<std::atomic<bool>>(true);
    uint32_t socket_generation_ = 0;
    std::shared_ptr<std::atomic<bool>> socket_alive_;

    void ParseServerHello(const cJSON* root);
    bool SendText(const std::string& text) override;
    std::string GetHelloMessage();
    void KeepaliveTask();
    void StartKeepalive();
    bool StopKeepalive();
    bool ResetWebsocket();
};

#endif
