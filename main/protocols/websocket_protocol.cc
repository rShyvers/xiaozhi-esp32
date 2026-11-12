#include "websocket_protocol.h"
#include "board.h"
#include "system_info.h"
#include "application.h"
#include "settings.h"

#include <cstring>
#include <cJSON.h>
#include <esp_log.h>
#include <arpa/inet.h>
#include "assets/lang_config.h"

#define TAG "WS"

WebsocketProtocol::WebsocketProtocol() {
    event_group_handle_ = xEventGroupCreate();
}

WebsocketProtocol::~WebsocketProtocol() {
    *alive_ = false;
    if (!ResetWebsocket()) {
        // Ping has no bounded-send API. Never free its socket or task argument
        // while it is still in use, even if shutdown has to wait for transport.
        ESP_LOGW(TAG, "Waiting for stalled keepalive during shutdown");
        xEventGroupWaitBits(event_group_handle_, WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT,
            pdFALSE, pdTRUE, portMAX_DELAY);
        ResetWebsocket();
    }
    vEventGroupDelete(event_group_handle_);
}

bool WebsocketProtocol::Start() {
    // Only connect to server when audio channel is needed
    return true;
}

bool WebsocketProtocol::SendAudio(std::unique_ptr<AudioStreamPacket> packet) {
    if (version_ == 2) {
        std::string serialized;
        serialized.resize(sizeof(BinaryProtocol2) + packet->payload.size());
        auto bp2 = (BinaryProtocol2*)serialized.data();
        bp2->version = htons(version_);
        bp2->type = 0;
        bp2->reserved = 0;
        bp2->timestamp = htonl(packet->timestamp);
        bp2->payload_size = htonl(packet->payload.size());
        memcpy(bp2->payload, packet->payload.data(), packet->payload.size());

        std::lock_guard<std::mutex> lock(socket_mutex_);
        return websocket_ && websocket_->IsConnected() &&
            websocket_->Send(serialized.data(), serialized.size(), true);
    } else if (version_ == 3) {
        std::string serialized;
        serialized.resize(sizeof(BinaryProtocol3) + packet->payload.size());
        auto bp3 = (BinaryProtocol3*)serialized.data();
        bp3->type = 0;
        bp3->reserved = 0;
        bp3->payload_size = htons(packet->payload.size());
        memcpy(bp3->payload, packet->payload.data(), packet->payload.size());

        std::lock_guard<std::mutex> lock(socket_mutex_);
        return websocket_ && websocket_->IsConnected() &&
            websocket_->Send(serialized.data(), serialized.size(), true);
    } else {
        std::lock_guard<std::mutex> lock(socket_mutex_);
        return websocket_ && websocket_->IsConnected() &&
            websocket_->Send(packet->payload.data(), packet->payload.size(), true);
    }
}

bool WebsocketProtocol::SendText(const std::string& text) {
    bool sent;
    {
        std::lock_guard<std::mutex> lock(socket_mutex_);
        if (websocket_ == nullptr || !websocket_->IsConnected()) {
            return false;
        }
        sent = websocket_->Send(text);
    }

    if (!sent) {
        ESP_LOGE(TAG, "Failed to send text: %s", text.c_str());
        SetError(Lang::Strings::SERVER_ERROR);
        return false;
    }

    return true;
}

bool WebsocketProtocol::IsAudioChannelOpened() const {
    // Conversation inactivity is not transport failure (Pongs are consumed
    // below OnData). Socket disconnect/error and server close own liveness.
    return websocket_ != nullptr && websocket_->IsConnected() && !error_occurred_ && !close_pending_;
}

void WebsocketProtocol::CloseAudioChannel(bool send_goodbye) {
    (void)send_goodbye;  // Websocket doesn't need to send goodbye message
    bool notify = websocket_ && websocket_->IsConnected();
    if (ResetWebsocket() && notify && on_audio_channel_closed_) {
        on_audio_channel_closed_();
    }
}

void WebsocketProtocol::KeepaliveTask() {
    const auto generation = socket_generation_;
    auto alive = alive_;
    while (!stop_requested_) {
        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(25000));
        if (stop_requested_) {
            break;
        }
        // Network I/O is confined to this task, never ESP_TIMER_TASK.
        std::lock_guard<std::mutex> lock(socket_mutex_);
        if (!stop_requested_ && websocket_ && websocket_->IsConnected()) {
            websocket_->Ping();
        }
    }
    if (close_pending_) {
        Application::GetInstance().ScheduleAfterAudioChannelOpen([this, alive, generation]() {
            if (!*alive) {
                return;
            }
            if (generation != socket_generation_ || !close_pending_) {
                ESP_LOGD(TAG, "Dropping stale keepalive close request");
                return;
            }
            // ResetWebsocket re-verifies the joined acknowledgement itself.
            CloseAudioChannel();
        });
    }
    // FreeRTOS task deletion does not run C++ stack destructors.
    alive.reset();
    // No access to this after signaling: the owner may now destroy us.
    xEventGroupSetBits(event_group_handle_, WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT);
    // The owner deletes the task after the acknowledgement, keeping its
    // handle valid even if StopKeepalive races the final notification.
    vTaskSuspend(nullptr);
}

void WebsocketProtocol::StartKeepalive() {
    stop_requested_ = false;
    close_pending_ = false;
    xEventGroupClearBits(event_group_handle_, WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT);
    if (xTaskCreate([](void* arg) {
        static_cast<WebsocketProtocol*>(arg)->KeepaliveTask();
    }, "ws_keepalive", 4096, this, 3, &keepalive_task_) != pdPASS) {
        keepalive_task_ = nullptr;
        ESP_LOGW(TAG, "Failed to start websocket keepalive task");
    }
}

bool WebsocketProtocol::StopKeepalive() {
    if (keepalive_task_ == nullptr) {
        return true;
    }
    close_pending_ = true;
    stop_requested_ = true;
    // The owner retains the handle until the task acknowledges stopping.
    if (!(xEventGroupGetBits(event_group_handle_) & WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT)) {
        xTaskNotifyGive(keepalive_task_);
    }
    auto bits = xEventGroupWaitBits(event_group_handle_, WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT,
        pdFALSE, pdTRUE, pdMS_TO_TICKS(500));
    if (!(bits & WEBSOCKET_PROTOCOL_KEEPALIVE_STOPPED_EVENT)) {
        ESP_LOGW(TAG, "Keepalive send stalled; deferring websocket close");
        return false;
    }
    vTaskDelete(keepalive_task_);
    keepalive_task_ = nullptr;
    return true;
}

bool WebsocketProtocol::ResetWebsocket() {
    if (!StopKeepalive()) {
        return false;
    }
    // Joining the ping task is the lifetime barrier. Destruction is outside
    // the socket-operation lock (the receive task may need to finish callbacks).
    if (socket_alive_) {
        *socket_alive_ = false;
    }
    websocket_.reset();
    ++socket_generation_;
    close_pending_ = false;
    return true;
}

bool WebsocketProtocol::OpenAudioChannel() {
    if (!ResetWebsocket()) {
        return false;
    }
    xEventGroupClearBits(event_group_handle_, WEBSOCKET_PROTOCOL_SERVER_HELLO_EVENT);
    Settings settings("websocket", false);
    std::string url = settings.GetString("url");
    std::string token = settings.GetString("token");
    int version = settings.GetInt("version");
    if (version != 0) {
        version_ = version;
    }

    error_occurred_ = false;

    auto network = Board::GetInstance().GetNetwork();
    websocket_ = network->CreateWebSocket(1);
    if (websocket_ == nullptr) {
        ESP_LOGE(TAG, "Failed to create websocket");
        return false;
    }

    if (!token.empty()) {
        // If token not has a space, add "Bearer " prefix
        if (token.find(" ") == std::string::npos) {
            token = "Bearer " + token;
        }
        websocket_->SetHeader("Authorization", token.c_str());
    }
    websocket_->SetHeader("Protocol-Version", std::to_string(version_).c_str());
    websocket_->SetHeader("Device-Id", SystemInfo::GetMacAddress().c_str());
    websocket_->SetHeader("Client-Id", Board::GetInstance().GetUuid().c_str());

    socket_alive_ = std::make_shared<std::atomic<bool>>(true);
    websocket_->OnData([this, socket_alive = socket_alive_](const char* data, size_t len, bool binary) {
        if (!*socket_alive) {
            return;
        }
        if (binary) {
            if (on_incoming_audio_ != nullptr) {
                if (version_ == 2) {
                    BinaryProtocol2* bp2 = (BinaryProtocol2*)data;
                    bp2->version = ntohs(bp2->version);
                    bp2->type = ntohs(bp2->type);
                    bp2->timestamp = ntohl(bp2->timestamp);
                    bp2->payload_size = ntohl(bp2->payload_size);
                    auto payload = (uint8_t*)bp2->payload;
                    on_incoming_audio_(std::make_unique<AudioStreamPacket>(AudioStreamPacket{
                        .sample_rate = server_sample_rate_,
                        .frame_duration = server_frame_duration_,
                        .timestamp = bp2->timestamp,
                        .payload = std::vector<uint8_t>(payload, payload + bp2->payload_size)
                    }));
                } else if (version_ == 3) {
                    BinaryProtocol3* bp3 = (BinaryProtocol3*)data;
                    bp3->type = bp3->type;
                    bp3->payload_size = ntohs(bp3->payload_size);
                    auto payload = (uint8_t*)bp3->payload;
                    on_incoming_audio_(std::make_unique<AudioStreamPacket>(AudioStreamPacket{
                        .sample_rate = server_sample_rate_,
                        .frame_duration = server_frame_duration_,
                        .timestamp = 0,
                        .payload = std::vector<uint8_t>(payload, payload + bp3->payload_size)
                    }));
                } else {
                    on_incoming_audio_(std::make_unique<AudioStreamPacket>(AudioStreamPacket{
                        .sample_rate = server_sample_rate_,
                        .frame_duration = server_frame_duration_,
                        .timestamp = 0,
                        .payload = std::vector<uint8_t>((uint8_t*)data, (uint8_t*)data + len)
                    }));
                }
            }
        } else {
            // Parse JSON data
            auto root = cJSON_Parse(data);
            auto type = cJSON_GetObjectItem(root, "type");
            if (cJSON_IsString(type)) {
                if (strcmp(type->valuestring, "hello") == 0) {
                    ParseServerHello(root);
                } else {
                    if (on_incoming_json_ != nullptr) {
                        on_incoming_json_(root);
                    }
                }
            } else {
                ESP_LOGE(TAG, "Missing message type, data: %s", data);
            }
            cJSON_Delete(root);
        }
        last_incoming_time_ = std::chrono::steady_clock::now();
    });

    websocket_->OnDisconnected([this, alive = alive_, socket_alive = socket_alive_, generation = socket_generation_]() {
        if (!*socket_alive) {
            return;
        }
        ESP_LOGI(TAG, "Websocket disconnected");
        // Receive context only signals; joining here could deadlock its own
        // socket teardown. Lifetime/generation fencing rejects stale requests.
        Application::GetInstance().ScheduleAfterAudioChannelOpen([this, alive, generation]() {
            if (*alive && generation == socket_generation_) {
                CloseAudioChannel();
            }
        });
        if (on_audio_channel_closed_ != nullptr) {
            on_audio_channel_closed_();
        }
    });

    ESP_LOGI(TAG, "Connecting to websocket server: %s with version: %d", url.c_str(), version_);
    if (!websocket_->Connect(url.c_str())) {
        ESP_LOGE(TAG, "Failed to connect to websocket server, code=%d", websocket_->GetLastError());
        SetError(Lang::Strings::SERVER_NOT_CONNECTED);
        return false;
    }

    // Send hello message to describe the client
    auto message = GetHelloMessage();
    if (!SendText(message)) {
        return false;
    }

    // Wait for server hello
    EventBits_t bits = xEventGroupWaitBits(event_group_handle_, WEBSOCKET_PROTOCOL_SERVER_HELLO_EVENT, pdTRUE, pdFALSE, pdMS_TO_TICKS(10000));
    if (!(bits & WEBSOCKET_PROTOCOL_SERVER_HELLO_EVENT)) {
        ESP_LOGE(TAG, "Failed to receive server hello");
        SetError(Lang::Strings::SERVER_TIMEOUT);
        return false;
    }

    StartKeepalive();

    if (on_audio_channel_opened_ != nullptr) {
        on_audio_channel_opened_();
    }

    return true;
}

std::string WebsocketProtocol::GetHelloMessage() {
    // keys: message type, version, audio_params (format, sample_rate, channels)
    cJSON* root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "type", "hello");
    cJSON_AddNumberToObject(root, "version", version_);
    cJSON* features = cJSON_CreateObject();
#if CONFIG_USE_SERVER_AEC
    cJSON_AddBoolToObject(features, "aec", true);
#endif
    cJSON_AddBoolToObject(features, "mcp", true);
    cJSON_AddItemToObject(root, "features", features);
    cJSON_AddStringToObject(root, "transport", "websocket");
    cJSON* audio_params = cJSON_CreateObject();
    cJSON_AddStringToObject(audio_params, "format", "opus");
    cJSON_AddNumberToObject(audio_params, "sample_rate", 16000);
    cJSON_AddNumberToObject(audio_params, "channels", 1);
    cJSON_AddNumberToObject(audio_params, "frame_duration", OPUS_FRAME_DURATION_MS);
    cJSON_AddItemToObject(root, "audio_params", audio_params);
    auto json_str = cJSON_PrintUnformatted(root);
    std::string message(json_str);
    cJSON_free(json_str);
    cJSON_Delete(root);
    return message;
}

void WebsocketProtocol::ParseServerHello(const cJSON* root) {
    auto transport = cJSON_GetObjectItem(root, "transport");
    if (transport == nullptr || strcmp(transport->valuestring, "websocket") != 0) {
        ESP_LOGE(TAG, "Unsupported transport: %s", transport->valuestring);
        return;
    }

    auto session_id = cJSON_GetObjectItem(root, "session_id");
    if (cJSON_IsString(session_id)) {
        session_id_ = session_id->valuestring;
        ESP_LOGI(TAG, "Session ID: %s", session_id_.c_str());
    }

    auto audio_params = cJSON_GetObjectItem(root, "audio_params");
    if (cJSON_IsObject(audio_params)) {
        auto sample_rate = cJSON_GetObjectItem(audio_params, "sample_rate");
        if (cJSON_IsNumber(sample_rate)) {
            server_sample_rate_ = sample_rate->valueint;
        }
        auto frame_duration = cJSON_GetObjectItem(audio_params, "frame_duration");
        if (cJSON_IsNumber(frame_duration)) {
            server_frame_duration_ = frame_duration->valueint;
        }
    }

    xEventGroupSetBits(event_group_handle_, WEBSOCKET_PROTOCOL_SERVER_HELLO_EVENT);
}
