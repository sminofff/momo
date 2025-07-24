#include "pion_client.h"

// boost
#include <boost/beast/websocket/stream.hpp>
#include <boost/json.hpp>

// std
#include <chrono>
#include <thread>
#include <cstdlib>

#include "momo_version.h"
#include "ssl_verifier.h"
#include "url_parts.h"
#include "util.h"

bool PionClient::ParseURL(URLParts& parts) const {
  std::string url = config_.signaling_url;

  if (!URLParts::Parse(url, parts)) {
    throw std::exception();
  }

  std::string default_port;
  if (parts.scheme == "wss") {
    return true;
  } else if (parts.scheme == "ws") {
    return false;
  } else {
    throw std::exception();
  }
}

void PionClient::GetStats(
    std::function<void(const rtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
        callback) {
  if (connection_ && (rtc_state_ ==
                         webrtc::PeerConnectionInterface::IceConnectionState::
                             kIceConnectionConnected ||
                       rtc_state_ ==
                         webrtc::PeerConnectionInterface::IceConnectionState::
                             kIceConnectionCompleted
                             )) {
    connection_->GetStats(std::move(callback));
  } else {
    callback(nullptr);
  }
}

PionClient::PionClient(boost::asio::io_context& ioc,
                       RTCManager* manager,
                       PionClientConfig config)
    : ioc_(ioc),
      manager_(manager),
      retry_count_(0),
      config_(std::move(config)),
      watchdog_(ioc, std::bind(&PionClient::OnWatchdogExpired, this)) {
  Reset();
}

PionClient::~PionClient() {
  destructed_ = true;
  connection_ = nullptr;
}

void PionClient::Reset() {
  // Soraモードと同様にシンプルにリセット
  watchdog_.Disable();
  connection_ = nullptr;
  
  // WebSocketを再作成
  URLParts parts;
  if (ParseURL(parts)) {
    ws_.reset(new Websocket(Websocket::ssl_tag(), ioc_, config_.insecure,
                            config_.client_cert, config_.client_key));
  } else {
    ws_.reset(new Websocket(ioc_));
  }
  
  RTC_LOG(LS_INFO) << "Reset completed";
}

void PionClient::Connect() {
  RTC_LOG(LS_INFO) << __FUNCTION__ << " - Starting connection to " << config_.signaling_url;

  // WebSocketが存在することを確認
  if (!ws_) {
    RTC_LOG(LS_ERROR) << "WebSocket is null, cannot connect!";
    return;
  }

  watchdog_.Enable(20);  // SFUのpongTimeout(20秒)に合わせる

  RTC_LOG(LS_INFO) << "Calling ws_->Connect to " << config_.signaling_url;
  ws_->Connect(config_.signaling_url,
               std::bind(&PionClient::OnConnect, shared_from_this(),
                         std::placeholders::_1));
}

void PionClient::ReconnectAfter() {
  // Ayameと同じ再接続ロジック
  int interval = 5 * (2 * retry_count_);  // 0, 10, 20, 30秒
  if (interval > 30) {
    interval = 30;
  }
  
  // 初回は少し待機（DTLSのクリーンアップを待つ）
  if (interval == 0) {
    interval = 2;  // 2秒待機
  }
  
  RTC_LOG(LS_INFO) << __FUNCTION__ << " - Setting watchdog timer for " << interval << " seconds"
                   << " (retry #" << retry_count_ + 1 << ")";
  
  watchdog_.Enable(interval);
  retry_count_++;
  
  RTC_LOG(LS_INFO) << __FUNCTION__ << " - Watchdog enabled, waiting for timer expiration";
}

void PionClient::OnWatchdogExpired() {
  RTC_LOG(LS_WARNING) << __FUNCTION__ << " - Watchdog timer expired, exiting...";

  // 再接続せずに終了
  std::exit(1);
}

void PionClient::OnConnect(boost::system::error_code ec) {
  if (ec) {
    RTC_LOG(LS_ERROR) << "Failed to connect to SFU, exiting...";
    std::exit(1);
  }

  // Ayameモードと同様に、offerを受け取ってからPeerConnectionを作成する
  // これにより、再接続時の古いRTPストリーム状態の問題を回避
  RTC_LOG(LS_INFO) << "WebSocket connected, waiting for offer from SFU";

  // Send capability information to SFU
  SendCapability();

  // 読み込みを開始
  DoRead();
}

void PionClient::DoRead() {
  ws_->Read(std::bind(&PionClient::OnRead, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3));
}

void PionClient::CreatePeerConnection() {
  // 既存の接続があれば先にクリーンアップ
  if (connection_) {
    RTC_LOG(LS_WARNING) << "Existing PeerConnection found, cleaning up...";
    
    // PeerConnectionを明示的に閉じる
    auto pc = connection_->GetConnection();
    if (pc) {
      pc->Close();
    }
    connection_ = nullptr;
  }

  // PeerConnectionを作成
  CreatePeerConnectionInternal();
}

void PionClient::CreatePeerConnectionInternal() {
  webrtc::PeerConnectionInterface::RTCConfiguration rtc_config;

  // Pion は ICE サーバーを返さないので、デフォルトで Google STUN を使用
  if (!config_.no_google_stun) {
    webrtc::PeerConnectionInterface::IceServer ice_server;
    ice_server.uri = "stun:stun.l.google.com:19302";
    rtc_config.servers.push_back(ice_server);
  }

  connection_ = manager_->CreateConnection(rtc_config, this);
  
  // 再接続時はトラックの初期化を遅延させない
  // 代わりに、PeerConnection作成前の長い待機時間で対応
  RTC_LOG(LS_INFO) << "Initializing audio tracks (retry_count=" << retry_count_ << ")...";
  manager_->InitTracks(connection_.get());
  
  RTC_LOG(LS_INFO) << "PeerConnection and tracks initialized";
}

void PionClient::Close() {
  if (ws_) {
    RTC_LOG(LS_INFO) << "Closing WebSocket connection";
    ws_->Close(std::bind(&PionClient::OnClose, shared_from_this(),
                         std::placeholders::_1));
  } else {
    RTC_LOG(LS_WARNING) << "WebSocket already null, triggering OnClose directly";
    // WebSocketが既にnullの場合は直接OnCloseを呼ぶ
    OnClose(boost::system::error_code());
  }
}

void PionClient::CloseAndReconnect() {
  RTC_LOG(LS_INFO) << "CloseAndReconnect: Starting clean reconnection";
  
  // watchdogを無効化
  watchdog_.Disable();
  
  // SFU再起動の場合は、retry_countをリセットして完全に新規接続として扱う
  retry_count_ = 0;
  
  // PeerConnectionを先にクローズ
  if (connection_) {
    auto pc = connection_->GetConnection();
    if (pc) {
      RTC_LOG(LS_INFO) << "Closing PeerConnection before WebSocket";
      pc->Close();
    }
    connection_ = nullptr;
  }
  
  if (ws_) {
    ws_->Close([this, self = shared_from_this()](boost::system::error_code) {
      RTC_LOG(LS_INFO) << "WebSocket closed, initiating reconnection";
      ws_ = nullptr;
      
      // SFU再起動後の再接続では、より長い待機時間が必要
      // ALSAデバイスとWebRTCスタックの完全なリセットを待つ
      boost::asio::post(ioc_, [this, self]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));  // 5秒待機
        RTC_LOG(LS_INFO) << "Starting reconnection after 5 second delay";
        Reset();
        Connect();
      });
    });
  } else {
    // WebSocketが既にない場合
    // 直接再接続
    boost::asio::post(ioc_, [this, self = shared_from_this()]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(5000));  // 5秒待機
      RTC_LOG(LS_INFO) << "Starting reconnection after 5 second delay";
      Reset();
      Connect();
    });
  }
}

void PionClient::Restart() {
  RTC_LOG(LS_INFO) << "Restarting PionClient at higher level";
  
  // 完全なクリーンアップ
  if (connection_) {
    // 既存のトラックを停止
    auto senders = connection_->GetConnection()->GetSenders();
    for (auto& sender : senders) {
      auto track = sender->track();
      if (track) {
        track->set_enabled(false);  // トラックを無効化
      }
    }
    
    auto pc = connection_->GetConnection();
    if (pc) {
      pc->Close();
    }
    connection_ = nullptr;
  }
  
  // watchdogを無効化
  watchdog_.Disable();
  
  // retry_countをリセット
  retry_count_ = 0;
  
  if (ws_) {
    ws_->Close([this, self = shared_from_this()](boost::system::error_code) {
      // WebSocket closeコールバック内で再接続
      ws_.reset();
      
      // より長い待機時間でオーディオデバイスの完全なリリースを待つ
      boost::asio::post(ioc_, [this, self]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));  // 3秒待機
        RTC_LOG(LS_INFO) << "Restarting after cleanup delay";
        Reset();
        Connect();
      });
    });
  } else {
    // WebSocketが既にない場合は直接再接続
    boost::asio::post(ioc_, [this, self = shared_from_this()]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(3000));  // 3秒待機
      RTC_LOG(LS_INFO) << "Restarting after cleanup delay";
      Reset();
      Connect();
    });
  }
}

void PionClient::OnClose(boost::system::error_code ec) {
  if (ec)
    MOMO_BOOST_ERROR(ec, "Close");
  
  // PeerConnectionを確実にクリーンアップ
  if (connection_) {
    auto pc = connection_->GetConnection();
    if (pc) {
      pc->Close();
    }
    connection_ = nullptr;
  }
  
  retry_count_ = 0;
  ReconnectAfter();
}

void PionClient::OnRead(boost::system::error_code ec,
                        std::size_t bytes_transferred,
                        std::string text) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << ": " << ec;

  // 書き込みのために読み込み処理がキャンセルされた時にこのエラーになるので、これはエラーとして扱わない
  if (ec == boost::asio::error::operation_aborted)
    return;

  // WebSocket が closed なエラーが返ってきた場合
  if (ec == boost::beast::websocket::error::closed || ec == boost::asio::error::eof) {
    RTC_LOG(LS_ERROR) << "WebSocket closed (EOF), exiting...";
    
    // 再接続せずに終了
    std::exit(1);
  }

  if (ec) {
    RTC_LOG(LS_ERROR) << "WebSocket read error: " << ec.message() << ", exiting...";
    
    // 再接続せずに終了
    std::exit(1);
  }

  RTC_LOG(LS_INFO) << __FUNCTION__ << ": text=" << text;

  boost::json::value json_message;
  try {
    json_message = boost::json::parse(text);
  } catch (const std::exception& e) {
    RTC_LOG(LS_ERROR) << "Failed to parse JSON: " << e.what();
    return;
  }

  const std::string event = json_message.at("event").as_string().c_str();
  const std::string data = json_message.at("data").as_string().c_str();

  if (event == "offer") {
    // 初回のofferでPeerConnectionを作成
    if (!connection_) {
      RTC_LOG(LS_INFO) << "First offer received, creating PeerConnection";
      CreatePeerConnection();
    }
    // サーバーから offer を受信したら answer を作成
    DoSendAnswer(data);
  } else if (event == "candidate") {
    // ICE candidate を受信
    boost::json::value candidate_json = boost::json::parse(data);
    std::string sdp_mid = candidate_json.at("sdpMid").as_string().c_str();
    int sdp_mlineindex = candidate_json.at("sdpMLineIndex").to_number<int>();
    std::string candidate = candidate_json.at("candidate").as_string().c_str();
    connection_->AddIceCandidate(sdp_mid, sdp_mlineindex, candidate);
  } else if (event == "ping") {
    // Pion SFUからのpingに応答
    boost::json::value pong_message = {
        {"event", "pong"},
        {"data", ""}
    };
    ws_->WriteText(boost::json::serialize(pong_message));
    RTC_LOG(LS_INFO) << "Responded to ping with pong";
  }

  // Pion SFUが15秒ごとにpingを送信するため、watchdogを20秒に設定
  watchdog_.Enable(20);  // SFUのpongTimeoutと一致
  DoRead();
}

void PionClient::DoSendAnswer(const std::string& offer_sdp) {
  // offer を JSON として解析
  boost::json::value offer_json = boost::json::parse(offer_sdp);
  const std::string sdp = offer_json.at("sdp").as_string().c_str();

  connection_->SetOffer(sdp, [this]() {
    boost::asio::post(ioc_, [this, self = shared_from_this()]() {
      // 再接続時は少し待機してから answer を作成
      // オーディオデバイスが安定するのを待つ
      if (retry_count_ > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // 1秒待機
      }
      
      connection_->CreateAnswer(
          [this](webrtc::SessionDescriptionInterface* desc) {
            std::string answer_sdp;
            desc->ToString(&answer_sdp);
            manager_->SetParameters();
            
            // answer を送信
            boost::json::value answer_json = {
                {"type", "answer"},
                {"sdp", answer_sdp}
            };
            boost::json::value message = {
                {"event", "answer"},
                {"data", boost::json::serialize(answer_json)}
            };
            ws_->WriteText(boost::json::serialize(message));
          });
    });
  });
}

void PionClient::DoSendCandidate(const std::string& candidate_json) {
  boost::json::value message = {
      {"event", "candidate"},
      {"data", candidate_json}
  };
  ws_->WriteText(boost::json::serialize(message));
}

void PionClient::SendCapability() {
  // Determine capability based on media modes
  bool audio_send = (config_.audio_mode == MomoArgs::MediaMode::SENDONLY || 
                     config_.audio_mode == MomoArgs::MediaMode::SENDRECV);
  bool audio_recv = (config_.audio_mode == MomoArgs::MediaMode::RECVONLY || 
                     config_.audio_mode == MomoArgs::MediaMode::SENDRECV);
  bool video_send = (config_.video_mode == MomoArgs::MediaMode::SENDONLY || 
                     config_.video_mode == MomoArgs::MediaMode::SENDRECV);
  bool video_recv = (config_.video_mode == MomoArgs::MediaMode::RECVONLY || 
                     config_.video_mode == MomoArgs::MediaMode::SENDRECV);
  
  // Explicitly handle NONE mode
  if (config_.audio_mode == MomoArgs::MediaMode::NONE) {
    audio_send = false;
    audio_recv = false;
  }
  if (config_.video_mode == MomoArgs::MediaMode::NONE) {
    video_send = false;
    video_recv = false;
  }
  
  boost::json::value capability_data = {
      {"audioSend", audio_send},
      {"audioRecv", audio_recv},
      {"videoSend", video_send},
      {"videoRecv", video_recv}
  };
  
  boost::json::value message = {
      {"event", "capability"},
      {"data", boost::json::serialize(capability_data)}
  };
  
  ws_->WriteText(boost::json::serialize(message));
  RTC_LOG(LS_INFO) << "Sent capability information: " 
                   << "audioSend=" << audio_send 
                   << " audioRecv=" << audio_recv
                   << " videoSend=" << video_send 
                   << " videoRecv=" << video_recv;
}

void PionClient::OnIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << " state:" << new_state;
  
  boost::asio::post(ioc_,
      std::bind(&PionClient::DoIceConnectionStateChange, shared_from_this(), new_state));
}

void PionClient::DoIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  if (destructed_) {
    return;
  }
  
  rtc_state_ = new_state;
  RTC_LOG(LS_INFO) << "ICE connection state changed to: " << new_state;
  
  switch (new_state) {
    case webrtc::PeerConnectionInterface::IceConnectionState::kIceConnectionConnected:
    case webrtc::PeerConnectionInterface::IceConnectionState::kIceConnectionCompleted:
      // 接続成功時
      RTC_LOG(LS_INFO) << "ICE connection established successfully";
      // watchdogを無効化（接続中は不要）
      watchdog_.Disable();
      break;
    case webrtc::PeerConnectionInterface::IceConnectionState::kIceConnectionFailed:
      RTC_LOG(LS_ERROR) << "ICE connection failed, exiting...";
      // 再接続せずに終了
      std::exit(1);
      break;
    case webrtc::PeerConnectionInterface::IceConnectionState::kIceConnectionDisconnected:
      RTC_LOG(LS_ERROR) << "ICE connection disconnected, exiting...";
      // 再接続せずに終了
      std::exit(1);
      break;
    default:
      break;
  }
}

void PionClient::OnIceCandidate(const std::string sdp_mid,
                                const int sdp_mlineindex,
                                const std::string sdp) {
  boost::json::value candidate = {
      {"sdpMid", sdp_mid},
      {"sdpMLineIndex", sdp_mlineindex},
      {"candidate", sdp}
  };
  
  boost::asio::post(ioc_,
      std::bind(&PionClient::DoSendCandidate, shared_from_this(), 
                boost::json::serialize(candidate)));
}