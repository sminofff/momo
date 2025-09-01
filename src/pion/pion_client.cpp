#include "pion_client.h"

// boost
#include <boost/beast/websocket/stream.hpp>
#include <boost/json.hpp>

// WebRTC
#include <api/peer_connection_interface.h>
#include <api/rtp_transceiver_interface.h>

#include "momo_version.h"
#include "rtc/rtc_connection.h"
#include "ssl_verifier.h"
#include "url_parts.h"
#include "util.h"

// ログ出力のためのマクロ
#include <rtc_base/logging.h>

namespace {
// 映像補助コーデック
const std::vector<std::string> kVideoAuxiliaryCodecs = {"RTX", "RED", "ULPFEC",
                                                        "FLEXFEC-03"};

// 音声補助コーデック
const std::vector<std::string> kAudioAuxiliaryCodecs = {"TELEPHONE-EVENT",
                                                        "CN"};

// コーデックが補助コーデックかどうかを判定
bool IsAuxiliaryCodec(const std::string& codec_name,
                      webrtc::MediaType media_type) {
  const auto& auxiliary_codecs = media_type == webrtc::MediaType::VIDEO
                                     ? kVideoAuxiliaryCodecs
                                     : kAudioAuxiliaryCodecs;
  return std::find(auxiliary_codecs.begin(), auxiliary_codecs.end(),
                   codec_name) != auxiliary_codecs.end();
}
}  // namespace

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
  // ここで OnIceConnectionStateChange が呼ばれる
  connection_ = nullptr;
}

void PionClient::Reset() {
  watchdog_.Disable();
  connection_ = nullptr;
  rtc_state_ = webrtc::PeerConnectionInterface::IceConnectionState::
      kIceConnectionNew;
  ice_connected_ = false;  // Reset ICE connection flag
}

void PionClient::Connect() {
  RTC_LOG(LS_INFO) << __FUNCTION__;

  watchdog_.Enable(kWatchdogInitialTimeout);

  // URL からプロトコルを判定
  URLParts parts;
  if (!URLParts::Parse(config_.signaling_url, parts)) {
    RTC_LOG(LS_ERROR) << "Failed to parse URL: " << config_.signaling_url;
    return;
  }
  
  bool ssl = parts.scheme == "wss";
  if (ssl) {
    ws_.reset(new Websocket(Websocket::ssl_tag(), ioc_, config_.insecure, "", ""));
  } else {
    ws_.reset(new Websocket(ioc_));
  }
  
  ws_->Connect(config_.signaling_url,
               std::bind(&PionClient::OnConnect, shared_from_this(),
                         std::placeholders::_1));
}

void PionClient::ReconnectAfter() {
  // 再接続の間隔を設定（指数バックオフ）
  int interval = kReconnectIntervalBase * (retry_count_ + 1);
  if (interval > kReconnectIntervalMax) {
    interval = kReconnectIntervalMax;
  }
  
  RTC_LOG(LS_INFO) << __FUNCTION__ << " Reconnecting after " << interval << " seconds";
  retry_count_++;
  
  // watchdog タイマーを使って遅延後に再接続
  watchdog_.Enable(interval);
}

void PionClient::OnWatchdogExpired() {
  RTC_LOG(LS_WARNING) << __FUNCTION__;

  // Watchdog タイムアウト時は再接続を実行
  RTC_LOG(LS_INFO) << __FUNCTION__ << " reconnecting...";
  Reset();
  Connect();
}

void PionClient::OnConnect(boost::system::error_code ec) {
  if (ec) {
    RTC_LOG(LS_ERROR) << __FUNCTION__ << " error: " << ec;
    ReconnectAfter();
    return;
  }

  RTC_LOG(LS_INFO) << __FUNCTION__ << " connected";

  // Send codec info if specified
  if (!config_.video_codec_type.empty()) {
    boost::json::object codec_info = {
        {"videoCodec", config_.video_codec_type}
    };
    boost::json::object msg = {
        {"event", "codecInfo"},
        {"data", boost::json::serialize(codec_info)}
    };
    ws_->WriteText(boost::json::serialize(msg));
    RTC_LOG(LS_INFO) << "Sent codec info: " << config_.video_codec_type;
  }

  retry_count_ = 0;
  DoRead();
  watchdog_.Enable(kWatchdogOfferTimeout);
}

std::shared_ptr<RTCConnection> PionClient::CreateRTCConnection() {
  webrtc::PeerConnectionInterface::RTCConfiguration rtc_config;
  
  // Google STUN サーバーをデフォルトで使用
  webrtc::PeerConnectionInterface::IceServer ice_server;
  ice_server.uri = "stun:stun.l.google.com:19302";
  rtc_config.servers.push_back(ice_server);
  
  // RTCP 設定を追加
  // RTCP reduced-size mode を有効化（Pion SFU のデフォルト）
  rtc_config.rtcp_mux_policy = webrtc::PeerConnectionInterface::kRtcpMuxPolicyRequire;
  
  // Bundle policy を設定（全メディアストリームを単一のトランスポートにバンドル）
  rtc_config.bundle_policy = webrtc::PeerConnectionInterface::kBundlePolicyMaxBundle;
  
  // ICE candidate pool size を設定
  rtc_config.ice_candidate_pool_size = 2;
  
  // Continual gathering policy を設定（ICE 候補の継続的な収集）
  rtc_config.continual_gathering_policy = 
      webrtc::PeerConnectionInterface::ContinualGatheringPolicy::GATHER_CONTINUALLY;

  return manager_->CreateConnection(rtc_config, this);
}

void PionClient::SetCodecPreferences() {
  if (config_.video_codec_type.empty() && config_.audio_codec_type.empty()) {
    return;
  }

  auto pc = connection_->GetConnection();
  if (pc == nullptr) {
    RTC_LOG(LS_ERROR) << "PeerConnection is null";
    return;
  }

  // PeerConnectionFactory から GetRtpSenderCapabilities を使ってコーデック一覧を取得
  auto factory = manager_->GetFactory();
  if (factory == nullptr) {
    RTC_LOG(LS_ERROR) << "PeerConnectionFactory is null";
    return;
  }

  auto transceivers = pc->GetTransceivers();

  // Transceiver が存在しない場合はエラーを出して何もしない
  if (transceivers.empty()) {
    RTC_LOG(LS_ERROR)
        << "No transceivers found when trying to set codec preferences";
    return;
  }

  for (auto transceiver : transceivers) {
    // VIDEO でも AUDIO でもない Transceiver はスキップ
    if (transceiver->media_type() != webrtc::MediaType::VIDEO &&
        transceiver->media_type() != webrtc::MediaType::AUDIO) {
      continue;
    }

    // 指定されたコーデックが空の場合はスキップ
    if (transceiver->media_type() == webrtc::MediaType::VIDEO &&
            config_.video_codec_type.empty() ||
        transceiver->media_type() == webrtc::MediaType::AUDIO &&
            config_.audio_codec_type.empty()) {
      continue;
    }

    std::string target_codec =
        transceiver->media_type() == webrtc::MediaType::VIDEO
            ? config_.video_codec_type
            : config_.audio_codec_type;

    // PeerConnectionFactory から送信側と受信側の両方の capabilities を取得
    webrtc::RtpCapabilities sender_capabilities =
        factory->GetRtpSenderCapabilities(transceiver->media_type());
    webrtc::RtpCapabilities receiver_capabilities =
        factory->GetRtpReceiverCapabilities(transceiver->media_type());

    // 送信側と受信側の両方でサポートされているコーデックを見つける
    std::vector<webrtc::RtpCodecCapability> common_codecs;
    for (const auto& sender_codec : sender_capabilities.codecs) {
      for (const auto& receiver_codec : receiver_capabilities.codecs) {
        // MIMEタイプが一致する場合に共通コーデックとみなす
        if (sender_codec.mime_type() == receiver_codec.mime_type()) {
          common_codecs.push_back(sender_codec);
          break;
        }
      }
    }

    // 共通コーデックが空の場合はスキップ
    if (common_codecs.empty()) {
      RTC_LOG(LS_WARNING)
          << "No common codec capabilities available for transceiver";
      continue;
    }

    RTC_LOG(LS_INFO) << "Found " << common_codecs.size()
                     << " common codecs for "
                     << webrtc::MediaTypeToString(transceiver->media_type());

    // コーデックのフィルタリング
    std::vector<webrtc::RtpCodecCapability> filtered_codecs;
    for (const auto& codec : common_codecs) {
      // 指定されたコーデックまたは補助的なコーデックは残す
      if (codec.name == target_codec ||
          IsAuxiliaryCodec(codec.name, transceiver->media_type())) {
        filtered_codecs.push_back(codec);
      }
    }

    // 指定されたコーデックが見つからなかった場合はエラー
    if (filtered_codecs.empty()) {
      RTC_LOG(LS_ERROR) << "Specified codec '" << target_codec << "' for "
                        << webrtc::MediaTypeToString(transceiver->media_type())
                        << " is not available. Available codecs:";
      for (const auto& codec : common_codecs) {
        RTC_LOG(LS_ERROR) << "  - " << codec.name;
      }
      continue;
    }

    auto error = transceiver->SetCodecPreferences(filtered_codecs);
    if (!error.ok()) {
      RTC_LOG(LS_ERROR) << "Failed to set codec preferences: "
                        << error.message();
      continue;
    }

    RTC_LOG(LS_INFO) << "Successfully set codec preferences for "
                     << webrtc::MediaTypeToString(transceiver->media_type())
                     << " to " << target_codec;
  }
}

void PionClient::Close() {
  ws_->Close(std::bind(&PionClient::OnClose, shared_from_this(),
                       std::placeholders::_1));
}

void PionClient::OnClose(boost::system::error_code ec) {
  if (ec)
    RTC_LOG(LS_ERROR) << __FUNCTION__ << " error: " << ec;

  RTC_LOG(LS_INFO) << __FUNCTION__;

  watchdog_.Disable();
  
  // 明示的に Close() が呼ばれた場合は再接続しない
  if (destructed_) {
    return;
  }
  
  // WebSocket が切断されたら遅延を入れて再接続
  // ユーザーの要望: "wsが切断されたらresetしてwsを接続しなおしてofferから始める"
  RTC_LOG(LS_INFO) << "WebSocket disconnected, scheduling reconnection";
  Reset();
  ReconnectAfter();
}

void PionClient::DoRead() {
  ws_->Read(std::bind(&PionClient::OnRead, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3));
}

void PionClient::OnRead(boost::system::error_code ec,
                        std::size_t bytes_transferred,
                        std::string text) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << ": " << ec;

  boost::ignore_unused(bytes_transferred);

  // 接続が切断された
  if (ec == boost::asio::error::operation_aborted)
    return;

  // WebSocket が closed なエラーが返ってきた場合
  if (ec == boost::beast::websocket::error::closed) {
    RTC_LOG(LS_INFO) << "WebSocket closed, scheduling reconnection";
    Reset();
    ReconnectAfter();
    return;
  }

  if (ec) {
    RTC_LOG(LS_ERROR) << __FUNCTION__ << " error: " << ec;
    Reset();
    ReconnectAfter();
    return;
  }

  RTC_LOG(LS_INFO) << __FUNCTION__ << ": text=" << text;
  
  // 詳細ログ: 受信したメッセージを VERBOSE レベルで出力
  RTC_LOG(LS_VERBOSE) << "Received WebSocket message: " << text;
  
  // メッセージを受信したら watchdog をリセット
  // pion-sfu は RFC 6455 ping フレームを送信するが、
  // データメッセージも送信するので、いずれかを受信したら接続は生きていると判断
  watchdog_.Enable(kWatchdogKeepaliveTimeout);

  // JSON パース
  boost::json::value json_message = boost::json::parse(text);
  const std::string event = json_message.at("event").as_string().c_str();
  
  // 受信したイベントをログ出力（デバッグ用）
  RTC_LOG(LS_WARNING) << "Received event: " << event;

  if (event == "offer") {
    // サーバーから offer を受信
    RTC_LOG(LS_INFO) << __FUNCTION__ << ": Received offer from server";
    
    // data フィールドの処理
    boost::json::value offer_json;
    const auto& data = json_message.at("data");
    
    if (data.is_string()) {
      offer_json = boost::json::parse(data.as_string());
    } else if (data.is_object()) {
      offer_json = data;
    } else {
      RTC_LOG(LS_ERROR) << "Unexpected data type in offer message";
      // DoRead() を呼ぶために return せずに処理を続ける
      DoRead();
      return;
    }
    
    const std::string sdp = offer_json.at("sdp").as_string().c_str();
    
    // 既存の接続がある場合は再ネゴシエーション
    if (connection_) {
      RTC_LOG(LS_INFO) << "Existing connection found, performing renegotiation";
      
      // 再ネゴシエーション: 既存の接続を使用して新しい offer を設定
      connection_->SetOffer(sdp, [self = shared_from_this()]() {
        boost::asio::post(self->ioc_, [self]() {
          if (!self->connection_) {
            return;
          }
          
          // answer を生成（再ネゴシエーション用）
          self->connection_->CreateAnswer(
              [self](webrtc::SessionDescriptionInterface* desc) {
                std::string sdp;
                desc->ToString(&sdp);
                
                // answer を boost::asio コンテキストで送信
                boost::asio::post(self->ioc_, [self, sdp]() {
                  // answer を送信（再ネゴシエーション用）
                  boost::json::value answer_obj = {
                      {"type", "answer"},
                      {"sdp", sdp}};
                  boost::json::value response = {
                      {"event", "answer"},
                      {"data", boost::json::serialize(answer_obj)}};
                  self->ws_->WriteText(boost::json::serialize(response));
                });
              });
        });
      });
      // DoRead() を呼ぶために return しない
    } else {
      // 新規接続の場合: RTCConnection を作成
      RTC_LOG(LS_INFO) << "Creating new RTCConnection for initial offer";
      connection_ = CreateRTCConnection();
      
      // offer を設定して answer を生成
      connection_->SetOffer(sdp, [self = shared_from_this()]() {
        boost::asio::post(self->ioc_, [self]() {
          if (!self->connection_) {
            return;
          }

          // トラックを初期化（pion は sendrecv なので direction は指定しない）
          self->manager_->InitTracks(self->connection_.get(), std::nullopt);

          // InitTracks で Transceiver が作成された後に SetCodecPreferences を呼ぶ
          self->SetCodecPreferences();

          // answer を生成
          self->connection_->CreateAnswer(
            [self](webrtc::SessionDescriptionInterface* desc) {
              std::string sdp;
              desc->ToString(&sdp);
              
              self->manager_->SetParameters();
              
              boost::asio::post(self->ioc_, [self, sdp]() {
                if (!self->connection_) {
                  return;
                }

                // pion-sfu 形式で answer を送信
                // data フィールドには JSON オブジェクトを文字列化して入れる
                boost::json::value answer_obj = {
                    {"type", "answer"},
                    {"sdp", sdp}
                };
                boost::json::value response = {
                    {"event", "answer"},
                    {"data", boost::json::serialize(answer_obj)}
                };
                self->ws_->WriteText(boost::json::serialize(response));
              });
            });
        });
      });
    }
  } else if (event == "candidate") {
    // SFU optimization: Ignore candidates after ICE connection is established
    if (ice_connected_.load()) {
      RTC_LOG(LS_INFO) << "ICE already connected, ignoring received candidate";
      DoRead();
      return;
    }
    
    // ICE candidate を受信
    boost::json::value candidate_json;
    const auto& data = json_message.at("data");
    
    if (data.is_string()) {
      candidate_json = boost::json::parse(data.as_string());
    } else if (data.is_object()) {
      candidate_json = data;
    } else {
      RTC_LOG(LS_ERROR) << "Unexpected data type in candidate message";
      // DoRead() を呼ぶために return せずに処理を続ける
      DoRead();
      return;
    }
    
    // pion-sfu は sdpMid が空文字列を送ることがあるので、
    // 空の場合は sdpMLineIndex から適切な mid を決定する
    std::string sdp_mid;
    auto mid_it = candidate_json.as_object().find("sdpMid");
    if (mid_it != candidate_json.as_object().end() && 
        !mid_it->value().is_null() && 
        !mid_it->value().as_string().empty()) {
      sdp_mid = mid_it->value().as_string().c_str();
    }
    
    const int sdp_mlineindex = candidate_json.at("sdpMLineIndex").to_number<int>();
    const std::string candidate = candidate_json.at("candidate").as_string().c_str();
    
    // connection_ が存在する場合のみ ICE candidate を追加
    if (connection_) {
      // sdp_mid が空の場合は、sdpMLineIndex に基づいて mid を設定
      // 0 = audio (mid="0"), 1 = video (mid="1")
      if (sdp_mid.empty()) {
        sdp_mid = std::to_string(sdp_mlineindex);
      }
      connection_->AddIceCandidate(sdp_mid, sdp_mlineindex, candidate);
    } else {
      RTC_LOG(LS_WARNING) << "Received ICE candidate before connection is ready";
    }
  } else if (event == "ping") {
    // JSON ping を受信したら pong を返す
    RTC_LOG(LS_INFO) << "Received ping from pion-sfu";
    
    // pong を送信
    boost::json::value pong_message = {
        {"event", "pong"},
        {"data", ""}
    };
    RTC_LOG(LS_INFO) << "Sending pong to pion-sfu";
    ws_->WriteText(boost::json::serialize(pong_message));
  } else if (event == "track_removed") {
    // トラック削除通知を受信
    RTC_LOG(LS_INFO) << "Received track_removed notification from pion-sfu";
    
    // data フィールドから削除されたトラックIDを取得
    try {
      boost::json::value track_removal_json;
      const auto& data = json_message.at("data");
      
      if (data.is_string()) {
        track_removal_json = boost::json::parse(data.as_string());
      } else {
        track_removal_json = data;
      }
      
      const auto& track_ids = track_removal_json.at("trackIds").as_array();
      
      for (const auto& track_id : track_ids) {
        std::string removed_track_id = track_id.as_string().c_str();
        RTC_LOG(LS_INFO) << "Track removed: " << removed_track_id;
        
        // RTCConnection に通知してトラックを削除
        if (connection_) {
          connection_->RemoveRemoteTrack(removed_track_id);
        }
      }
    } catch (const std::exception& e) {
      RTC_LOG(LS_ERROR) << "Failed to parse track_removed message: " << e.what();
    }
  } else {
    // 未処理のイベントをログ出力
    RTC_LOG(LS_WARNING) << "Unhandled event received: " << event;
    RTC_LOG(LS_WARNING) << "Full message: " << text;
  }

  DoRead();
}

// WebRTC からのコールバック
// これらは別スレッドからやってくるので取り扱い注意
void PionClient::OnIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << " state:" << new_state;
  
  // デストラクタだと shared_from_this が機能しないので無視する
  if (destructed_)
    return;

  boost::asio::post(ioc_, std::bind(&PionClient::DoIceConnectionStateChange,
                                    shared_from_this(), new_state));
}

void PionClient::OnIceCandidate(const std::string sdp_mid,
                                const int sdp_mlineindex,
                                const std::string sdp) {
  // SFU optimization: Don't send candidates after ICE connection is established
  if (ice_connected_.load()) {
    RTC_LOG(LS_INFO) << "ICE already connected, ignoring new candidate";
    return;
  }
  
  // ICE candidate を JSON 形式で作成
  // pion-sfu は JSON 文字列として data に入れることを期待
  boost::json::value candidate = {
      {"sdpMid", sdp_mid},
      {"sdpMLineIndex", sdp_mlineindex},
      {"candidate", sdp}
  };
  
  // pion-sfu 形式で送信
  boost::json::value json_message = {
      {"event", "candidate"},
      {"data", boost::json::serialize(candidate)}
  };
  
  ws_->WriteText(boost::json::serialize(json_message));
}

void PionClient::DoIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << " state:" << new_state;
  
  switch (new_state) {
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionConnected:
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionCompleted:
      retry_count_ = 0;
      // SFU optimization: Mark ICE as connected to stop sending candidates
      if (!ice_connected_.load()) {
        ice_connected_ = true;
        RTC_LOG(LS_INFO) << "ICE connected - stopping candidate generation";
      }
      // pion-sfu は RFC 6455 ping フレームを10秒間隔で送信
      // メッセージ受信時に watchdog をリセットするため、
      // 初期値は長めに設定
      watchdog_.Enable(kWatchdogIceConnected);
      break;
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionDisconnected:
      // 一時的な切断の場合は遅延を入れて再接続
      RTC_LOG(LS_WARNING) << "ICE disconnected, scheduling reconnection";
      Reset();
      ReconnectAfter();
      break;
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionFailed:
      // 失敗時も遅延を入れて再接続
      RTC_LOG(LS_ERROR) << "ICE connection failed, scheduling reconnection";
      Reset();
      ReconnectAfter();
      break;
    default:
      break;
  }
  rtc_state_ = new_state;
}

void PionClient::GetStats(
    std::function<void(const webrtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
        callback) {
  if (connection_ && (rtc_state_ ==
                         webrtc::PeerConnectionInterface::IceConnectionState::
                             kIceConnectionConnected ||
                       rtc_state_ ==
                         webrtc::PeerConnectionInterface::IceConnectionState::
                             kIceConnectionCompleted)) {
    connection_->GetStats(std::move(callback));
  } else {
    callback(nullptr);
  }
}