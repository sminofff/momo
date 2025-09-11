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
  
  // bitrate設定のログ出力
  if (config_.video_bitrate > 0) {
    RTC_LOG(LS_INFO) << "Video bitrate limit: " << config_.video_bitrate << " kbps";
  }
  if (config_.audio_bitrate > 0) {
    RTC_LOG(LS_INFO) << "Audio bitrate limit: " << config_.audio_bitrate << " kbps";
  }

  // Codec negotiation is handled via SDP, no need to send codecInfo separately

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

void PionClient::SetBitrateParameters() {
  // bitrate が設定されていない場合は何もしない
  if (config_.video_bitrate == 0 && config_.audio_bitrate == 0) {
    return;
  }

  auto pc = connection_->GetConnection();
  if (pc == nullptr) {
    RTC_LOG(LS_ERROR) << "PeerConnection is null";
    return;
  }

  auto transceivers = pc->GetTransceivers();
  
  for (auto transceiver : transceivers) {
    // 送信方向を持たない transceiver はスキップ
    auto direction = transceiver->direction();
    if (direction != webrtc::RtpTransceiverDirection::kSendRecv &&
        direction != webrtc::RtpTransceiverDirection::kSendOnly) {
      continue;
    }

    auto sender = transceiver->sender();
    if (!sender) {
      continue;
    }

    auto parameters = sender->GetParameters();
    bool parameters_modified = false;

    // Video bitrate 設定
    if (transceiver->media_type() == cricket::MediaType::MEDIA_TYPE_VIDEO && 
        config_.video_bitrate > 0) {
      for (auto& encoding : parameters.encodings) {
        encoding.max_bitrate_bps = config_.video_bitrate * 1000;  // kbps to bps
        parameters_modified = true;
      }
      RTC_LOG(LS_INFO) << "Setting video max bitrate to " 
                       << config_.video_bitrate << " kbps";
    }
    // Audio bitrate 設定
    else if (transceiver->media_type() == cricket::MediaType::MEDIA_TYPE_AUDIO && 
             config_.audio_bitrate > 0) {
      for (auto& encoding : parameters.encodings) {
        encoding.max_bitrate_bps = config_.audio_bitrate * 1000;  // kbps to bps
        parameters_modified = true;
      }
      RTC_LOG(LS_INFO) << "Setting audio max bitrate to " 
                       << config_.audio_bitrate << " kbps";
    }

    if (parameters_modified) {
      auto error = sender->SetParameters(parameters);
      if (!error.ok()) {
        RTC_LOG(LS_ERROR) << "Failed to set bitrate parameters: " 
                          << error.message();
      }
    }
  }
}

std::string PionClient::SetCodecPreferencesInSDP(const std::string& sdp) {
  std::string modified_sdp = sdp;
  
  // ビデオコーデックの優先順位を設定
  if (!config_.video_codec_type.empty() && config_.video_codec_type != "ALL") {
    // m=video行を見つける
    size_t video_pos = modified_sdp.find("m=video ");
    if (video_pos != std::string::npos) {
      // m=video行の終わりを見つける
      size_t line_end = modified_sdp.find("\r\n", video_pos);
      if (line_end != std::string::npos) {
        // m=video行を取得
        std::string m_line = modified_sdp.substr(video_pos, line_end - video_pos);
        
        // ペイロードタイプを解析
        size_t port_end = m_line.find(" ", 8); // "m=video "の後
        size_t fmt_start = m_line.find(" ", port_end + 1) + 1; // RTP/AVPやRTP/SAVPF等の後
        std::string formats = m_line.substr(fmt_start);
        
        RTC_LOG(LS_INFO) << "Original video payload types: " << formats;
        
        // 各ペイロードタイプに対応するコーデックを確認
        std::vector<std::string> payload_types;
        std::vector<std::string> preferred_pts;
        std::vector<std::string> rtx_pts;  // RTXペイロードタイプを別管理
        std::vector<std::string> other_pts;
        std::string preferred_rtx;  // 優先コーデック用のRTX
        
        size_t pos = 0;
        while (pos < formats.length()) {
          size_t space = formats.find(" ", pos);
          std::string pt = (space == std::string::npos) ? 
                          formats.substr(pos) : 
                          formats.substr(pos, space - pos);
          payload_types.push_back(pt);
          pos = (space == std::string::npos) ? formats.length() : space + 1;
        }
        
        // 各ペイロードタイプのコーデックを確認
        for (const auto& pt : payload_types) {
          std::string rtpmap_line = "a=rtpmap:" + pt + " ";
          size_t rtpmap_pos = modified_sdp.find(rtpmap_line);
          if (rtpmap_pos != std::string::npos) {
            size_t codec_end = modified_sdp.find("/", rtpmap_pos);
            if (codec_end != std::string::npos) {
              std::string codec_name = modified_sdp.substr(
                  rtpmap_pos + rtpmap_line.length(),
                  codec_end - (rtpmap_pos + rtpmap_line.length()));
              
              RTC_LOG(LS_INFO) << "Payload type " << pt << " is codec: " << codec_name;
              
              // 指定されたコーデックを最優先
              if (codec_name == config_.video_codec_type) {
                preferred_pts.insert(preferred_pts.begin(), pt);
                
                // このコーデック用のRTXを探す
                for (const auto& rtx_pt : payload_types) {
                  std::string fmtp_line = "a=fmtp:" + rtx_pt + " apt=" + pt;
                  if (modified_sdp.find(fmtp_line) != std::string::npos) {
                    preferred_rtx = rtx_pt;
                    RTC_LOG(LS_INFO) << "Found RTX " << rtx_pt << " for preferred codec " << pt;
                    break;
                  }
                }
              } else if (codec_name == "rtx") {
                // RTXは後で適切な位置に配置
                rtx_pts.push_back(pt);
              } else {
                other_pts.push_back(pt);
              }
            }
          } else {
            other_pts.push_back(pt);
          }
        }
        
        // 優先ペイロードタイプリストを再構築
        std::string new_formats;
        
        // 1. 優先コーデックを最初に
        for (const auto& pt : preferred_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        
        // 2. 優先コーデック用のRTXを直後に
        if (!preferred_rtx.empty()) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += preferred_rtx;
          // rtx_ptsから除外
          rtx_pts.erase(std::remove(rtx_pts.begin(), rtx_pts.end(), preferred_rtx), rtx_pts.end());
        }
        
        // 3. その他のコーデック
        for (const auto& pt : other_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        
        // 4. 残りのRTX
        for (const auto& pt : rtx_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        
        RTC_LOG(LS_INFO) << "Modified video payload types: " << new_formats;
        
        // m=video行を再構築
        std::string new_m_line = m_line.substr(0, fmt_start) + new_formats;
        modified_sdp.replace(video_pos, line_end - video_pos, new_m_line);
        
        RTC_LOG(LS_INFO) << "Successfully modified video codec order in SDP, prioritizing: " 
                        << config_.video_codec_type;
      }
    }
  }
  
  // オーディオコーデックの優先順位を設定
  if (!config_.audio_codec_type.empty()) {
    size_t audio_pos = modified_sdp.find("m=audio ");
    if (audio_pos != std::string::npos) {
      size_t line_end = modified_sdp.find("\r\n", audio_pos);
      if (line_end != std::string::npos) {
        std::string m_line = modified_sdp.substr(audio_pos, line_end - audio_pos);
        
        size_t port_end = m_line.find(" ", 8);
        size_t fmt_start = m_line.find(" ", port_end + 1) + 1;
        std::string formats = m_line.substr(fmt_start);
        
        std::vector<std::string> payload_types;
        std::vector<std::string> preferred_pts;
        std::vector<std::string> other_pts;
        
        size_t pos = 0;
        while (pos < formats.length()) {
          size_t space = formats.find(" ", pos);
          std::string pt = (space == std::string::npos) ? 
                          formats.substr(pos) : 
                          formats.substr(pos, space - pos);
          payload_types.push_back(pt);
          pos = (space == std::string::npos) ? formats.length() : space + 1;
        }
        
        for (const auto& pt : payload_types) {
          std::string rtpmap_line = "a=rtpmap:" + pt + " ";
          size_t rtpmap_pos = modified_sdp.find(rtpmap_line);
          if (rtpmap_pos != std::string::npos) {
            size_t codec_end = modified_sdp.find("/", rtpmap_pos);
            if (codec_end != std::string::npos) {
              std::string codec_name = modified_sdp.substr(
                  rtpmap_pos + rtpmap_line.length(),
                  codec_end - (rtpmap_pos + rtpmap_line.length()));
              
              if (codec_name == config_.audio_codec_type) {
                preferred_pts.insert(preferred_pts.begin(), pt);
              } else if (codec_name == "telephone-event") {
                preferred_pts.push_back(pt);
              } else {
                other_pts.push_back(pt);
              }
            }
          } else {
            other_pts.push_back(pt);
          }
        }
        
        std::string new_formats;
        for (const auto& pt : preferred_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        for (const auto& pt : other_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        
        std::string new_m_line = m_line.substr(0, fmt_start) + new_formats;
        modified_sdp.replace(audio_pos, line_end - audio_pos, new_m_line);
        
        RTC_LOG(LS_INFO) << "Modified audio codec order in SDP, prioritizing: " 
                        << config_.audio_codec_type;
      }
    }
  }
  
  return modified_sdp;
}

std::string PionClient::AddPlayoutDelayExtension(const std::string& sdp) {
  // Check if playout-delay extension already exists
  if (sdp.find("http://www.webrtc.org/experiments/rtp-hdrext/playout-delay") != 
      std::string::npos) {
    return sdp;  // Already exists
  }

  // Find available extension ID (typically 12-14)
  int extension_id = 12;
  for (int id = 12; id <= 14; ++id) {
    if (sdp.find("a=extmap:" + std::to_string(id) + " ") == std::string::npos) {
      extension_id = id;
      break;
    }
  }

  std::string extension_line = "a=extmap:" + std::to_string(extension_id) +
                               " http://www.webrtc.org/experiments/rtp-hdrext/playout-delay\r\n";

  // Find video section to insert the extension
  size_t video_pos = sdp.find("m=video");
  if (video_pos == std::string::npos) {
    return sdp;  // No video section found
  }

  // Find the right position to insert (after other extmap lines if they exist)
  size_t insert_pos = sdp.find("\r\n", video_pos) + 2;  // After m=video line
  
  // Look for existing extmap lines in the video section
  size_t search_pos = insert_pos;
  size_t last_extmap_pos = std::string::npos;
  while (true) {
    size_t next_line_end = sdp.find("\r\n", search_pos);
    if (next_line_end == std::string::npos) break;
    
    std::string line = sdp.substr(search_pos, next_line_end - search_pos);
    if (line.find("a=extmap:") == 0) {
      last_extmap_pos = next_line_end + 2;
    } else if (line.find("m=") == 0 && search_pos != video_pos) {
      // Reached next media section
      break;
    }
    search_pos = next_line_end + 2;
  }

  // Insert after the last extmap line if found, otherwise after m=video line
  if (last_extmap_pos != std::string::npos) {
    insert_pos = last_extmap_pos;
  }

  // Insert the extension line
  std::string modified_sdp = sdp.substr(0, insert_pos) + extension_line + 
                            sdp.substr(insert_pos);
  
  RTC_LOG(LS_INFO) << "Added playout-delay extension with ID " << extension_id;
  return modified_sdp;
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
    
    std::string sdp = offer_json.at("sdp").as_string().c_str();
    
    // Offer SDPのコーデック優先順位も修正
    sdp = SetCodecPreferencesInSDP(sdp);
    RTC_LOG(LS_INFO) << "Modified received offer SDP to prioritize configured codecs";
    
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
                
                // Set codec preferences in SDP
                sdp = self->SetCodecPreferencesInSDP(sdp);
                
                // Add playout-delay extension if ultra low latency is enabled
                if (self->config_.ultra_low_latency) {
                  sdp = self->AddPlayoutDelayExtension(sdp);
                }
                
                // Bitrate パラメータを設定（再ネゴシエーション時）
                self->SetBitrateParameters();
                
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

          // トラックを初期化
          self->manager_->InitTracks(self->connection_.get());

          // answer を生成
          self->connection_->CreateAnswer(
            [self](webrtc::SessionDescriptionInterface* desc) {
              std::string sdp;
              desc->ToString(&sdp);
              
              // Set codec preferences in SDP
              sdp = self->SetCodecPreferencesInSDP(sdp);
              
              // Add playout-delay extension if ultra low latency is enabled
              if (self->config_.ultra_low_latency) {
                sdp = self->AddPlayoutDelayExtension(sdp);
              }
              
              self->manager_->SetParameters();
              
              // Bitrate パラメータを設定
              self->SetBitrateParameters();
              
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
    std::function<void(const rtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
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
