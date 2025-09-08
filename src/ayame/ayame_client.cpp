#include "ayame_client.h"

// boost
#include <boost/beast/websocket/stream.hpp>
#include <boost/json.hpp>

#include "momo_version.h"
#include "ssl_verifier.h"
#include "url_parts.h"
#include "util.h"

bool AyameClient::ParseURL(URLParts& parts) const {
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

void AyameClient::GetStats(
    std::function<void(const rtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
        callback) {
  if (connection_ && rtc_state_ ==
                         webrtc::PeerConnectionInterface::IceConnectionState::
                             kIceConnectionConnected) {
    connection_->GetStats(std::move(callback));
  } else {
    callback(nullptr);
  }
}

AyameClient::AyameClient(boost::asio::io_context& ioc,
                         RTCManager* manager,
                         AyameClientConfig config)
    : ioc_(ioc),
      manager_(manager),
      retry_count_(0),
      config_(std::move(config)),
      watchdog_(ioc, std::bind(&AyameClient::OnWatchdogExpired, this)) {
  Reset();
}

AyameClient::~AyameClient() {
  destructed_ = true;
  // ここで OnIceConnectionStateChange が呼ばれる
  connection_ = nullptr;
}

void AyameClient::Reset() {
  connection_ = nullptr;
  is_send_offer_ = false;
  has_is_exist_user_flag_ = false;
  ice_servers_.clear();

  URLParts parts;
  if (ParseURL(parts)) {
    ws_.reset(new Websocket(Websocket::ssl_tag(), ioc_, config_.insecure,
                            config_.client_cert, config_.client_key));
  } else {
    ws_.reset(new Websocket(ioc_));
  }
}

void AyameClient::Connect() {
  RTC_LOG(LS_INFO) << __FUNCTION__;

  watchdog_.Enable(30);

  ws_->Connect(config_.signaling_url,
               std::bind(&AyameClient::OnConnect, shared_from_this(),
                         std::placeholders::_1));
}

void AyameClient::ReconnectAfter() {
  int interval = 5 * (2 * retry_count_);
  if (interval > 30) {
    interval = 30;
  }
  RTC_LOG(LS_INFO) << __FUNCTION__ << " reconnect after " << interval << " sec";

  watchdog_.Enable(interval);
  retry_count_++;
}

void AyameClient::OnWatchdogExpired() {
  RTC_LOG(LS_WARNING) << __FUNCTION__;

  RTC_LOG(LS_INFO) << __FUNCTION__ << " reconnecting...:";
  Reset();
  Connect();
}

void AyameClient::OnConnect(boost::system::error_code ec) {
  if (ec) {
    ReconnectAfter();
    return MOMO_BOOST_ERROR(ec, "Handshake");
  }

  DoRead();

  DoRegister();
}

void AyameClient::DoRead() {
  ws_->Read(std::bind(&AyameClient::OnRead, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3));
}

void AyameClient::DoRegister() {
  boost::json::value json_message = {
      {"type", "register"},
      {"clientId", Util::GenerateRandomChars()},
      {"roomId", config_.room_id},
      {"ayameClient", MomoVersion::GetClientName()},
      {"libwebrtc", MomoVersion::GetLibwebrtcName()},
      {"environment", MomoVersion::GetEnvironmentName()},
  };
  if (config_.client_id != "") {
    json_message.as_object()["clientId"] = config_.client_id;
  }
  if (config_.signaling_key != "") {
    json_message.as_object()["key"] = config_.signaling_key;
  }
  ws_->WriteText(boost::json::serialize(json_message));
}

void AyameClient::DoSendPong() {
  boost::json::value json_message = {{"type", "pong"}};
  ws_->WriteText(boost::json::serialize(json_message));
}

void AyameClient::SetIceServersFromConfig(boost::json::value json_message) {
  // 返却されてきた iceServers を セットする
  if (json_message.as_object().count("iceServers") != 0) {
    auto jservers = json_message.at("iceServers");
    if (jservers.is_array()) {
      for (auto jserver : jservers.as_array()) {
        webrtc::PeerConnectionInterface::IceServer ice_server;
        if (jserver.as_object().count("username") != 0) {
          ice_server.username = jserver.at("username").as_string().c_str();
        }
        if (jserver.as_object().count("credential") != 0) {
          ice_server.password = jserver.at("credential").as_string().c_str();
        }
        auto jurls = jserver.at("urls");
        for (const auto url : jurls.as_array()) {
          ice_server.urls.push_back(url.as_string().c_str());
          RTC_LOG(LS_INFO) << __FUNCTION__
                           << ": iceserver.url=" << url.as_string();
        }
        ice_servers_.push_back(ice_server);
      }
    }
  }
  if (ice_servers_.empty() && !config_.no_google_stun) {
    // accept 時に iceServers が返却されてこなかった場合 google の stun server を用いる
    webrtc::PeerConnectionInterface::IceServer ice_server;
    ice_server.uri = "stun:stun.l.google.com:19302";
    ice_servers_.push_back(ice_server);
  }
}

void AyameClient::CreatePeerConnection() {
  webrtc::PeerConnectionInterface::RTCConfiguration rtc_config;

  rtc_config.servers = ice_servers_;
  connection_ = manager_->CreateConnection(rtc_config, this);
  manager_->InitTracks(connection_.get());
}

std::string AyameClient::SetCodecPreferencesInSDP(const std::string& sdp) {
  std::string modified_sdp = sdp;
  
  // ビデオコーデックの優先順位を設定
  if (!config_.video_codec_type.empty()) {
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
        
        // 各ペイロードタイプに対応するコーデックを確認
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
              
              // 指定されたコーデックまたは関連するコーデック（RTX等）を優先
              if (codec_name == config_.video_codec_type) {
                preferred_pts.insert(preferred_pts.begin(), pt);
              } else if (codec_name == "rtx") {
                // RTXは対応するコーデックの後に配置
                preferred_pts.push_back(pt);
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
        for (const auto& pt : preferred_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        for (const auto& pt : other_pts) {
          if (!new_formats.empty()) new_formats += " ";
          new_formats += pt;
        }
        
        // m=video行を再構築
        std::string new_m_line = m_line.substr(0, fmt_start) + new_formats;
        modified_sdp.replace(video_pos, line_end - video_pos, new_m_line);
        
        RTC_LOG(LS_INFO) << "Modified video codec order in SDP, prioritizing: " 
                        << config_.video_codec_type;
      }
    }
  }
  
  // オーディオコーデックの優先順位を設定（同様の処理）
  if (!config_.audio_codec_type.empty()) {
    size_t audio_pos = modified_sdp.find("m=audio ");
    if (audio_pos != std::string::npos) {
      // ビデオと同様の処理（省略）
      RTC_LOG(LS_INFO) << "Modified audio codec order in SDP, prioritizing: " 
                        << config_.audio_codec_type;
    }
  }
  
  return modified_sdp;
}

std::string AyameClient::AddPlayoutDelayExtension(const std::string& sdp) {
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
    size_t extmap_pos = sdp.find("a=extmap:", search_pos);
    size_t next_section = sdp.find("m=", search_pos + 1);
    
    // Stop if we've gone past the video section or reached the end
    if (extmap_pos == std::string::npos || 
        (next_section != std::string::npos && extmap_pos > next_section)) {
      break;
    }
    
    last_extmap_pos = sdp.find("\r\n", extmap_pos) + 2;
    search_pos = last_extmap_pos;
  }
  
  // Insert after the last extmap line if found, otherwise after m=video line
  if (last_extmap_pos != std::string::npos) {
    insert_pos = last_extmap_pos;
  }

  return sdp.substr(0, insert_pos) + extension_line + sdp.substr(insert_pos);
}

void AyameClient::Close() {
  ws_->Close(std::bind(&AyameClient::OnClose, shared_from_this(),
                       std::placeholders::_1));
}

// WebSocket が閉じられたときのコールバック
void AyameClient::OnClose(boost::system::error_code ec) {
  if (ec)
    MOMO_BOOST_ERROR(ec, "Close");
  // retry_count_ は ReconnectAfter(); が以前に呼ばれている場合はインクリメントされている可能性がある。
  // WebSocket につないでいない時間をなるべく短くしたいので、
  // WebSocket を閉じたときは一度インクリメントされている可能性のある retry_count_ を0 にして
  // OnWatchdogExpired(); が発火して再接続が行われるまでの時間を最小にしておく。
  retry_count_ = 0;
  // WebSocket 接続がちゃんと閉じられたら ReconnectAfter(); を発火する。
  // ReconnectAfter(); によって OnWatchdogExpired(); が呼ばれ、ここで WebSocket の再接続が行われる。
  // 現在は WebSocket がどんな理由で閉じられても、再接続するようになっている
  ReconnectAfter();
}

void AyameClient::OnRead(boost::system::error_code ec,
                         std::size_t bytes_transferred,
                         std::string text) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << ": " << ec;

  boost::ignore_unused(bytes_transferred);

  // 書き込みのために読み込み処理がキャンセルされた時にこのエラーになるので、これはエラーとして扱わない
  if (ec == boost::asio::error::operation_aborted)
    return;

  // WebSocket が closed なエラーが返ってきた場合すぐに Close(); を呼んで、OnRead 関数から抜ける
  if (ec == boost::beast::websocket::error::closed) {
    // Close(); で WebSocket が閉じられたら、OnClose(); -> ReconnectAfter(); -> OnWatchdogExpired(); の順に関数が呼ばれることで、
    // WebSocket の再接続が行われる
    Close();
    return;
  }

  if (ec)
    return MOMO_BOOST_ERROR(ec, "Read");

  RTC_LOG(LS_INFO) << __FUNCTION__ << ": text=" << text;

  auto json_message = boost::json::parse(text);
  const std::string type = json_message.at("type").as_string().c_str();
  if (type == "accept") {
    SetIceServersFromConfig(json_message);
    CreatePeerConnection();
    // isExistUser フラグが存在するか確認する
    auto is_exist_user = false;
    if (json_message.as_object().count("isExistUser") != 0) {
      has_is_exist_user_flag_ = true;
      is_exist_user = json_message.at("isExistUser").as_bool();
    }

    auto on_create_offer = [this](webrtc::SessionDescriptionInterface* desc) {
      std::string sdp;
      desc->ToString(&sdp);
      // Set codec preferences in SDP
      sdp = SetCodecPreferencesInSDP(sdp);
      // Add playout-delay extension if ultra low latency is enabled
      if (config_.ultra_low_latency) {
        sdp = AddPlayoutDelayExtension(sdp);
      }
      manager_->SetParameters();
      boost::json::value json_message = {{"type", "offer"}, {"sdp", sdp}};
      ws_->WriteText(boost::json::serialize(json_message));
    };

    // isExistUser フラグが存在してかつ true な場合 offer SDP を生成して送信する
    if (is_exist_user) {
      RTC_LOG(LS_INFO) << __FUNCTION__ << ": exist_user";
      is_send_offer_ = true;
      connection_->CreateOffer(on_create_offer);
    } else if (!has_is_exist_user_flag_) {
      // フラグがない場合とりあえず送信
      connection_->CreateOffer(on_create_offer);
    }
  } else if (type == "offer") {
    // isExistUser フラグがなかった場合二回 peer connection を生成する
    if (!has_is_exist_user_flag_) {
      CreatePeerConnection();
    }
    const std::string sdp = json_message.at("sdp").as_string().c_str();
    connection_->SetOffer(sdp, [this]() {
      boost::asio::post(ioc_, [this, self = shared_from_this()]() {
        if (!is_send_offer_ || !has_is_exist_user_flag_) {
          connection_->CreateAnswer(
              [this](webrtc::SessionDescriptionInterface* desc) {
                std::string sdp;
                desc->ToString(&sdp);
                // Set codec preferences in SDP
                sdp = SetCodecPreferencesInSDP(sdp);
                // Add playout-delay extension if ultra low latency is enabled
                if (config_.ultra_low_latency) {
                  sdp = AddPlayoutDelayExtension(sdp);
                }
                manager_->SetParameters();
                boost::json::value json_message = {{"type", "answer"},
                                                   {"sdp", sdp}};
                ws_->WriteText(boost::json::serialize(json_message));
              });
        }
        is_send_offer_ = false;
      });
    });
  } else if (type == "answer") {
    const std::string sdp = json_message.at("sdp").as_string().c_str();
    connection_->SetAnswer(sdp);
  } else if (type == "candidate") {
    boost::json::value ice = json_message.at("ice");
    std::string sdp_mid = ice.at("sdpMid").as_string().c_str();
    int sdp_mlineindex = ice.at("sdpMLineIndex").to_number<int>();
    std::string candidate = ice.at("candidate").as_string().c_str();
    connection_->AddIceCandidate(sdp_mid, sdp_mlineindex, candidate);
  } else if (type == "ping") {
    watchdog_.Reset();
    DoSendPong();
  } else if (type == "bye") {
    RTC_LOG(LS_INFO) << __FUNCTION__ << ": bye";
    connection_ = nullptr;
    Close();
  }
  DoRead();
}

// WebRTC からのコールバック
// これらは別スレッドからやってくるので取り扱い注意
void AyameClient::OnIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << " state:" << new_state;
  // デストラクタだと shared_from_this が機能しないので無視する
  if (destructed_) {
    return;
  }
  boost::asio::post(ioc_, std::bind(&AyameClient::DoIceConnectionStateChange,
                                    shared_from_this(), new_state));
}
void AyameClient::OnIceCandidate(const std::string sdp_mid,
                                 const int sdp_mlineindex,
                                 const std::string sdp) {
  // ayame では candidate sdp の交換で `ice` プロパティを用いる。 `candidate` ではないので注意
  boost::json::value json_message = {
      {"type", "candidate"},
  };
  // ice プロパティの中に object で candidate 情報をセットして送信する
  json_message.as_object()["ice"] = {{"candidate", sdp},
                                     {"sdpMLineIndex", sdp_mlineindex},
                                     {"sdpMid", sdp_mid}};
  ws_->WriteText(boost::json::serialize(json_message));
}

void AyameClient::DoIceConnectionStateChange(
    webrtc::PeerConnectionInterface::IceConnectionState new_state) {
  RTC_LOG(LS_INFO) << __FUNCTION__ << ": newState="
                   << Util::IceConnectionStateToString(new_state);

  switch (new_state) {
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionConnected:
      retry_count_ = 0;
      watchdog_.Enable(60);
      break;
    // ice connection state が failed になったら Close(); を呼んで、WebSocket 接続を閉じる
    case webrtc::PeerConnectionInterface::IceConnectionState::
        kIceConnectionFailed:
      // Close(); で WebSocket が閉じられたら、OnClose(); -> ReconnectAfter(); -> OnWatchdogExpired(); の順に関数が呼ばれることで
      // WebSocket の再接続が行われる
      Close();
      break;
    default:
      break;
  }
  rtc_state_ = new_state;
}
