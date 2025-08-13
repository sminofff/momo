#include "pion_client.h"

// boost
#include <boost/beast/websocket/stream.hpp>
#include <boost/json.hpp>

// WebRTC
#include <api/peer_connection_interface.h>

#include "momo_version.h"
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
  connection_ = nullptr;
  rtc_state_ = webrtc::PeerConnectionInterface::IceConnectionState::
      kIceConnectionNew;
}

void PionClient::Connect() {
  RTC_LOG(LS_INFO) << __FUNCTION__;

  watchdog_.Enable(30);

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
  int interval = 5 * (retry_count_ + 1);
  if (interval > 30) {
    interval = 30;  // 最大30秒
  }
  
  RTC_LOG(LS_INFO) << __FUNCTION__ << " Reconnecting after " << interval << " seconds";
  retry_count_++;
  
  // watchdog タイマーを使って遅延後に再接続
  watchdog_.Enable(interval);
}

void PionClient::OnWatchdogExpired() {
  RTC_LOG(LS_WARNING) << __FUNCTION__;

  // Watchdog タイムアウト時の処理
  // 接続中の場合は再接続、再接続待機中の場合は再接続実行
  if (rtc_state_ == webrtc::PeerConnectionInterface::IceConnectionState::
                       kIceConnectionNew) {
    // 再接続タイマーとして使われた場合
    RTC_LOG(LS_INFO) << __FUNCTION__ << " Executing reconnection";
    Reset();
    Connect();
  } else {
    // 通常の watchdog タイムアウト
    RTC_LOG(LS_INFO) << __FUNCTION__ << " Watchdog timeout, scheduling reconnection";
    Reset();
    ReconnectAfter();
  }
}

void PionClient::OnConnect(boost::system::error_code ec) {
  if (ec) {
    RTC_LOG(LS_ERROR) << __FUNCTION__ << " error: " << ec;
    // 接続失敗時は再接続を遅延させる
    ReconnectAfter();
    return;
  }

  RTC_LOG(LS_INFO) << __FUNCTION__ << " connected";

  // SFU の場合、接続後はサーバーからの offer を待つだけ
  // ayame のような register 送信は不要
  retry_count_ = 0;
  
  // pion-sfu は自動的に offer を送ってくるため、
  // すぐに DoRead() で待機
  DoRead();
  
  // offer 受信のタイムアウト設定
  // pion-sfu は接続後すぐに offer を送信するはず
  watchdog_.Enable(15);  // 15秒以内に offer が来なければ再接続
}

std::shared_ptr<RTCConnection> PionClient::CreateRTCConnection() {
  webrtc::PeerConnectionInterface::RTCConfiguration rtc_config;
  
  // Google STUN サーバーをデフォルトで使用
  webrtc::PeerConnectionInterface::IceServer ice_server;
  ice_server.uri = "stun:stun.l.google.com:19302";
  rtc_config.servers.push_back(ice_server);

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
    // エラーが発生したら遅延を入れて再接続
    Reset();
    ReconnectAfter();
    return;
  }

  RTC_LOG(LS_INFO) << __FUNCTION__ << ": text=" << text;
  
  // メッセージを受信したら watchdog をリセット
  // pion-sfu は RFC 6455 ping フレームを送信するが、
  // データメッセージも送信するので、いずれかを受信したら接続は生きていると判断
  if (rtc_state_ == webrtc::PeerConnectionInterface::IceConnectionState::
                       kIceConnectionConnected) {
    watchdog_.Enable(45);  // 45秒（ping間隔10秒の4倍以上の余裕）
  }

  // JSON パース
  boost::json::value json_message = boost::json::parse(text);
  const std::string event = json_message.at("event").as_string().c_str();

  if (event == "offer") {
    // サーバーから offer を受信
    RTC_LOG(LS_INFO) << __FUNCTION__ << ": Received offer from server";
    
    // offer を受信したら watchdog をリセット
    // ping/pong が始まるまでの猶予を設定
    watchdog_.Enable(20);  // ping が来るまでの猶予時間
    
    // data フィールドの処理
    // SFU は初回接続時は文字列、再ネゴシエーション時はオブジェクトを送ることがある
    boost::json::value offer_json;
    const auto& data = json_message.at("data");
    
    if (data.is_string()) {
      // 初回接続: data は JSON 文字列
      offer_json = boost::json::parse(data.as_string());
    } else if (data.is_object()) {
      // 再ネゴシエーション: data は既に JSON オブジェクト
      offer_json = data;
    } else {
      RTC_LOG(LS_ERROR) << "Unexpected data type in offer message";
      return;
    }
    
    const std::string sdp = offer_json.at("sdp").as_string().c_str();
    
    // Offer SDP の playout-delay extension を確認
    if (sdp.find("playout-delay") != std::string::npos) {
      RTC_LOG(LS_INFO) << "Offer contains playout-delay extension";
      // extmap 行を抽出して表示
      size_t pos = 0;
      while ((pos = sdp.find("a=extmap:", pos)) != std::string::npos) {
        size_t end = sdp.find("\r\n", pos);
        if (end != std::string::npos) {
          RTC_LOG(LS_INFO) << "Offer extmap: " << sdp.substr(pos, end - pos);
        }
        pos = end;
      }
    } else {
      RTC_LOG(LS_INFO) << "Offer does NOT contain playout-delay extension";
    }
    
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
      return;  // 再ネゴシエーション処理完了
    }
    
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
              self->manager_->SetParameters();
              
              boost::asio::post(self->ioc_, [self, sdp]() {
                if (!self->connection_) {
                  return;
                }

                // Answer SDP の playout-delay extension を確認
                if (sdp.find("playout-delay") != std::string::npos) {
                  RTC_LOG(LS_INFO) << "Answer contains playout-delay extension";
                } else {
                  RTC_LOG(LS_INFO) << "Answer does NOT contain playout-delay extension";
                  // Answer の extmap 行を全て表示
                  size_t pos = 0;
                  int extmap_count = 0;
                  while ((pos = sdp.find("a=extmap:", pos)) != std::string::npos) {
                    size_t end = sdp.find("\r\n", pos);
                    if (end != std::string::npos) {
                      RTC_LOG(LS_INFO) << "Answer extmap: " << sdp.substr(pos, end - pos);
                      extmap_count++;
                    }
                    pos = end;
                  }
                  if (extmap_count == 0) {
                    RTC_LOG(LS_INFO) << "Answer contains NO extmap lines";
                  }
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
  } else if (event == "candidate") {
    // ICE candidate を受信
    // data フィールドの処理（文字列またはオブジェクトの両方に対応）
    boost::json::value candidate_json;
    const auto& data = json_message.at("data");
    
    if (data.is_string()) {
      // data は JSON 文字列
      candidate_json = boost::json::parse(data.as_string());
    } else if (data.is_object()) {
      // data は既に JSON オブジェクト
      candidate_json = data;
    } else {
      RTC_LOG(LS_ERROR) << "Unexpected data type in candidate message";
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
    RTC_LOG(LS_INFO) << "Received JSON ping from pion-sfu, sending pong";
    
    // watchdog をリセット
    if (rtc_state_ == webrtc::PeerConnectionInterface::IceConnectionState::
                         kIceConnectionConnected) {
      watchdog_.Enable(45);  // ping を受信したので watchdog をリセット
    }
    
    // pong を送信
    boost::json::value pong_message = {
        {"event", "pong"},
        {"data", ""}
    };
    ws_->WriteText(boost::json::serialize(pong_message));
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
  // ICE candidate を JSON 形式で作成
  // pion-sfu は JSON 文字列として data に入れることを期待
  boost::json::value candidate = {
      {"sdpMid", sdp_mid},
      {"sdpMLineIndex", sdp_mlineindex},
      {"candidate", sdp},
      {"usernameFragment", nullptr}  // pion-sfu 互換性のため
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
      retry_count_ = 0;
      // pion-sfu は RFC 6455 ping フレームを10秒間隔で送信
      // メッセージ受信時に watchdog をリセットするため、
      // 初期値は長めに設定
      watchdog_.Enable(45);  // 45秒
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