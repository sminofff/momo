#ifndef PION_CLIENT_H_
#define PION_CLIENT_H_

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>

// Boost
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/json.hpp>

#include "metrics/stats_collector.h"
#include "rtc/rtc_manager.h"
#include "rtc/rtc_message_sender.h"
#include "url_parts.h"
#include "watchdog.h"
#include "websocket.h"
#include "../momo_args.h"

struct PionClientConfig {
  bool insecure = false;
  bool no_google_stun = false;
  std::string client_cert;
  std::string client_key;

  std::string signaling_url;
  
  // Media mode settings
  MomoArgs::MediaMode audio_mode = MomoArgs::MediaMode::SENDRECV;
  MomoArgs::MediaMode video_mode = MomoArgs::MediaMode::SENDRECV;
};

class PionClient : public std::enable_shared_from_this<PionClient>,
                   public RTCMessageSender,
                   public StatsCollector {
  PionClient(boost::asio::io_context& ioc,
             RTCManager* manager,
             PionClientConfig config);

 public:
  static std::shared_ptr<PionClient> Create(boost::asio::io_context& ioc,
                                            RTCManager* manager,
                                            PionClientConfig config) {
    return std::shared_ptr<PionClient>(
        new PionClient(ioc, manager, std::move(config)));
  }
  ~PionClient();

  void Reset();
  void Connect();
  void Close();
  void Restart();  // 上位レベルでの再起動

  void GetStats(std::function<
                void(const rtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
                    callback) override;

 private:
  void ReconnectAfter();
  void OnWatchdogExpired();
  void CloseAndReconnect();
  bool ParseURL(URLParts& parts) const;

 private:
  void DoRead();
  void CreatePeerConnection();
  void CreatePeerConnectionInternal();
  void DoSendAnswer(const std::string& offer_sdp);
  void DoSendCandidate(const std::string& candidate_json);
  void SendCapability();

 private:
  void OnConnect(boost::system::error_code ec);
  void OnClose(boost::system::error_code ec);
  void OnRead(boost::system::error_code ec,
              std::size_t bytes_transferred,
              std::string text);

 private:
  // WebRTC からのコールバック
  // これらは別スレッドからやってくるので取り扱い注意
  void OnIceConnectionStateChange(
      webrtc::PeerConnectionInterface::IceConnectionState new_state) override;
  void OnIceCandidate(const std::string sdp_mid,
                      const int sdp_mlineindex,
                      const std::string sdp) override;

 private:
  void DoIceConnectionStateChange(
      webrtc::PeerConnectionInterface::IceConnectionState new_state);

 private:
  boost::asio::io_context& ioc_;
  std::unique_ptr<Websocket> ws_;

  std::atomic_bool destructed_ = {false};

  RTCManager* manager_;
  std::shared_ptr<RTCConnection> connection_;
  PionClientConfig config_;

  int retry_count_;
  webrtc::PeerConnectionInterface::IceConnectionState rtc_state_;

  WatchDog watchdog_;
};

#endif  // PION_CLIENT_H_