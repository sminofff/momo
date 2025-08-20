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

struct PionClientConfig {
  bool insecure = false;
  std::string signaling_url;  // ws://host:port/ws or wss://host:port/ws
  std::string video_codec_type = "";  // VP8, VP9, H264, H265, AV1, ALL, or empty for H264 default
};

class PionClient : public std::enable_shared_from_this<PionClient>,
                   public RTCMessageSender,
                   public StatsCollector {
  // Watchdog タイマー設定値
  static constexpr int kWatchdogInitialTimeout = 30;     // 初回接続時
  static constexpr int kWatchdogOfferTimeout = 15;       // offer 待機時
  static constexpr int kWatchdogKeepaliveTimeout = 45;   // 通常運用時
  static constexpr int kWatchdogIceConnected = 60;       // ICE 接続後
  static constexpr int kReconnectIntervalBase = 5;       // 再接続間隔ベース
  static constexpr int kReconnectIntervalMax = 30;       // 再接続間隔最大値

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

  void GetStats(std::function<void(
                    const webrtc::scoped_refptr<const webrtc::RTCStatsReport>&)>
                    callback) override;

 private:
  void ReconnectAfter();
  void OnWatchdogExpired();

 private:
  void DoRead();
  std::shared_ptr<RTCConnection> CreateRTCConnection();

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
  std::atomic_bool ice_connected_ = {false};  // SFU optimization: track ICE connection

  RTCManager* manager_;
  std::shared_ptr<RTCConnection> connection_;
  PionClientConfig config_;

  int retry_count_;
  webrtc::PeerConnectionInterface::IceConnectionState rtc_state_;

  WatchDog watchdog_;
};

#endif  // PION_CLIENT_H_