//
//  NTPClient.swift
//  TrueTime
//
//  Created by Michael Sanders on 10/12/16.
//  Copyright © 2016 Instacart. All rights reserved.
//

struct NTPConfig {
    let timeout: TimeInterval
    let maxRetries: Int
    let maxConnections: Int
    let maxServers: Int
    let numberOfSamples: Int
    let pollInterval: TimeInterval
}

final class NTPClient {
    let config: NTPConfig
    init(config: NTPConfig) {
        self.config = config
    }

    func start(pool: [String], port: Int) {
        precondition(!pool.isEmpty, "Must include at least one pool URL")
        queue.async {
            precondition(self.reachability.callback == nil, "Already started")
            self.pool = pool
            self.port = port
            self.reachability.callbackQueue = self.queue
            self.reachability.callback = self.updateReachability
            self.reachability.startMonitoring()
            self.startTimer()
        }
    }

    func pause() {
        queue.async {
            self.cancelTimer()
            self.reachability.stopMonitoring()
            self.reachability.callback = nil
            self.stopQueue()
        }
    }

    func fetchIfNeeded(queue callbackQueue: DispatchQueue,
                       first: ReferenceTimeCallback?,
                       completion: ReferenceTimeCallback?) {
        queue.async {
            precondition(self.reachability.callback != nil,
                         "Must start client before retrieving time")
            if let time = self.referenceTime {
                callbackQueue.async { first?(.success(time)) }
            } else if let first = first {
                self.startCallbacks.append((callbackQueue, first))
            }

            if let time = self.referenceTime, self.finished {
                callbackQueue.async { completion?(.success(time)) }
            } else {
                if let completion = completion {
                    self.completionCallbacks.append((callbackQueue, completion))
                }
                self.updateReachability(status: self.reachability.status ?? .notReachable)
            }
        }
    }

    private let referenceTimeLock: GCDLock<ReferenceTime?> = GCDLock(value: nil)
    var referenceTime: ReferenceTime? {
        get { return referenceTimeLock.read() }
        set { referenceTimeLock.write(newValue) }
    }

    fileprivate func debugLog(_ message: @autoclosure () -> String) {
#if DEBUG_LOGGING
        logger?(message())
#endif
    }

    var logger: LogCallback? = defaultLogger
    private let queue = DispatchQueue(label: "com.instacart.ntp.client")
    private let reachability = Reachability()
    private var completionCallbacks: [(DispatchQueue, ReferenceTimeCallback)] = []
    private var connections: [NTPConnection] = []
    private var finished: Bool = false
    private var invalidated: Bool = false
    private var startCallbacks: [(DispatchQueue, ReferenceTimeCallback)] = []
    private var startTime: TimeInterval?
    private var timer: DispatchSourceTimer?
    private var port: Int = 123
    private var pool: [String] = [] {
        didSet { invalidate() }
    }
}

private extension NTPClient {
    var started: Bool { return startTime != nil }
    func updateReachability(status: ReachabilityStatus) {
        switch status {
        case .notReachable:
            debugLog("Network unreachable")
            cancelTimer()
            finish(.failure(NSError(trueTimeError: .offline)))
        case .reachableViaWWAN, .reachableViaWiFi:
            debugLog("Network reachable")
            startTimer()
            startPool(pool: pool, port: port)
        }
    }

    func startTimer() {
        cancelTimer()
        if let referenceTime = referenceTime {
            let remainingInterval = max(0, config.pollInterval - referenceTime.underlyingValue.uptimeInterval)
            timer = DispatchSource.makeTimerSource(flags: [], queue: queue)
            timer?.setEventHandler(handler: invalidate)
            timer?.schedule(deadline: .now() + remainingInterval)
            timer?.resume()
        }
    }

    func cancelTimer() {
        timer?.cancel()
        timer = nil
    }

    func startPool(pool: [String], port: Int) {
        guard !started && !finished else {
            debugLog("Already \(started ? "started" : "finished")")
            return
        }

        startTime = CFAbsoluteTimeGetCurrent()
        debugLog("Resolving pool: \(pool)")
        HostResolver.resolve(hosts: pool.map { ($0, port) },
                             timeout: config.timeout,
                             logger: logger,
                             callbackQueue: queue) { resolver, result in
            guard self.started && !self.finished else {
                self.debugLog("Got DNS response after queue stopped: \(resolver), \(result)")
                return
            }
            guard pool == self.pool, port == self.port else {
                self.debugLog("Got DNS response after pool URLs changed: \(resolver), \(result)")
                return
            }

            switch result {
            case let .success(addresses): self.query(addresses: addresses, host: resolver.host)
            case let .failure(error): self.finish(.failure(error))
            }
        }
    }

    func stopQueue() {
        debugLog("Stopping queue")
        startTime = nil
        connections.forEach { $0.close(waitUntilFinished: true) }
        connections = []
    }

    func invalidate() {
        stopQueue()
        finished = false
        if let referenceTime = referenceTime,
               reachability.status != .notReachable && !pool.isEmpty {
            debugLog("Invalidated time \(referenceTime.debugDescription)")
            startPool(pool: pool, port: port)
        }
    }

    func query(addresses: [SocketAddress], host: String) {
        // 按IP地址组织结果：每个IP地址对应多个样本结果
        var results: [String: [FrozenNetworkTimeResult]] = [:]
        
        connections = NTPConnection.query(addresses: addresses,
                                          config: config,
                                          logger: logger,
                                          callbackQueue: queue) { connection, result in
            guard self.started && !self.finished else {
                self.debugLog("Got NTP response after queue stopped: \(result)")
                return
            }

            // 使用完整的IP地址作为key，而不是host
            let ipAddress = connection.address.host
            results[ipAddress] = (results[ipAddress] ?? []) + [result]

            // 计算总样本数
            let sampleSize = results.values.flatMap { $0 }.count
            let expectedCount = addresses.count * self.config.numberOfSamples
            let atEnd = sampleSize == expectedCount

            self.debugLog("Got \(sampleSize) out of \(expectedCount) samples from \(results.keys.count) IPs")

            // 为每个IP地址筛选出最小延迟的响应
            let bestTimePerIP = results.compactMap { (ip, samples) -> (String, FrozenNetworkTime)? in
                let validTimes = samples.compactMap { try? $0.get() }
                guard let bestTime = validTimes.min(by: { $0.serverResponse.delay < $1.serverResponse.delay }) else {
                    return nil
                }
                self.debugLog("Best time for IP \(ip): δ=\(bestTime.serverResponse.delay)ms, θ=\(bestTime.serverResponse.offset)ms")
                return (ip, bestTime)
            }

            // 从所有IP的最佳响应中选择offset的中位数
            if let selectedTime = self.selectMedianTimeByOffset(from: bestTimePerIP.map { $0.1 }) {
                let selectedIP = bestTimePerIP.first { $0.1.time == selectedTime.time }?.0 ?? "unknown"
                let finalTime = FrozenNetworkTime(networkTime: selectedTime, 
                                                  sampleSize: sampleSize, 
                                                  host: selectedIP)
                
                self.debugLog("\(atEnd ? "Final" : "Current best") time from IP \(selectedIP): " +
                              "δ: \(finalTime.serverResponse.delay)ms, " +
                              "θ: \(finalTime.serverResponse.offset)ms " +
                              "(median from \(bestTimePerIP.count) IPs)")

                let referenceTime = self.referenceTime ?? ReferenceTime(finalTime)
                if self.referenceTime == nil {
                    self.referenceTime = referenceTime
                } else {
                    referenceTime.underlyingValue = finalTime
                }

                if atEnd {
                    self.finish(.success(referenceTime))
                } else {
                    self.updateProgress(.success(referenceTime))
                }
            } else if atEnd {
                // 所有请求都完成了但没有有效结果
                let error: NSError
                if case let .failure(failure) = result {
                    error = failure as NSError
                } else {
                    error = NSError(trueTimeError: .noValidPacket)
                }
                self.finish(ReferenceTimeResult.failure(error))
            }
        }
    }
    
    /// 从多个时间响应中选择offset的中位数
    /// - Parameter times: 候选时间响应数组
    /// - Returns: 选中的中位数时间，如果输入为空则返回nil
    func selectMedianTimeByOffset(from times: [FrozenNetworkTime]) -> FrozenNetworkTime? {
        guard !times.isEmpty else { return nil }
        
        // 按offset排序
        let sortedTimes = times.sorted { $0.serverResponse.offset < $1.serverResponse.offset }
        
        // 返回中位数
        let medianIndex = sortedTimes.count / 2
        return sortedTimes[medianIndex]
    }

    func updateProgress(_ result: ReferenceTimeResult) {
        let endTime = CFAbsoluteTimeGetCurrent()
        let hasStartCallbacks = !startCallbacks.isEmpty
        startCallbacks.forEach { queue, callback in
            queue.async {
                callback(result)
            }
        }
        startCallbacks = []
        if hasStartCallbacks {
            logDuration(endTime, to: "get first result")
        }

        NotificationCenter.default.post(Notification(name: .TrueTimeUpdated, object: self, userInfo: nil))
    }

    func finish(_ result: ReferenceTimeResult) {
        let endTime = CFAbsoluteTimeGetCurrent()
        updateProgress(result)
        completionCallbacks.forEach { queue, callback in
            queue.async {
                callback(result)
            }
        }
        completionCallbacks = []
        logDuration(endTime, to: "get last result")
        finished = (try? result.get()) != nil
        stopQueue()
        startTimer()
    }

    func logDuration(_ endTime: CFAbsoluteTime, to description: String) {
        if let startTime = startTime {
            debugLog("Took \(endTime - startTime)s to \(description)")
        }
    }
}
