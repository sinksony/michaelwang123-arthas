package network

import (
	"fmt"
	"sync"
	"time"

	"github.com/arthas/arthas-server/internal/activity"
	"github.com/arthas/arthas-server/internal/dailytopic"
	"github.com/arthas/arthas-server/internal/hub"
	"github.com/arthas/arthas-server/internal/logger"
	"github.com/arthas/arthas-server/internal/match"
	"github.com/arthas/arthas-server/internal/room"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// Compile-time interface check: Hub must satisfy dailytopic.RoomCreator.
var _ dailytopic.RoomCreator = (*Hub)(nil)

// Compile-time interface check: Client must satisfy match.ClientRef.
var _ match.ClientRef = (*Client)(nil)

// Compile-time interface check: Hub must satisfy match.RoomCreator.
var _ match.RoomCreator = (*Hub)(nil)

// 📚 学习要点: 服务器端传输超时 — 兜底清理机制
// 正常流程中，文件传输通过 MSG_SEND_FILE_COMPLETE 或 MSG_SEND_FILE_CANCEL 消息结束，
// 此时服务器会清除客户端的 activeTransferID。
// 但如果这些终止消息丢失（网络异常、客户端崩溃但 TCP 连接未断开），
// activeTransferID 会永远残留，导致该客户端无法发起新的文件传输。
//
// serverTransferTimeout 设置为 90 秒，比客户端的 60 秒超时稍长。
// 这确保正常的客户端超时流程优先触发（客户端 60s 超时 → 发送 CANCEL → 服务器清除状态），
// 服务器端 90s 清理仅作为最后的兜底机制。
//
// staleTransferCheckInterval 设置为 30 秒，是清理频率与性能开销的平衡：
// - 太频繁（如 5s）：每次遍历所有客户端的开销不必要
// - 太稀疏（如 5min）：过期传输可能阻塞客户端过久
// - 30s 意味着最坏情况下，过期传输在 90s + 30s = 120s 后被清理
//
// expiryCheckInterval 设置为 60 秒，是过期检查的扫描间隔。
// 这意味着房间过期后最多 60 秒内会被检测并销毁。
// 对于用户体验来说 ±60s 的精度完全可接受（房间有效期最短 1 小时）。
// 此间隔与 staleTransferCheckInterval 独立，因为两者的关注点不同：
// - staleTransfer: 关注单个客户端的传输状态（需要更频繁检查，30s）
// - expiry: 关注房间级别的生命周期（60s 精度足够）
const (
	serverTransferTimeout      = 90 * time.Second // 服务器端传输超时（兜底清理阈值）
	staleTransferCheckInterval = 30 * time.Second // 过期传输检查间隔
	expiryCheckInterval        = 60 * time.Second // 房间过期检查间隔
)

// Hub 管理所有 WebSocket 连接，并将消息路由到对应的房间处理逻辑。
//
// 📚 学习要点: CSP 并发模型与关闭顺序
// Hub 采用 CSP（Communicating Sequential Processes）模型：
// - clients map 只在 Run() goroutine 中被修改（单一写者）
// - 其他 goroutine 通过 register/unregister channel 请求修改
//
// Goroutine 拓扑：
//
//	main goroutine → Hub.Run() goroutine
//	               → Client.readPump() goroutine (per client)
//	               → Client.writePump() goroutine (per client)
//
// 关闭顺序：
// 1. main() 调用 Hub.Stop() → 关闭 done channel
// 2. Run() 检测到 done 关闭 → 退出循环
// 3. Stop() 调用每个 client.closeSend() → 关闭 client.done → writePump 退出
// 4. writePump 退出后关闭 conn → readPump 读取失败退出
// 5. WaitGroup 计数归零 → main() 的 hub.Wait() 返回，继续退出
//
// ⚠️ 注意：client.send 通道**故意不被关闭**（见 Client 结构体注释）。
// 由于该通道可能被其它客户端的 goroutine 并发写入（文件分片转发），
// 关闭它会产生 panic: send on closed channel。改用 client.done 通知退出。
type Hub struct {
	roomManager     *room.RoomManager
	hubRegistry     *hub.HubRegistry // public room directory registry (nil = Hub disabled)
	activityTracker *activity.Tracker
	matchServer     *match.MatchServer // random match module (nil = feature disabled)
	clients         map[*Client]bool
	register        chan *Client
	unregister      chan *Client
	mu              sync.RWMutex

	// warnedExpiry 记录已发送过期预警的房间 ID，防止重复发送。
	// 📚 学习要点: 过期预警的幂等性
	// 每 60s 扫描一次，可能多次发现同一房间 remaining <= 300s。
	// 使用 set 记录已警告的房间，确保每个房间只收到一次预警消息。
	// 房间被销毁时从 set 中移除（cleanupExpiredRooms 中处理）。
	warnedExpiry map[string]bool

	// 📚 学习要点: done channel 的「close 广播」模式
	// 关闭一个 channel 会让所有阻塞在该 channel 上的 <-ch 操作立即返回零值。
	// 这是 Go 中实现「一对多取消通知」的惯用模式。
	// 当 Stop() 被调用时，close(done) 会同时唤醒 Run() 中的 select
	// 以及任何其他监听 done 的 goroutine，实现一对多的关闭通知。
	done chan struct{}

	// 📚 学习要点: sync.WaitGroup 用于等待一组 goroutine 完成
	// Add(n) 增加计数，Done() 减少计数，Wait() 阻塞直到计数归零。
	// 这里用于跟踪所有 readPump/writePump goroutine，确保关闭时等待它们退出。
	// 使用规则：Add() 必须在启动 goroutine 之前调用（在 ServeWs 中），
	// 否则可能出现 Wait() 在 Add() 之前返回的竞态条件。
	wg sync.WaitGroup
}

// NewHub 创建一个新的 Hub 实例，内部初始化 RoomManager。
func NewHub() *Hub {
	return &Hub{
		roomManager:     room.NewRoomManager(),
		activityTracker: activity.New(5*time.Minute, 10000),
		clients:         make(map[*Client]bool),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		done:            make(chan struct{}),
		warnedExpiry:    make(map[string]bool),
	}
}

// SetHubRegistry sets the Hub directory registry for public room listing.
func (h *Hub) SetHubRegistry(registry *hub.HubRegistry) {
	h.hubRegistry = registry
}

// SetMatchServer sets the match server for random match functionality.
// If nil, match messages (0x20-0x2F) will receive an M001 "feature disabled" error.
func (h *Hub) SetMatchServer(ms *match.MatchServer) {
	h.matchServer = ms
}

// CreateDailyTopicRoom creates a room internally (no WebSocket client involved)
// and registers it as a daily topic in the Hub directory.
// Called by the DailyTopic scheduler.
func (h *Hub) CreateDailyTopicRoom(params dailytopic.DailyRoomParams) (string, error) {
	// 防御性检查：hubRegistry 未初始化时拒绝操作
	if h.hubRegistry == nil {
		return "", fmt.Errorf("hub registry not initialized, cannot create daily topic room")
	}

	// 1. 生成 NanoID 作为房间 ID
	roomID, err := gonanoid.New()
	if err != nil {
		return "", fmt.Errorf("failed to generate room ID: %w", err)
	}

	// 2. 创建房间（无密码、非阅后即焚、指定过期时间、默认成员上限）
	h.roomManager.CreateRoom(roomID, "", 0, params.ExpiresAt, 0)

	// 3. 构建 RoomListing 并注册到 Hub Registry
	listing := &hub.RoomListing{
		RoomID:       roomID,
		KeyEncoded:   params.KeyEncoded,
		Title:        params.Title,
		Description:  params.Description,
		Tags:         params.Tags,
		MemberCount:  0,
		HasPassword:  false,
		CreatedAt:    time.Now().Unix(),
		ExpiresAt:    params.ExpiresAt,
		Ephemeral:    0,
		IsDailyTopic: true,
	}

	if err := h.hubRegistry.Register(listing); err != nil {
		// 注册失败时清理已创建的房间，避免资源泄漏
		h.roomManager.RemoveRoom(roomID)
		return "", fmt.Errorf("failed to register daily topic room: %w", err)
	}

	logger.Info("Hub", "daily topic room %s created: %q", roomID, params.Title)
	return roomID, nil
}

// CreateMatchRoom creates a temporary match room with maxMembers=2.
// The room is NOT registered in HubRegistry (not visible in public room listing).
// Called by MatchServer via the match.RoomCreator interface.
func (h *Hub) CreateMatchRoom(expiresAt int64, ephemeral int) (string, error) {
	// 1. Generate NanoID as room ID.
	roomID, err := gonanoid.New()
	if err != nil {
		return "", fmt.Errorf("failed to generate room ID: %w", err)
	}

	// 2. Create room with maxMembers=2, no password, specified ephemeral and expiry.
	// NOT registered in HubRegistry — match rooms are private 1v1 rooms.
	h.roomManager.CreateRoom(roomID, "", ephemeral, expiresAt, 2)

	logger.Info("Hub", "match room %s created (expires=%d, ephemeral=%d)", roomID, expiresAt, ephemeral)
	return roomID, nil
}

// JoinClientToRoom joins a client (referenced via match.ClientRef) into the specified room.
// Sets the client's name and room association, then adds it as a room member.
// Broadcasts MSG_MEMBER_JOINED to existing members and sends MSG_ROOM_JOINED to the joiner.
// Called by MatchServer via the match.RoomCreator interface.
func (h *Hub) JoinClientToRoom(client match.ClientRef, roomId string, name string) error {
	// Type-assert ClientRef to *Client (Hub owns the concrete type).
	c, ok := client.(*Client)
	if !ok {
		return fmt.Errorf("client is not a *network.Client")
	}

	// Look up the room.
	r := h.roomManager.GetRoom(roomId)
	if r == nil {
		return fmt.Errorf("room %s not found", roomId)
	}

	// Set client fields. Use short ID suffix to differentiate anonymous users.
	c.Name = name
	c.Color = generateColor(c.ID)
	c.RoomID = roomId

	// Add client as a member to the room.
	member := &room.Member{
		ID:    c.ID,
		Name:  c.Name,
		Color: c.Color,
		SendFunc: func(data []byte) {
			c.Send(data)
		},
		SendFileFunc: func(data []byte) bool {
			return c.SendFileData(data)
		},
	}
	if err := r.AddMember(member); err != nil {
		c.RoomID = ""
		return fmt.Errorf("failed to add client to room %s: %w", roomId, err)
	}

	// Broadcast MSG_MEMBER_JOINED to existing members (excluding the new joiner).
	// This ensures all participants see each other in the member list.
	memberJoinedMsg := Message{
		Type: MsgMemberJoined,
		Data: MemberJoinedData{
			ID:    c.ID,
			Name:  c.Name,
			Color: c.Color,
		},
	}
	broadcastData, err := msgpack.Marshal(memberJoinedMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal MemberJoined in match room: %v", err)
	} else {
		r.Broadcast(c.ID, broadcastData)
	}

	// Send MSG_ROOM_JOINED to the joining client so chatStore properly initializes
	// roomId, myId, members, ephemeral, etc.
	members := r.GetMembers()
	memberInfos := make([]MemberInfo, len(members))
	for i, m := range members {
		memberInfos[i] = MemberInfo{ID: m.ID, Name: m.Name, Color: m.Color}
	}
	h.sendToClient(c, MsgRoomJoined, RoomJoinedData{
		RoomID:    roomId,
		Members:   memberInfos,
		Ephemeral: r.Ephemeral,
		ExpiresAt: r.GetExpiresAt(),
	})

	return nil
}

// ExtendMatchRoom updates the expiration timestamp for a match room.
// Returns false if the room does not exist.
// Called by MatchServer via the match.RoomCreator interface.
func (h *Hub) ExtendMatchRoom(roomId string, newExpiresAt int64) bool {
	return h.roomManager.ExtendRoomExpiry(roomId, newExpiresAt)
}

// LeaveMatchRoom removes a client from their current room.
// Handles member removal and room cleanup (same logic as normal leave).
// Called by MatchServer via the match.RoomCreator interface.
func (h *Hub) LeaveMatchRoom(client match.ClientRef) error {
	c, ok := client.(*Client)
	if !ok {
		return fmt.Errorf("client is not a *network.Client")
	}

	if c.RoomID == "" {
		return nil // not in a room
	}

	r := h.roomManager.GetRoom(c.RoomID)
	if r == nil {
		c.RoomID = ""
		return nil
	}

	r.RemoveMember(c.ID)
	roomID := c.RoomID
	c.RoomID = ""

	// Clean up empty room
	if r.MemberCount() == 0 {
		h.roomManager.RemoveRoom(roomID)
	}

	return nil
}

// Run 启动 Hub 主循环，处理客户端注册/注销事件。
// 当 done channel 被关闭时（通过 Stop() 调用），Run 退出循环并返回。
//
// 📚 学习要点: select 多路复用与 done channel
// select 语句让一个 goroutine 同时等待多个 channel 操作。
// 当 done channel 被关闭后，<-h.done 会立即返回零值（struct{}{}），
// 使得每次循环迭代都会匹配到该 case，从而退出 for 循环。
// 这是 Go 中实现「可取消事件循环」的标准模式。
func (h *Hub) Run() {
	// 📚 学习要点: 定时器（Ticker）用于周期性任务
	// time.NewTicker 创建一个定时器，每隔指定时间向其 C channel 发送当前时间。
	// 这里用于周期性扫描并清理超时的文件传输状态。
	//
	// 📚 学习要点: defer ticker.Stop() 防止资源泄漏
	// Ticker 在创建后会持续运行，即使没有人从 C channel 读取。
	// 如果 Run() 退出但 Ticker 未停止，底层的 runtime timer 会一直存在（内存泄漏）。
	// defer 确保无论 Run() 如何退出（正常 return 或 panic），Ticker 都会被停止。
	staleTransferTicker := time.NewTicker(staleTransferCheckInterval)
	defer staleTransferTicker.Stop()

	// expiryTicker 用于周期性扫描并销毁过期房间（与 staleTransferTicker 模式一致）。
	// 每 60 秒触发一次，调用 cleanupExpiredRooms() 检查并销毁所有已过期的房间。
	// 📚 学习要点: 多个 Ticker 在同一 select 中的协作
	// Hub.Run() 的 select 同时监听多个 ticker channel，Go runtime 保证
	// 当多个 case 同时就绪时随机选择一个执行（公平调度）。
	// 这意味着 expiryTicker 和 staleTransferTicker 不会互相饿死。
	expiryTicker := time.NewTicker(expiryCheckInterval)
	defer expiryTicker.Stop()

	for {
		select {
		case <-h.done:
			// 📚 学习要点: 关闭的 channel 立即返回零值
			// 一旦 done 被 close()，每次 select 都会走到这个 case，退出循环。
			// 这使得 Run() 可以被外部（Stop()）优雅地终止。
			return
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			logger.Info("Hub", "client %s connected, total: %d", client.ID, h.clientCount())

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				// ⚠️ 不 close(client.send)！
				// send 通道可能被其它客户端的 goroutine 并发写入
				// （文件分片经 BroadcastFileData 跨连接转发），
				// 关闭它会产生 panic: send on closed channel，导致整个进程崩溃。
				// 改为关闭 client.done 通知 writePump 退出，通道交由 GC 回收。
				client.closeSend()
			}
			h.mu.Unlock()

			// 断线自动离开房间
			h.handleClientDisconnect(client)
			// 通知 Match_Server 客户端断线
			if h.matchServer != nil {
				h.matchServer.HandleDisconnect(client.ID)
			}
			logger.Info("Hub", "client %s disconnected, total: %d", client.ID, h.clientCount())

		case <-staleTransferTicker.C:
			// 📚 学习要点: 定时清理过期传输（Stale Transfer Cleanup）
			// 每 30 秒触发一次，扫描所有客户端，清理超过 90 秒的活跃传输。
			// 这是一个兜底机制，处理以下异常场景：
			// 1. 客户端发送了 Meta 但从未发送 Complete/Cancel（bug 或恶意行为）
			// 2. Complete/Cancel 消息在网络中丢失（极端网络条件）
			// 3. 客户端进程崩溃但 TCP 连接未立即断开（TCP keepalive 延迟）
			//
			// 📚 学习要点: 为什么在 select 中处理而非独立 goroutine？
			// 将 ticker 放在 Run() 的 select 中（而非启动独立 goroutine）的好处：
			// - 与 done channel 自然集成：Run() 退出时 ticker 自动停止
			// - 无需额外的同步机制：cleanupStaleTransfers 在 Run() goroutine 中执行
			// - 生命周期清晰：ticker 的生命周期与 Hub 完全一致
			h.cleanupStaleTransfers()

		case <-expiryTicker.C:
			// 📚 学习要点: 房间过期清理（Room Expiry Cleanup）
			// 每 60 秒触发一次，扫描所有房间，销毁已过期的房间。
			// 与 staleTransferTicker 采用相同的 select-case 模式：
			// - 在 Hub.Run() goroutine 中执行，无需额外同步
			// - 与 done channel 自然集成，Run() 退出时自动停止
			// - 可以安全访问 h.clients map（同一 goroutine，无竞态）
			//
			// 📚 学习要点: 为什么不与 staleTransferTicker 合并？
			// 两者关注点不同且最优间隔不同：
			// - staleTransfer (30s): 客户端级别，需要更快响应以解除传输阻塞
			// - expiry (60s): 房间级别，±60s 精度对小时/天级有效期完全足够
			// 分离 ticker 使得未来可以独立调整各自的间隔而不互相影响。
			h.cleanupExpiredRooms()
			// Prune expired activity data (piggyback on existing 60s ticker — Req 3.2).
			h.activityTracker.Cleanup()
			// 检查即将过期的房间并发送预警（在清理之后执行，避免对已销毁房间发送预警）
			h.warnApproachingExpiry(h.roomManager.NowFunc())
		}
	}
}

// Stop 触发优雅关闭：通知 Run() 退出，并主动关闭所有客户端连接。
//
// 📚 学习要点: 关闭顺序的重要性
// 1. 先 close(done) — 让 Run() 退出，不再处理 register/unregister
// 2. 再对每个 client 调用 closeSend()（关闭 client.done）— 让 writePump 退出
// 3. writePump 退出时关闭 WebSocket conn — 让 readPump 读取失败退出
// 该顺序配合「不关闭 send 通道」的策略，确保不会出现「向已关闭 channel 发送」的 panic。
//
// 📚 学习要点: 为什么使用 Lock()（写锁）而非 RLock()（读锁）？
// Stop() 需要修改 clients map（delete 操作）和关闭 send channel，
// 这是写操作。同时 clientCount() 使用 RLock 读取 map 长度。
// 如果 Stop() 用 RLock，会与 clientCount() 的 RLock 并发执行，
// 但 delete 操作不是并发安全的 — 必须用写锁互斥所有读者。
func (h *Hub) Stop() {
	close(h.done)

	h.mu.Lock()
	for client := range h.clients {
		// ⚠️ 不 close(client.send)！理由同 Run() 的注销分支：
		// 该通道可能仍被别的客户端 goroutine 并发写入，关闭会 panic。
		// 用 closeSend() 通知 writePump 退出即可。
		client.closeSend()
		delete(h.clients, client)
	}
	h.mu.Unlock()
}

// Wait 阻塞直到所有客户端 goroutine（readPump + writePump）退出。
//
// 📚 学习要点: WaitGroup 的语义
// wg.Wait() 会阻塞调用者，直到内部计数器归零。
// 每个 readPump/writePump 在启动前调用 wg.Add(1)，退出时调用 wg.Done()。
// 当所有 pump goroutine 都退出后，Wait() 返回，表示可以安全退出进程。
//
// 典型调用模式（在 main.go 中）：
//
//	hub.Stop()   // 触发关闭
//	hub.Wait()   // 等待所有 goroutine 退出
//	os.Exit(0)   // 安全退出
func (h *Hub) Wait() {
	h.wg.Wait()
}

// HandleMessage 解析原始消息并根据类型路由到对应的处理函数。
//
// 📚 学习要点: 消息路由与频率限制的分层设计
// HandleMessage 是一个纯路由器（dispatcher），不做任何业务逻辑判断。
// 频率限制（rate limiting）不在此处集中实施，而是由各 handler 自行决定：
//
//   - 聊天消息 (MsgSendMessage): 使用 IsRateLimited() 滑动窗口限流
//     → 防止刷屏，10 秒内最多 N 条消息
//
//   - 文件传输 (MsgSendFileMeta/Chunk/Complete/Cancel/Ack): 不使用 IsRateLimited()
//     → 文件传输有自己的并发控制机制：每客户端最多 1 个活跃传输 (activeTransferID)
//     → 原因：文件传输需要高频发送 chunk（每 10ms 一个），
//     如果套用聊天的频率限制（10 秒 N 条），传输会被频繁阻断
//     → 安全保障：activeTransferID 检查确保客户端只能为自己的活跃传输发送数据
//
// 这种「每个 handler 自治」的设计比「集中式前置检查」更灵活：
// 不同消息类型有不同的流量特征，统一限流策略无法兼顾所有场景。
func (h *Hub) HandleMessage(client *Client, msg *Message) {
	// Match messages: 0x20-0x2F → delegate to MatchServer
	if msg.Type >= 0x20 && msg.Type <= 0x2F {
		if h.matchServer != nil {
			// Use pre-computed rawData bytes to avoid double-serialization.
			data := msg.rawData
			if data == nil {
				// Fallback: re-marshal if rawData wasn't set (e.g., direct HandleMessage call in tests).
				var err error
				data, err = msgpack.Marshal(msg.Data)
				if err != nil {
					logger.Error("Hub", "failed to marshal match message data: %v", err)
					return
				}
			}
			h.matchServer.HandleMessage(client, msg.Type, data)
		} else {
			// Feature disabled: send M001 error in match protocol format
			h.sendMatchError(client, match.ErrCodeMatchDisabled, "random match is disabled")
		}
		return
	}

	switch msg.Type {
	case MsgCreateRoom:
		h.handleCreateRoom(client, msg.Data)
	case MsgJoinRoom:
		h.handleJoinRoom(client, msg.Data)
	case MsgSendMessage:
		h.handleSendMessage(client, msg.Data)
	case MsgLeaveRoom:
		h.handleLeaveRoom(client, msg.Data)
	case MsgTyping:
		h.handleTyping(client, msg.Data)
	case MsgPong:
		h.handlePong(client, msg.Data)
	case MsgSendReaction:
		h.handleSendReaction(client, msg.Data)

	// --- 文件传输消息 (0x08-0x0C) ---
	// 📚 学习要点: 文件传输消息的频率限制豁免
	// 这些消息类型不经过 IsRateLimited() 检查（Requirements 4.9）。
	// 替代方案：handleFileMeta 中的 activeTransferID 检查确保
	// 每个客户端同时只有 1 个活跃传输，这比时间窗口限流更适合文件传输场景。
	// handleFileChunk 验证 transferId 必须匹配 activeTransferID，
	// 防止未授权的数据注入。
	case MsgSendFileMeta:
		h.handleFileMeta(client, msg.Data)
	case MsgSendFileChunk:
		h.handleFileChunk(client, msg.Data)
	case MsgSendFileComplete:
		h.handleFileComplete(client, msg.Data)
	case MsgSendFileCancel:
		h.handleFileCancel(client, msg.Data)
	case MsgSendFileAck:
		h.handleFileAck(client, msg.Data)

	default:
		logger.Warn("Hub", "unknown message type 0x%02x from client %s", msg.Type, client.ID)
	}
}

// ParseAndHandleMessage 从原始字节解析消息并路由处理。
// 供 client.go 的 readPump 调用。
func (h *Hub) ParseAndHandleMessage(client *Client, raw []byte) {
	var msg Message
	if err := msgpack.Unmarshal(raw, &msg); err != nil {
		logger.Warn("Hub", "failed to unmarshal message from %s: %v", client.ID, err)
		h.sendError(client, ErrCodeInvalidMessage, "invalid message format")
		return
	}

	// For match messages (0x20-0x2F), preserve raw data bytes to avoid double-serialization.
	// The match server will deserialize the data itself from these bytes.
	if msg.Type >= 0x20 && msg.Type <= 0x2F && msg.Data != nil {
		rawData, err := msgpack.Marshal(msg.Data)
		if err == nil {
			msg.rawData = rawData
		}
	}

	h.HandleMessage(client, &msg)
}

// --- 消息处理 stub（Task 2 实现） ---

func (h *Hub) handleCreateRoom(client *Client, data interface{}) {
	// Reject if client is already in a room
	if client.RoomID != "" {
		h.sendError(client, ErrCodeInvalidMessage, "already in a room")
		return
	}

	// Parse data — msgpack deserializes into map[string]interface{}
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid create room data")
		return
	}

	name, _ := dataMap["name"].(string)

	// Validate name: 1-20 characters, non-empty
	if len(name) == 0 || len([]rune(name)) > 20 {
		h.sendError(client, ErrCodeInvalidMessage, "name must be 1-20 characters")
		return
	}

	// Parse password (SHA-256 hash from client, 64 hex chars or empty)
	password, _ := dataMap["password"].(string)

	// Validate password: if non-empty, must be exactly 64 hex characters (SHA-256 hash)
	if password != "" && len(password) != 64 {
		h.sendError(client, ErrCodeInvalidMessage, "invalid password format")
		return
	}

	// Parse ephemeral — msgpack deserializes small integers as int8/uint8/int16/uint16 etc.
	ephemeral := toInt(dataMap["ephemeral"])

	// 📚 学习要点: 防御性输入清洗（Defensive Input Sanitization）
	// 客户端可能发送任意 expiry 值（bug、恶意行为、或旧客户端不发送此字段）。
	// 服务器必须做边界检查，确保系统安全和稳定：
	// - 负数: 视为 0（无过期），不返回错误（宽容处理，避免因客户端 bug 导致创建失败）
	// - 超大值: 截断为 MaxExpiryDuration（7天=604800秒），防止内存耗尽攻击
	//   （恶意客户端设置 expiry=999999999 会创建几乎永不过期的房间）
	// - 0: 保持原样，表示永不过期（向后兼容旧客户端）
	// 这种「静默修正」策略（而非返回错误）是 Postel 法则的体现：
	// "Be conservative in what you send, be liberal in what you accept."
	expiry := toInt(dataMap["expiry"])
	if expiry < 0 {
		expiry = 0
	}
	if int64(expiry) > room.MaxExpiryDuration {
		expiry = int(room.MaxExpiryDuration)
	}

	// 计算 expiresAt：expiry > 0 时为当前时间 + 有效期秒数，否则为 0（永不过期）
	var expiresAt int64
	if expiry > 0 {
		expiresAt = time.Now().Unix() + int64(expiry)
	}

	// Generate NanoID (21 chars) for roomId
	roomId, err := gonanoid.New()
	if err != nil {
		logger.Error("Hub", "failed to generate NanoID: %v", err)
		h.sendError(client, ErrCodeInvalidMessage, "failed to create room")
		return
	}

	// Create the room via RoomManager with password, ephemeral, computed expiresAt, and default max members
	r := h.roomManager.CreateRoom(roomId, password, ephemeral, expiresAt, 0)

	// Set client fields
	client.Name = name
	client.Color = generateColor(client.ID)
	client.RoomID = roomId

	// Add client as a member to the room
	member := &room.Member{
		ID:    client.ID,
		Name:  client.Name,
		Color: client.Color,
		SendFunc: func(data []byte) {
			client.Send(data)
		},
		SendFileFunc: func(data []byte) bool {
			return client.SendFileData(data)
		},
	}
	if err := r.AddMember(member); err != nil {
		logger.Error("Hub", "failed to add creator to room: %v", err)
		h.sendError(client, ErrCodeRoomFull, "room is full")
		return
	}

	// Send RoomCreated back to the client (包含计算后的 expiresAt 供客户端启动倒计时)
	h.sendToClient(client, MsgRoomCreated, RoomCreatedData{
		RoomID:    roomId,
		ExpiresAt: expiresAt,
	})

	// Send RoomJoined with the creator as the only member
	// 📚 学习要点: RoomJoined 必须包含 ExpiresAt 字段
	// 前端 chatStore 的 MSG_ROOM_JOINED handler 会用 data.expiresAt 覆盖 store 中的值。
	// 如果此处不包含 expiresAt，前端会将其视为 0（无过期），
	// 覆盖掉之前 MSG_ROOM_CREATED 中设置的正确值，导致倒计时不显示。
	h.sendToClient(client, MsgRoomJoined, RoomJoinedData{
		RoomID: roomId,
		Members: []MemberInfo{
			{
				ID:    client.ID,
				Name:  client.Name,
				Color: client.Color,
			},
		},
		HasPassword: password != "",
		Ephemeral:   ephemeral,
		ExpiresAt:   expiresAt,
	})

	// --- Hub public listing (optional) ---
	isPublic, _ := dataMap[FieldPublic].(bool)
	if isPublic && h.hubRegistry != nil {
		title, _ := dataMap[FieldTitle].(string)
		title = hub.SanitizeString(title)
		description, _ := dataMap[FieldDescription].(string)
		description = hub.SanitizeString(description)

		// Parse tags array
		var tags []string
		if rawTags, ok := dataMap[FieldTags].([]interface{}); ok {
			for _, t := range rawTags {
				if s, ok := t.(string); ok {
					tags = append(tags, s)
				}
			}
		}

		// Parse keyEncoded (base64url-encoded AES key from client)
		keyEncoded, _ := dataMap[FieldKeyEncoded].(string)

		// Validate listing metadata
		if err := hub.ValidateListing(title, description, tags); err != nil {
			// Validation failed — room is still created as private, send error info
			h.sendError(client, ErrCodeInvalidListing, err.Error())
			logger.Warn("Hub", "public listing validation failed for room %s: %v", roomId, err)
		} else if keyEncoded == "" {
			// keyEncoded is required for public listing
			h.sendError(client, ErrCodeInvalidListing, "keyEncoded is required for public rooms")
			logger.Warn("Hub", "public listing missing keyEncoded for room %s", roomId)
		} else {
			// Register the room in the Hub directory
			listing := &hub.RoomListing{
				RoomID:      roomId,
				KeyEncoded:  keyEncoded,
				Title:       title,
				Description: description,
				Tags:        tags,
				MemberCount: 1, // creator is the first member
				HasPassword: password != "",
				CreatedAt:   time.Now().Unix(),
				ExpiresAt:   expiresAt,
				Ephemeral:   ephemeral,
			}
			if err := h.hubRegistry.Register(listing); err != nil {
				// Hub is full — room still created as private
				h.sendError(client, ErrCodeHubFull, "hub is full, room created as private")
				logger.Warn("Hub", "hub registry full, room %s created as private", roomId)
			} else {
				logger.Info("Hub", "room %s listed publicly in Hub: %q", roomId, title)
			}
		}
	}

	logger.Info("Hub", "room %s created by client %s (%s), total rooms: %d", roomId, client.ID, name, h.roomManager.RoomCount())
}

func (h *Hub) handleJoinRoom(client *Client, data interface{}) {
	// Reject if client is already in a room
	if client.RoomID != "" {
		h.sendError(client, ErrCodeInvalidMessage, "already in a room")
		return
	}

	// Parse data — msgpack deserializes into map[string]interface{}
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid join room data")
		return
	}

	roomId, _ := dataMap["roomId"].(string)
	name, _ := dataMap["name"].(string)
	password, _ := dataMap["password"].(string)

	// Validate name: 1-20 characters
	if len(name) == 0 || len([]rune(name)) > 20 {
		h.sendError(client, ErrCodeInvalidMessage, "name must be 1-20 characters")
		return
	}

	// Look up the room
	r := h.roomManager.GetRoom(roomId)
	if r == nil {
		h.sendError(client, ErrCodeRoomNotFound, "room not found")
		return
	}

	// 📚 学习要点: JoinRoom 过期检查 — 双重防线（Dual Defense）
	// 即使 Expiry_Checker（60s 周期扫描）还没来得及清理过期房间，
	// handleJoinRoom 也会立即拒绝过期房间的加入请求。这提供了两层保护：
	// 1. Expiry_Checker: 主动清理（异步，±60s 精度）— 负责销毁房间和通知在线成员
	// 2. handleJoinRoom: 被动拒绝（同步，实时精度）— 负责阻止新成员加入已过期房间
	// 两者互补：Expiry_Checker 处理「已在房间内的成员」，JoinRoom 处理「试图加入的新成员」。
	// 最坏情况下，过期房间在 Expiry_Checker 下一次扫描前仍存在于内存中，
	// 但任何新的 JoinRoom 请求都会被实时拒绝，确保用户体验一致。
	if r.IsExpired(time.Now().Unix()) {
		h.sendError(client, ErrCodeRoomExpired, "room has expired")
		return
	}

	// Verify password if room is password-protected
	if r.PasswordHash != "" && password != r.PasswordHash {
		h.sendError(client, ErrCodeWrongPassword, "wrong password")
		return
	}

	// Check if room is full
	if r.IsFull() {
		h.sendError(client, ErrCodeRoomFull, "room is full")
		return
	}

	// Set client fields
	client.Name = name
	client.Color = generateColor(client.ID)
	client.RoomID = roomId

	// Get current members BEFORE adding the new client (for building the full member list)
	existingMembers := r.GetMembers()

	// Add client as a member to the room
	member := &room.Member{
		ID:    client.ID,
		Name:  client.Name,
		Color: client.Color,
		SendFunc: func(data []byte) {
			client.Send(data)
		},
		SendFileFunc: func(data []byte) bool {
			return client.SendFileData(data)
		},
	}
	if err := r.AddMember(member); err != nil {
		// Race condition: room became full between IsFull() check and AddMember()
		client.RoomID = ""
		h.sendError(client, ErrCodeRoomFull, "room is full")
		return
	}

	// Update Hub member count for public rooms
	if h.hubRegistry != nil {
		h.hubRegistry.UpdateMemberCount(roomId, r.MemberCount())
	}

	// Broadcast MemberJoined to all existing members (excluding the new joiner)
	memberJoinedMsg := Message{
		Type: MsgMemberJoined,
		Data: MemberJoinedData{
			ID:    client.ID,
			Name:  client.Name,
			Color: client.Color,
		},
	}
	broadcastData, err := msgpack.Marshal(memberJoinedMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal MemberJoined message: %v", err)
	} else {
		r.Broadcast(client.ID, broadcastData)
	}

	// Build full member list (existing members + the new joiner)
	members := make([]MemberInfo, 0, len(existingMembers)+1)
	for _, m := range existingMembers {
		members = append(members, MemberInfo{
			ID:    m.ID,
			Name:  m.Name,
			Color: m.Color,
		})
	}
	members = append(members, MemberInfo{
		ID:    client.ID,
		Name:  client.Name,
		Color: client.Color,
	})

	// Send RoomJoined to the joining client (包含 expiresAt 供客户端启动倒计时)
	h.sendToClient(client, MsgRoomJoined, RoomJoinedData{
		RoomID:      roomId,
		Members:     members,
		HasPassword: r.PasswordHash != "",
		Ephemeral:   r.Ephemeral,
		ExpiresAt:   r.GetExpiresAt(),
	})

	logger.Info("Hub", "client %s (%s) joined room %s", client.ID, name, roomId)
}

func (h *Hub) handleSendMessage(client *Client, data interface{}) {
	// 1. 检查客户端是否在房间中
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data 提取 iv 和 ciphertext
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid message data")
		return
	}

	iv, _ := dataMap["iv"].(string)
	ciphertext, _ := dataMap["ciphertext"].(string)

	// 3. 验证 iv 和 ciphertext 非空
	if iv == "" || ciphertext == "" {
		h.sendError(client, ErrCodeInvalidMessage, "iv and ciphertext must be non-empty strings")
		return
	}

	// 4. 频率限制检查
	if client.IsRateLimited() {
		h.sendError(client, ErrCodeRateLimited, "rate limited, please slow down")
		return
	}

	// 5. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		// 房间已被销毁（极端情况）
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 6. 构建 RelayMessage（附带服务器时间戳）
	now := time.Now().UnixMilli()
	relayMsg := Message{
		Type: MsgRelayMessage,
		Data: RelayMessageData{
			SenderID:   client.ID,
			SenderName: client.Name,
			IV:         iv,
			Ciphertext: ciphertext,
			T:          now,
		},
	}

	// 7. 序列化并广播给房间内其他成员（排除发送者）
	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayMessage: %v", err)
		return
	}
	r.Broadcast(client.ID, broadcastData)

	// Track activity for public rooms (used for Hub directory sorting).
	// Only message relays are counted — reactions are excluded (Req 1.8).
	if h.hubRegistry != nil && h.hubRegistry.GetListing(client.RoomID) != nil {
		h.activityTracker.Increment(client.RoomID)
	}

	// 注意：不记录 ciphertext 内容（零知识设计）
}

func (h *Hub) handleSendReaction(client *Client, data interface{}) {
	// 1. 检查客户端是否在房间中
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data 提取 iv 和 ciphertext
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid reaction data")
		return
	}

	iv, _ := dataMap["iv"].(string)
	ciphertext, _ := dataMap["ciphertext"].(string)

	// 3. 验证 iv 和 ciphertext 非空
	if iv == "" || ciphertext == "" {
		h.sendError(client, ErrCodeInvalidMessage, "iv and ciphertext must be non-empty strings")
		return
	}

	// 4. 不做频率限制（反应是轻量交互）

	// 5. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 6. 构建 RelayReaction 并广播给房间内其他成员
	now := time.Now().UnixMilli()
	relayMsg := Message{
		Type: MsgRelayReaction,
		Data: RelayReactionData{
			SenderID:   client.ID,
			SenderName: client.Name,
			IV:         iv,
			Ciphertext: ciphertext,
			T:          now,
		},
	}

	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayReaction: %v", err)
		return
	}
	r.Broadcast(client.ID, broadcastData)
}

func (h *Hub) handleLeaveRoom(client *Client, data interface{}) {
	// If client is not in a room, nothing to do
	if client.RoomID == "" {
		return
	}

	roomId := client.RoomID

	// Look up the room
	r := h.roomManager.GetRoom(roomId)
	if r != nil {
		// Remove the client from the room
		remaining := r.RemoveMember(client.ID)

		// Update Hub member count / unregister for public rooms
		if h.hubRegistry != nil {
			if remaining == 0 {
				// 每日话题房间空了不删除 listing（保留在 Hub 目录中直到过期）
				// 普通公开房间空了则从目录移除
				listing := h.hubRegistry.GetListing(roomId)
				if listing == nil || !listing.IsDailyTopic {
					h.hubRegistry.Unregister(roomId)
					h.activityTracker.Remove(roomId)
				} else {
					h.hubRegistry.UpdateMemberCount(roomId, 0)
				}
			} else {
				h.hubRegistry.UpdateMemberCount(roomId, remaining)
			}
		}

		// Broadcast MemberLeft to remaining members
		memberLeftMsg := Message{
			Type: MsgMemberLeft,
			Data: MemberLeftData{
				ID: client.ID,
			},
		}
		broadcastData, err := msgpack.Marshal(memberLeftMsg)
		if err != nil {
			logger.Error("Hub", "failed to marshal MemberLeft message: %v", err)
		} else {
			r.Broadcast(client.ID, broadcastData)
		}

		// If room is now empty, remove it (but keep daily topic rooms alive)
		if remaining == 0 {
			if h.hubRegistry != nil {
				listing := h.hubRegistry.GetListing(roomId)
				if listing != nil && listing.IsDailyTopic {
					// Daily topic room: keep the room alive in RoomManager (don't destroy)
					logger.Info("Hub", "daily topic room %s now empty, keeping alive", roomId)
				} else {
					h.roomManager.RemoveRoom(roomId)
					logger.Info("Hub", "room %s destroyed (empty), total rooms: %d", roomId, h.roomManager.RoomCount())
				}
			} else {
				h.roomManager.RemoveRoom(roomId)
				logger.Info("Hub", "room %s destroyed (empty), total rooms: %d", roomId, h.roomManager.RoomCount())
			}
		}
	}

	// Clear client's room association
	client.RoomID = ""

	logger.Info("Hub", "client %s left room %s", client.ID, roomId)
}

func (h *Hub) handleTyping(client *Client, data interface{}) {
	// 1. Check if client is in a room
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. Parse data to extract typing boolean
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid typing data")
		return
	}

	typing, _ := dataMap["typing"].(bool)

	// 3. Look up the room
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 4. Create MemberTyping message and broadcast to room (excluding sender)
	typingMsg := Message{
		Type: MsgMemberTyping,
		Data: MemberTypingData{
			ID:     client.ID,
			Typing: typing,
		},
	}
	broadcastData, err := msgpack.Marshal(typingMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal MemberTyping message: %v", err)
		return
	}
	r.Broadcast(client.ID, broadcastData)
}

func (h *Hub) handlePong(client *Client, data interface{}) {
	// 更新客户端最后一次 Pong 时间戳，确认客户端存活
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return
	}
	// 提取客户端回传的时间戳（可用于 RTT 计算）
	if t := toInt(dataMap["t"]); t != 0 {
		client.LastPong = int64(t)
	} else {
		// 如果无法解析时间戳，使用当前时间
		client.LastPong = time.Now().UnixMilli()
	}
}

// handleFileChunk 处理客户端发送的加密文件分片（MSG_SEND_FILE_CHUNK, 0x09）。
// 服务器不解密、不存储、不检查分片内容，仅验证传输状态后原样转发给房间内其他在线成员。
// 这是文件传输中最高频的消息（5MB 文件 = 80 次调用），性能路径需要尽量精简。
//
// 📚 学习要点: 零知识中转的数据流
// 客户端发送 SendFileChunkData → 服务器提取路由信息（transferId, index）→
// 附加发送方身份（senderId）→ 构建 RelayFileChunkData → BroadcastFileData 给房间成员。
// 服务器对 iv 和 data 字段完全不透明处理（直接转发原始字节），
// 无法推断文件内容、类型或任何有意义的信息。
//
// 📚 学习要点: 为什么使用 BroadcastFileData 而非普通 Broadcast？
// 普通 Broadcast 使用非阻塞 Send()，缓冲区满时静默丢弃消息。
// 文件分片丢失 = 接收方永远无法重组完整文件 → 传输必然失败。
// BroadcastFileData 使用带 5s 超时的阻塞发送（SendFileFunc），
// 确保分片要么成功进入接收方的 send buffer，要么明确超时失败。
// 同时使用并发 goroutine 发送，慢接收方不会拖住快接收方。
func (h *Hub) handleFileChunk(client *Client, data interface{}) {
	// 1. 验证客户端在房间中
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data map — msgpack 将消息体解码为 map[string]interface{}
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid file chunk data")
		return
	}

	// 3. 提取字段
	// transferId: 传输会话标识（明文字符串，用于路由和状态验证）
	transferId, _ := dataMap["transferId"].(string)
	if transferId == "" {
		h.sendError(client, ErrCodeInvalidMessage, "missing transferId")
		return
	}

	// 📚 学习要点: 使用 toInt() 解析 index 的必要性
	// vmihailenco/msgpack/v5 将小正整数解码为最小适配类型：
	//   0-127 → int8, 128-255 → uint8, 256-32767 → int16, ...
	// 直接 dataMap["index"].(int) 会 panic（类型不匹配）。
	// toInt() 统一处理所有整数类型，安全转换为 Go int。
	// 文件分片索引范围：0 到 79（5MB / 64KB = 80 chunks），
	// 所以 msgpack 会将其编码为 int8 或 uint8。
	index := toInt(dataMap["index"])

	// 📚 学习要点: []byte 在 msgpack 中的解码行为
	// msgpack bin 格式的数据在解码到 interface{} 时，
	// vmihailenco/msgpack/v5 会将其解码为 []byte 类型。
	// 这与 string 类型（msgpack str 格式）不同。
	// iv 和 data 字段在客户端使用 Uint8Array 发送，
	// msgpack 编码为 bin 格式，服务器端自然解码为 []byte。
	iv, _ := dataMap["iv"].([]byte)
	chunkData, _ := dataMap["data"].([]byte)

	// 4. 验证 iv 和 data 非空（基本完整性检查，不检查内容）
	if len(iv) == 0 || len(chunkData) == 0 {
		h.sendError(client, ErrCodeInvalidMessage, "iv and data must be non-empty")
		return
	}

	// 5. 验证 transferId 匹配客户端的活跃传输
	// 📚 学习要点: 为什么需要验证 activeTransferID？
	// 防止以下攻击/错误场景：
	// - 恶意客户端伪造 transferId，向其他人的传输注入垃圾数据
	// - 客户端 bug 导致发送了错误的 transferId
	// - 客户端在传输完成/取消后继续发送残留的 chunk
	// 服务器通过 activeTransferID 确保：只有正在进行活跃传输的客户端
	// 才能发送 chunk，且 chunk 的 transferId 必须与活跃传输匹配。
	if client.activeTransferID != transferId {
		h.sendError(client, ErrCodeInvalidMessage, "transferId does not match active transfer")
		return
	}

	// 6. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 7. 构建 RelayFileChunkData 并通过 BroadcastFileData 转发
	// 📚 学习要点: 服务器附加 senderId 的安全意义
	// senderId 由服务器从已认证的 Client 对象中提取（client.ID），
	// 而非信任客户端自行声明。这防止了身份伪造攻击：
	// 恶意客户端无法冒充其他用户发送文件分片。
	relayMsg := Message{
		Type: MsgRelayFileChunk,
		Data: RelayFileChunkData{
			SenderID:   client.ID,
			TransferID: transferId,
			Index:      index,
			IV:         iv,
			Data:       chunkData,
		},
	}

	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileChunk: %v", err)
		return
	}

	// 📚 学习要点: BroadcastFileData 的阻塞语义
	// 此调用会阻塞直到所有接收方的发送完成（或超时），最多 5s。
	// 这是可接受的，因为 handleFileChunk 在 Hub.HandleMessage 中被调用，
	// 而 HandleMessage 由 client.readPump 调用（每个客户端独立的 goroutine）。
	// 一个客户端的 BroadcastFileData 阻塞不会影响其他客户端的消息处理。
	r.BroadcastFileData(client.ID, broadcastData)
}

// handleFileAck 处理接收方发送的文件接收确认（MSG_SEND_FILE_ACK, 0x0C）。
// 接收方成功接收并重组完整文件后发送此确认。
// 服务器将此确认定向转发给原始发送方（非广播），
// 发送方据此更新 UI 显示 "已送达 (N/M)"。
//
// 📚 学习要点: ACK 的定向转发 vs 广播
// 与其他文件传输消息（Meta/Chunk/Complete/Cancel）的广播模式不同，
// ACK 只需要发送给原始发送方一人。原因：
// 1. ACK 的语义是「接收方 → 发送方」的确认，其他接收方不需要知道
// 2. 减少不必要的网络流量：N-2 个无关成员不会收到 ACK
// 3. 发送方需要统计 ACK 数量来显示送达状态，其他人不需要这个信息
//
// 📚 学习要点: 如何找到原始发送方？
// ACK 消息只包含 transferId，不包含 senderId（接收方可能不知道发送方的 client ID）。
// 服务器通过遍历同房间的所有客户端，找到 activeTransferID 匹配的那个客户端。
// 这依赖于：发送方的 activeTransferID 在收到所有 ACK 之前不会被清除。
// 如果找不到匹配的发送方（可能已断线或传输已超时清理），ACK 被静默丢弃。
func (h *Hub) handleFileAck(client *Client, data interface{}) {
	// 1. 验证客户端在房间中（发送 ACK 的是接收方）
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data map
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid file ack data")
		return
	}

	// 3. 提取 transferId
	transferId, _ := dataMap["transferId"].(string)
	if transferId == "" {
		h.sendError(client, ErrCodeInvalidMessage, "missing transferId")
		return
	}

	// 4. 查找房间（验证房间仍然存在）
	if h.roomManager.GetRoom(client.RoomID) == nil {
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 5. 在同房间的客户端中找到原始发送方
	// 📚 学习要点: 遍历查找发送方的策略
	// 方案选择：遍历 Hub 的 clients map，找到同房间且 activeTransferID 匹配的客户端。
	// 为什么不用 room.GetMembers()？
	// - Room.Member 结构体不包含 activeTransferID 字段（状态最小化原则）
	// - activeTransferID 是 Client 级别的状态，只有 Hub 层能访问
	// 为什么不在 SendFileAckData 中包含 senderId？
	// - 接收方可能不知道发送方的 client ID（虽然 RelayFileMeta 中有 senderId，
	//   但为了协议简洁性，ACK 只需要 transferId 就足够路由）
	// - 服务器端查找的开销很小（MaxMembers=50，遍历一次 O(N) 其中 N = 总连接数）
	//
	// 📚 学习要点: 读锁保护并发访问
	// h.clients map 可能被 Hub.Run() goroutine 修改（register/unregister），
	// 使用 RLock 确保遍历期间 map 不会被并发修改。
	// 注意：这里只读取 client 的字段（RoomID, activeTransferID），
	// 不修改任何状态，所以读锁足够。
	var sender *Client
	h.mu.RLock()
	for c := range h.clients {
		if c.RoomID == client.RoomID && (c.activeTransferID == transferId || c.lastCompletedTransferID == transferId) {
			sender = c
			break
		}
	}
	h.mu.RUnlock()

	// 6. 如果找不到发送方，静默丢弃 ACK
	// 📚 学习要点: 为什么静默丢弃而非返回错误？
	// 找不到发送方的可能原因：
	// - 发送方已断线（activeTransferID 被清理）
	// - 传输已超时（服务器端 90s 清理机制已清除 activeTransferID）
	// - 发送方已开始新的传输（activeTransferID 被覆盖）
	// 这些都是正常的边缘情况，不是接收方的错误。
	// 返回错误会让接收方困惑（"我明明收到了完整文件，为什么 ACK 失败？"）。
	// 发送方通过 ACK 缺失 + 超时来感知这种情况，不需要额外通知。
	if sender == nil {
		logger.Warn("Hub", "file ACK from %s: no sender found for transferId %s (sender may have disconnected)",
			client.ID, transferId)
		return
	}

	// 7. 构建 RelayFileAckData 并定向发送给原始发送方
	// 📚 学习要点: ReceiverID vs SenderID
	// RelayFileAckData 使用 ReceiverID（而非 SenderID），因为：
	// - 这条消息的方向是「接收方 → 服务器 → 发送方」
	// - 发送方需要知道「谁确认了接收」来更新送达计数
	// - 所以携带的是确认者（接收方）的 ID
	relayMsg := Message{
		Type: MsgRelayFileAck,
		Data: RelayFileAckData{
			ReceiverID: client.ID,
			TransferID: transferId,
		},
	}

	ackData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileAck: %v", err)
		return
	}

	// 定向发送给发送方（使用普通 Send，ACK 是轻量消息，丢失可接受）
	sender.Send(ackData)
}

// handleClientDisconnect 处理客户端断线，自动离开所在房间。
// 如果客户端有活跃的文件传输，先广播取消信号给房间成员，再执行离开逻辑。
//
// 📚 学习要点: 断线清理的顺序性
// 必须先广播 CANCEL 再离开房间，因为 handleLeaveRoom 会将客户端从房间成员列表中移除。
// 如果先离开再广播，BroadcastFileData 找不到房间或成员列表已变更，可能导致通知丢失。
func (h *Hub) handleClientDisconnect(client *Client) {
	if client.RoomID != "" {
		// 如果有活跃传输，广播 CANCEL 给房间成员（通知接收方释放缓冲区）
		if client.activeTransferID != "" {
			h.broadcastFileCancelForDisconnect(client)
			client.activeTransferID = ""
		}
		h.handleLeaveRoom(client, nil)
	}
}

// broadcastFileCancelForDisconnect 在客户端断线时广播文件传输取消信号。
// 通知房间内其他成员：发送方已断线，应丢弃该传输的分片缓冲区。
//
// 📚 学习要点: 孤立传输清理（Orphaned Transfer Cleanup）
// 正常流程中，传输通过 MSG_SEND_FILE_COMPLETE 或 MSG_SEND_FILE_CANCEL 结束。
// 但如果发送方突然断线（网络中断、浏览器崩溃），这些消息永远不会到达。
// 接收方会一直等待新 chunk，直到 60s 超时才标记失败。
// 服务器主动广播 CANCEL 可以让接收方立即释放资源，提升用户体验。
func (h *Hub) broadcastFileCancelForDisconnect(client *Client) {
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		return
	}

	// 构建 RelayFileCancel 消息
	relayMsg := Message{
		Type: MsgRelayFileCancel,
		Data: RelayFileCancelData{
			SenderID:   client.ID,
			TransferID: client.activeTransferID,
		},
	}
	data, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileCancel for disconnect: %v", err)
		return
	}

	// 使用普通 Broadcast（非 BroadcastFileData），因为 CANCEL 是轻量控制消息，
	// 不需要带超时的阻塞发送语义。
	r.Broadcast(client.ID, data)
	logger.Info("Hub", "broadcast file cancel for disconnected client %s, transferId=%s",
		client.ID, client.activeTransferID)
}

// cleanupStaleTransfers 扫描所有客户端，清理超过 serverTransferTimeout（90秒）的活跃传输。
// 由 Hub.Run() 中的 30 秒定时器周期性调用。
//
// 📚 学习要点: 兜底清理机制的必要性
// 正常情况下，文件传输通过以下方式结束：
//   - 发送方发送 MSG_SEND_FILE_COMPLETE → handleFileComplete 清除 activeTransferID
//   - 发送方发送 MSG_SEND_FILE_CANCEL → handleFileCancel 清除 activeTransferID
//   - 发送方断线 → handleClientDisconnect 清除 activeTransferID
//
// 但存在以下异常场景，上述机制都无法触发：
//   - 客户端进程崩溃但 TCP 连接未断开（TCP keepalive 默认 2 小时才检测到）
//   - 客户端 bug 导致发送了 Meta 但从未发送 Complete/Cancel
//   - 网络分区：客户端认为已发送 Complete，但消息在网络中丢失
//
// 如果不清理，该客户端的 activeTransferID 会永远非空，
// 导致其无法发起新的文件传输（handleFileMeta 中的并发检查会拒绝）。
//
// 📚 学习要点: RLock 读锁 vs Lock 写锁的选择
// 这里使用 RLock（读锁）遍历 clients map，因为：
// - 我们只读取 map 的键（遍历），不增删 map 条目
// - 修改的是 client 的字段（activeTransferID），不是 map 本身
// - 读锁允许与其他读操作（如 clientCount）并发执行
//
// 注意：修改 client.activeTransferID 不需要 map 的写锁，
// 因为 activeTransferID 是 Client 结构体的字段，不是 map 的键或值。
// Client 结构体的字段在当前架构中只被 Hub.Run() goroutine 修改
// （通过 handleFileMeta/handleFileComplete/handleFileCancel/handleClientDisconnect），
// 而 cleanupStaleTransfers 也在 Hub.Run() goroutine 中执行（通过 ticker），
// 所以不存在并发写入 activeTransferID 的风险。
func (h *Hub) cleanupStaleTransfers() {
	now := time.Now()

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		if client.activeTransferID != "" &&
			now.Sub(client.transferStartAt) > serverTransferTimeout {
			logger.Warn("Hub", "cleaning up stale transfer: client=%s, transferId=%s, age=%s",
				client.ID, client.activeTransferID, now.Sub(client.transferStartAt).Round(time.Second))
			client.activeTransferID = ""
		}
	}
}

// cleanupExpiredRooms 扫描并销毁所有已过期的房间。
// 由 Hub.Run() 中的 60 秒定时器周期性调用。
//
// 📚 学习要点: 防止 TOCTOU 竞态的双重检查模式（Double-Check Pattern）
// TOCTOU（Time-of-Check to Time-of-Use）是并发编程中的经典竞态条件：
// 在「检查条件」和「执行操作」之间，条件可能已经改变。
//
// 本方法的双重检查步骤：
//  1. GetExpiredRooms(now) 获取过期房间 ID 快照（RLock，快速返回）
//     → 这是第一次检查（Time-of-Check）
//  2. 对每个过期房间 ID:
//     a. GetRoom(id) — 如果返回 nil，说明已被并发删除（handleLeaveRoom 中所有人离开），跳过
//     b. 重新检查 room.IsExpired(now) — 这是第二次检查（Time-of-Use 前的再验证）
//     虽然 ExpiresAt 是只读字段不会变化，但双重检查是防御性编程的最佳实践，
//     防止未来代码变更引入 ExpiresAt 可修改的场景时出现 bug
//     c. 执行销毁操作（广播通知 → 清除状态 → 移除房间）
//
// 为什么 GetExpiredRooms 返回快照而非直接在锁内销毁？
// - 销毁操作涉及广播消息（可能阻塞）、遍历 clients map（需要另一把锁）
// - 如果在 RoomManager 的锁内执行这些操作，会导致锁持有时间过长
// - 快照模式：短暂持锁读取 → 释放锁 → 逐个处理（每个处理独立获取所需的锁）
func (h *Hub) cleanupExpiredRooms() {
	// 📚 学习要点: 使用 NowFunc 而非直接调用 time.Now()
	// 通过 RoomManager.NowFunc 获取当前时间，使集成测试可以注入可控时间源，
	// 测试"房间在运行中过期"的场景而无需等待真实的 60 秒。
	now := h.roomManager.NowFunc()

	// 步骤 1: 获取过期房间 ID 快照（RLock，不阻塞其他读操作）
	expiredRoomIDs := h.roomManager.GetExpiredRooms(now)
	if len(expiredRoomIDs) == 0 {
		return
	}

	// 步骤 2: 逐个处理过期房间
	for _, roomId := range expiredRoomIDs {
		// 2a. GetRoom 检查房间是否仍然存在
		// 房间可能在快照获取后被 handleLeaveRoom 销毁（所有成员离开）
		r := h.roomManager.GetRoom(roomId)
		if r == nil {
			continue
		}

		// 2b. 重新验证 room.IsExpired(now) — 防御性双重检查
		// 📚 学习要点: 为什么在 cleanupExpiredRooms 中重新检查 IsExpired？
		// GetExpiredRooms 返回的是某一时刻的快照。在遍历快照期间：
		// - 房间可能已被 handleLeaveRoom 销毁（所有人离开）→ GetRoom 返回 nil（上面已处理）
		// - ExpiresAt 当前是只读字段，理论上不会变化
		// - 但双重检查的成本极低（一次整数比较），而收益是防御性保护：
		//   如果未来引入「延长房间有效期」功能，此检查将防止误删
		if !r.IsExpired(now) {
			continue
		}

		// 2c. 处理房间内有活跃文件传输的客户端
		// 遍历 Hub 的 clients map，找到该房间内有 activeTransferID 的客户端
		h.mu.RLock()
		for client := range h.clients {
			if client.RoomID == roomId && client.activeTransferID != "" {
				h.broadcastFileCancelForExpiry(client, r)
				client.activeTransferID = ""
			}
		}
		h.mu.RUnlock()

		// 2d. 广播 MsgRoomClosed(reason="expired") 给房间所有成员
		roomClosedMsg := Message{
			Type: MsgRoomClosed,
			Data: RoomClosedData{Reason: "expired"},
		}
		closedData, err := msgpack.Marshal(roomClosedMsg)
		if err != nil {
			logger.Error("Hub", "failed to marshal RoomClosed for expired room %s: %v", roomId, err)
		} else {
			// 使用 GetMembers 获取所有成员并逐一发送（通知所有人，无排除）
			members := r.GetMembers()
			for _, m := range members {
				if m.SendFunc != nil {
					m.SendFunc(closedData)
				}
			}
		}

		// 2e. 清除所有成员的 client.RoomID（防止后续消息路由到已销毁房间）
		h.mu.RLock()
		for client := range h.clients {
			if client.RoomID == roomId {
				client.RoomID = ""
			}
		}
		h.mu.RUnlock()

		// 2f. Unregister from Hub directory (before removing room)
		if h.hubRegistry != nil {
			h.hubRegistry.Unregister(roomId)
		}
		h.activityTracker.Remove(roomId)

		// 从 RoomManager 中移除房间
		h.roomManager.RemoveRoom(roomId)

		// 清理过期预警记录
		delete(h.warnedExpiry, roomId)

		logger.Info("Hub", "expired room %s destroyed, total rooms: %d", roomId, h.roomManager.RoomCount())
	}
}

// warnApproachingExpiry 检查即将过期的房间（剩余 ≤ 5 分钟），向成员发送预警消息。
// 由 Hub.Run() 中的 expiryTicker 在 cleanupExpiredRooms 之后调用。
//
// 📚 学习要点: 过期预警的设计考量
// 房间过期时所有成员被立即踢出，正在输入的消息会丢失。
// 提前 5 分钟发送系统警告消息，让用户有心理准备保存重要信息。
// 使用 warnedExpiry map 确保每个房间只发送一次预警（幂等性）。
//
// 为什么不在客户端实现？
// 客户端已有 ExpiryCountdown 组件显示倒计时和警告色，但：
// 1. 客户端时钟可能不准确（偏差导致预警时机不一致）
// 2. 服务器端预警确保所有成员同时收到（一致的用户体验）
// 3. 预警消息出现在聊天流中，比 header 的倒计时更醒目
const expiryWarningThreshold int64 = 300 // 5 分钟

func (h *Hub) warnApproachingExpiry(now int64) {
	// 获取所有有过期时间的房间快照
	h.roomManager.ForEachExpiring(func(roomId string, expiresAt int64) {
		// 跳过已警告的房间
		if h.warnedExpiry[roomId] {
			return
		}

		remaining := expiresAt - now
		if remaining > 0 && remaining <= expiryWarningThreshold {
			// 发送预警消息给房间所有成员
			r := h.roomManager.GetRoom(roomId)
			if r == nil {
				return
			}

			warningMsg := Message{
				Type: MsgRelayMessage,
				Data: RelayMessageData{
					SenderID:   "system",
					SenderName: "System",
					IV:         "",
					Ciphertext: "__SYSTEM_EXPIRY_WARNING__",
					T:          now * 1000, // 转为毫秒与其他消息一致
				},
			}
			data, err := msgpack.Marshal(warningMsg)
			if err != nil {
				logger.Error("Hub", "failed to marshal expiry warning for room %s: %v", roomId, err)
				return
			}

			members := r.GetMembers()
			for _, m := range members {
				if m.SendFunc != nil {
					m.SendFunc(data)
				}
			}

			h.warnedExpiry[roomId] = true
			logger.Info("Hub", "sent expiry warning to room %s (remaining: %ds)", roomId, remaining)
		}
	})
}

// broadcastFileCancelForExpiry 在房间过期销毁时广播文件传输取消信号给所有成员。
// 通知房间内所有成员（包括发送方）：房间即将被销毁，应丢弃该传输的分片缓冲区。
//
// 📚 学习要点: broadcastFileCancelForExpiry vs broadcastFileCancelForDisconnect — 两种 FILE_CANCEL 广播的区别
//
// broadcastFileCancelForDisconnect（断线场景）：
//   - 触发条件：发送方断线（网络中断、浏览器崩溃）
//   - 广播范围：房间内其他成员（排除已断线的发送方，因为其连接已失效）
//   - 使用方式：r.Broadcast(client.ID, data) — 排除 senderId
//   - 目的：通知接收方释放分片缓冲区，发送方已无法继续传输
//
// broadcastFileCancelForExpiry（过期场景）：
//   - 触发条件：房间过期被 Expiry_Checker 销毁
//   - 广播范围：房间内所有成员（包括发送方），因为所有人都将被踢出
//   - 使用方式：遍历 room.GetMembers() 逐一发送（无排除）
//   - 目的：通知所有人（含发送方）传输已终止，发送方也需要清理本地传输状态
//
// 为什么过期场景需要通知发送方？
// 断线场景中发送方已经不在线，发送消息给它没有意义。
// 但过期场景中发送方仍然在线且可能正在发送 chunk，
// 它需要收到 CANCEL 信号来停止发送并清理本地状态（如关闭文件读取流）。
//
// Parameters:
//   - client: 拥有活跃传输的客户端（发送方）
//   - r: 即将被销毁的房间
func (h *Hub) broadcastFileCancelForExpiry(client *Client, r *room.Room) {
	// 构建 RelayFileCancel 消息
	relayMsg := Message{
		Type: MsgRelayFileCancel,
		Data: RelayFileCancelData{
			SenderID:   client.ID,
			TransferID: client.activeTransferID,
		},
	}
	data, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileCancel for expiry: %v", err)
		return
	}

	// 遍历房间所有成员逐一发送（包括发送方自己）
	// 📚 学习要点: 为什么不使用 r.Broadcast(excludeID, data)？
	// Broadcast 方法会排除 senderId，但过期场景需要通知所有人（含发送方）。
	// 因此使用 GetMembers() 获取完整成员列表并逐一调用 SendFunc。
	// 使用普通 SendFunc（非阻塞）而非 SendFileFunc（带超时阻塞），
	// 因为 CANCEL 是轻量控制消息，丢失可接受（房间即将被销毁，
	// 客户端最终会收到 MsgRoomClosed 通知）。
	members := r.GetMembers()
	for _, m := range members {
		if m.SendFunc != nil {
			m.SendFunc(data)
		}
	}

	logger.Info("Hub", "broadcast file cancel for expiry: room=%s, client=%s, transferId=%s",
		r.ID, client.ID, client.activeTransferID)
}

// --- 文件传输生命周期处理函数 ---
//
// 📚 学习要点: 文件传输的三阶段生命周期
// 文件传输遵循严格的三阶段生命周期：
//   1. Meta（开始）：发送方声明传输意图，服务器记录活跃状态，广播元数据
//   2. Chunk（传输中）：发送方逐片发送加密数据，服务器逐片转发（Task 2.4 实现）
//   3. Complete/Cancel（结束）：传输正常完成或被取消，服务器清除活跃状态
//
// 服务器在整个过程中保持零知识：
// - 不解密任何数据（iv、ciphertext、chunk data 都是不透明字节流）
// - 不存储任何文件内容（收到即转发，内存占用仅在写操作期间）
// - 仅追踪 transferId 和时间戳（用于限制并发和超时清理）

// handleFileMeta 处理客户端发送的加密文件元数据（MSG_SEND_FILE_META）。
// 这是文件传输的第一步：发送方声明传输意图，服务器验证后广播给房间成员。
//
// 📚 学习要点: 为什么需要服务器端验证？
// 虽然服务器不检查文件内容（零知识），但仍需验证：
// 1. 发送方确实在某个房间中（防止未加入房间的客户端发送文件）
// 2. 发送方没有其他活跃传输（限制每客户端 1 个并发传输，防止资源滥用）
// 这些验证不涉及文件内容，不违反零知识原则。
//
// 处理流程：
//  1. 验证客户端已加入房间（RoomID != ""）
//  2. 验证没有其他活跃传输（activeTransferID == ""）
//  3. 从 data map 提取 transferId、iv、ciphertext
//  4. 设置 activeTransferID 和 transferStartAt
//  5. 构建 RelayFileMetaData 并广播给房间其他成员
//
// 验证失败时的错误码：
//   - E003 (ErrCodeNotInRoom): 客户端未加入任何房间
//   - E005 (ErrCodeInvalidMessage): 已有活跃传输或数据格式错误
func (h *Hub) handleFileMeta(client *Client, data interface{}) {
	// 1. 验证客户端已加入房间
	// 📚 学习要点: 房间成员验证是所有消息处理的第一步
	// 未加入房间的客户端不应该能发送任何业务消息（聊天、文件、反应等）。
	// 这是一个安全边界：防止恶意客户端绕过加入流程直接发送数据。
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 验证没有其他活跃传输
	// 📚 学习要点: 单活跃传输限制（Per-Client Concurrency Control）
	// 每个客户端同时只能有 1 个活跃文件传输。这个限制的目的：
	// - 防止单个客户端占用过多服务器带宽（50 个成员 × 多个并发传输 = 带宽爆炸）
	// - 简化客户端状态管理（发送队列 FIFO，一次只处理一个）
	// - 降低接收方内存压力（同一发送方不会同时发送多个文件）
	if client.activeTransferID != "" {
		h.sendError(client, ErrCodeInvalidMessage, "already has active transfer")
		return
	}

	// 3. 解析 data map
	// 📚 学习要点: msgpack 解码后的类型断言
	// vmihailenco/msgpack/v5 将 msgpack map 解码为 map[string]interface{}。
	// 我们需要逐字段提取并进行类型断言。
	// 对于 []byte 字段（ciphertext），msgpack 的 bin 格式会解码为 []byte。
	// 对于 string 字段（transferId, iv），msgpack 的 str 格式会解码为 string。
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid file meta data")
		return
	}

	transferId, _ := dataMap["transferId"].(string)
	iv, _ := dataMap["iv"].(string)
	ciphertext, _ := dataMap["ciphertext"].([]byte)

	// 4. 验证必要字段非空
	if transferId == "" || iv == "" || len(ciphertext) == 0 {
		h.sendError(client, ErrCodeInvalidMessage, "transferId, iv, and ciphertext are required")
		return
	}

	// 5. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 6. 设置活跃传输状态
	// 📚 学习要点: 状态设置的时机
	// 在广播之前设置 activeTransferID，确保：
	// - 如果广播过程中客户端又发送了新的 Meta，会被步骤 2 拦截
	// - transferStartAt 记录传输开始时间，用于服务器端超时清理（90s 兜底）
	client.activeTransferID = transferId
	client.transferStartAt = time.Now()

	// 7. 构建 RelayFileMetaData 并广播
	// 📚 学习要点: 服务器附加身份信息
	// 服务器从已认证的 Client 对象中提取 senderId 和 senderName，
	// 而非信任客户端自行声明。这防止了身份伪造攻击：
	// 恶意客户端无法冒充其他用户发送文件。
	relayMsg := Message{
		Type: MsgRelayFileMeta,
		Data: RelayFileMetaData{
			SenderID:   client.ID,
			SenderName: client.Name,
			TransferID: transferId,
			IV:         iv,
			Ciphertext: ciphertext,
			T:          time.Now().UnixMilli(),
		},
	}
	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileMeta: %v", err)
		// 回滚活跃传输状态（序列化失败意味着广播不会发生）
		client.activeTransferID = ""
		return
	}

	// 使用 BroadcastFileData（带超时阻塞发送）而非普通 Broadcast
	// 📚 学习要点: 为什么 Meta 也用 BroadcastFileData？
	// Meta 消息虽然不大（通常 < 1KB），但它是传输的起点。
	// 如果 Meta 被普通 Broadcast 的非阻塞 Send 丢弃（缓冲区满），
	// 接收方永远不会知道有文件传输开始，后续 chunk 全部无意义。
	// 使用 BroadcastFileData 确保 Meta 有更高的送达保证。
	r.BroadcastFileData(client.ID, broadcastData)

	logger.Info("Hub", "file transfer started: client=%s, transferId=%s, room=%s",
		client.ID, transferId, client.RoomID)
}

// handleFileComplete 处理客户端发送的传输完成信号（MSG_SEND_FILE_COMPLETE）。
// 发送方在所有分片发送完毕后调用此函数，服务器清除活跃传输状态并广播完成信号。
//
// 📚 学习要点: Complete 信号的作用
// 接收方需要 Complete 信号来确认「所有 chunk 已发送完毕」：
// - 接收方可能因为网络延迟还没收到最后几个 chunk
// - Complete 信号告诉接收方「不会再有新 chunk 了」
// - 接收方收到 Complete 后检查是否所有 chunk 都已到齐
// - 如果到齐则重组文件，否则等待剩余 chunk（依赖 TCP 保序）
//
// 处理流程：
//  1. 验证客户端已加入房间
//  2. 从 data map 提取 transferId
//  3. 验证 transferId 匹配当前活跃传输
//  4. 清除 activeTransferID（允许发起新传输）
//  5. 构建 RelayFileCompleteData 并广播
func (h *Hub) handleFileComplete(client *Client, data interface{}) {
	// 1. 验证客户端已加入房间
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data map
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid file complete data")
		return
	}

	transferId, _ := dataMap["transferId"].(string)

	// 3. 验证 transferId 非空
	if transferId == "" {
		h.sendError(client, ErrCodeInvalidMessage, "transferId is required")
		return
	}

	// 4. 验证 transferId 匹配当前活跃传输
	// 📚 学习要点: 为什么需要匹配验证？
	// 防止以下攻击/错误场景：
	// - 恶意客户端发送伪造的 Complete 信号，试图中断其他人的传输
	// - 客户端 bug 导致发送了错误的 transferId
	// - 网络延迟导致旧传输的 Complete 在新传输开始后才到达
	// 只有 transferId 与 activeTransferID 完全匹配时才处理。
	if client.activeTransferID != transferId {
		h.sendError(client, ErrCodeInvalidMessage, "transferId does not match active transfer")
		return
	}

	// 5. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		client.activeTransferID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 6. 清除活跃传输状态（允许客户端发起新的传输）
	// 📚 学习要点: 保存 lastCompletedTransferID 用于 ACK 路由
	// ACK 在 Complete 之后才到达，此时 activeTransferID 已清除。
	// 保存到 lastCompletedTransferID 让 handleFileAck 仍能找到发送方。
	client.lastCompletedTransferID = client.activeTransferID
	client.activeTransferID = ""

	// 7. 构建 RelayFileCompleteData 并广播
	relayMsg := Message{
		Type: MsgRelayFileComplete,
		Data: RelayFileCompleteData{
			SenderID:   client.ID,
			TransferID: transferId,
		},
	}
	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileComplete: %v", err)
		return
	}

	// Complete 是轻量控制消息，使用 BroadcastFileData 确保送达
	// 📚 学习要点: 为什么 Complete 也用 BroadcastFileData？
	// Complete 信号对接收方至关重要：没有它，接收方不知道传输已结束。
	// 如果 Complete 被丢弃，接收方会一直等待新 chunk 直到 60s 超时。
	// 使用 BroadcastFileData 提供更高的送达保证，避免不必要的超时等待。
	r.BroadcastFileData(client.ID, broadcastData)

	logger.Info("Hub", "file transfer completed: client=%s, transferId=%s, room=%s",
		client.ID, transferId, client.RoomID)
}

// handleFileCancel 处理客户端发送的取消传输信号（MSG_SEND_FILE_CANCEL）。
// 发送方主动取消正在进行的文件传输（如用户点击取消按钮）。
// 服务器清除活跃传输状态并广播取消信号，接收方据此释放分片缓冲区。
//
// 📚 学习要点: Cancel 与 Complete 的对称性
// Cancel 和 Complete 都是传输的终止信号，处理逻辑几乎相同：
// 1. 验证 transferId 匹配
// 2. 清除 activeTransferID
// 3. 广播给房间成员
// 区别仅在于接收方的处理：Complete → 重组文件，Cancel → 丢弃缓冲区。
// 这种对称设计使得代码结构清晰，状态转换可预测。
//
// 处理流程：
//  1. 验证客户端已加入房间
//  2. 从 data map 提取 transferId
//  3. 验证 transferId 匹配当前活跃传输
//  4. 清除 activeTransferID
//  5. 构建 RelayFileCancelData 并广播
func (h *Hub) handleFileCancel(client *Client, data interface{}) {
	// 1. 验证客户端已加入房间
	if client.RoomID == "" {
		h.sendError(client, ErrCodeNotInRoom, "not in a room")
		return
	}

	// 2. 解析 data map
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		h.sendError(client, ErrCodeInvalidMessage, "invalid file cancel data")
		return
	}

	transferId, _ := dataMap["transferId"].(string)

	// 3. 验证 transferId 非空
	if transferId == "" {
		h.sendError(client, ErrCodeInvalidMessage, "transferId is required")
		return
	}

	// 4. 验证 transferId 匹配当前活跃传输
	// 📚 学习要点: 幂等性考虑
	// 如果客户端发送了重复的 Cancel（网络重试），第二次到达时
	// activeTransferID 已被清除（== ""），不会匹配任何 transferId。
	// 此时返回错误是正确的行为：告诉客户端「没有需要取消的传输」。
	if client.activeTransferID != transferId {
		h.sendError(client, ErrCodeInvalidMessage, "transferId does not match active transfer")
		return
	}

	// 5. 查找房间
	r := h.roomManager.GetRoom(client.RoomID)
	if r == nil {
		client.RoomID = ""
		client.activeTransferID = ""
		h.sendError(client, ErrCodeNotInRoom, "room no longer exists")
		return
	}

	// 6. 清除活跃传输状态
	client.activeTransferID = ""

	// 7. 构建 RelayFileCancelData 并广播
	relayMsg := Message{
		Type: MsgRelayFileCancel,
		Data: RelayFileCancelData{
			SenderID:   client.ID,
			TransferID: transferId,
		},
	}
	broadcastData, err := msgpack.Marshal(relayMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal RelayFileCancel: %v", err)
		return
	}

	// Cancel 是轻量控制消息，使用 BroadcastFileData 确保送达
	r.BroadcastFileData(client.ID, broadcastData)

	logger.Info("Hub", "file transfer cancelled: client=%s, transferId=%s, room=%s",
		client.ID, transferId, client.RoomID)
}

// --- 辅助方法 ---

// sendError 向客户端发送错误消息。
func (h *Hub) sendError(client *Client, code string, msg string) {
	errMsg := Message{
		Type: MsgError,
		Data: ErrorData{
			Code: code,
			Msg:  msg,
		},
	}
	data, err := msgpack.Marshal(errMsg)
	if err != nil {
		logger.Error("Hub", "failed to marshal error message: %v", err)
		return
	}
	client.Send(data)
}

// sendMatchError sends a match-protocol formatted error to the client.
// Used when the match feature is disabled (matchServer is nil) and the Hub
// receives a message in the 0x20-0x2F range. Uses the same {type, data} envelope
// format as all other server messages for client compatibility.
func (h *Hub) sendMatchError(client *Client, code string, msg string) {
	envelope := Message{
		Type: match.MsgMatchError,
		Data: match.MatchErrorData{
			Code: code,
			Msg:  msg,
		},
	}
	data, err := msgpack.Marshal(envelope)
	if err != nil {
		logger.Error("Hub", "failed to marshal match error message: %v", err)
		return
	}
	client.Send(data)
}

// sendToClient 向客户端发送指定类型的消息。
func (h *Hub) sendToClient(client *Client, msgType uint8, msgData interface{}) {
	msg := Message{Type: msgType, Data: msgData}
	data, err := msgpack.Marshal(msg)
	if err != nil {
		logger.Error("Hub", "failed to marshal message: %v", err)
		return
	}
	client.Send(data)
}

// clientCount 返回当前连接的客户端数量（需在持有锁或调用后使用）。
func (h *Hub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ClientCount returns the number of currently connected WebSocket clients.
// Safe for concurrent use (acquires read lock internally).
func (h *Hub) ClientCount() int {
	return h.clientCount()
}

// ActivityTracker returns the Hub's activity tracker instance.
// Used to pass the tracker as an ActivityGetter to the Hub API handler.
func (h *Hub) ActivityTracker() *activity.Tracker {
	return h.activityTracker
}

// generateColor 根据客户端 ID 生成一个十六进制颜色值。
// 使用简单的 hash 算法将 ID 映射到颜色空间。
func generateColor(id string) string {
	var hash uint32
	for _, c := range id {
		hash = hash*31 + uint32(c)
	}
	// 生成 RGB 颜色，确保颜色不会太暗（每个通道至少 64）
	r := (hash>>16)&0xFF | 0x40
	g := (hash>>8)&0xFF | 0x40
	b := hash&0xFF | 0x40
	// 限制到 0xFF
	if r > 0xFF {
		r = 0xFF
	}
	if g > 0xFF {
		g = 0xFF
	}
	if b > 0xFF {
		b = 0xFF
	}
	return fmt.Sprintf("#%02x%02x%02x", r, g, b)
}

// toInt converts a msgpack-decoded interface{} value to int.
// vmihailenco/msgpack/v5 decodes integers into the smallest Go type that fits:
// 0-127 → int8, 128-255 → uint8, 256-32767 → int16, etc.
func toInt(v interface{}) int {
	switch n := v.(type) {
	case int8:
		return int(n)
	case uint8:
		return int(n)
	case int16:
		return int(n)
	case uint16:
		return int(n)
	case int32:
		return int(n)
	case uint32:
		return int(n)
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case int:
		return n
	case uint:
		return int(n)
	default:
		return 0
	}
}
