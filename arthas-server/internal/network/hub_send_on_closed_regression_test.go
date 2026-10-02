package network

import (
	"sync"
	"testing"
	"time"

	"github.com/arthas/arthas-server/internal/room"
)

// TestRegression_StalledReceiverDisconnect_NoSendOnClosedChannel 是针对
// 「任意未认证远程可致服务进程崩溃（panic: send on closed channel）」的回归测试。
//
// 原始缺陷链路：
//  1. 一个接收方停止读取自己的 socket（例如客户端挂起 / 恶意连接）；
//  2. 发送方持续发送文件分片 → room.BroadcastFileData 在独立 goroutine 中
//     对每个接收方调用 SendFileFunc → SendFileData 做带 5s 超时的**阻塞发送**；
//  3. 接收方 send buffer（256）被填满，SendFileData 卡在 `c.send <- data` 上；
//  4. 与此同时接收方的 readPump 检测到断线并注销 → Hub.Run() 执行
//     close(receiver.send)；
//  5. 仍在阻塞中的 SendFileData 向**已关闭的通道**发送 → panic，
//     整个进程崩溃，所有房间一并丢失。
//
// 关键点是第 3 步与第 4 步**并发**：无论怎样调整「先检查再关闭」的顺序，
// 都无法消除这个窗口。修复方案是不再关闭 send 通道（改用 done 通道通知）。
//
// 本测试在修复前必然 panic（send on closed channel），修复后应稳定通过。
// 它同时是一个 goroutine 竞态检测用例：配合 `go test -race` 运行价值最大。
func TestRegression_StalledReceiverDisconnect_NoSendOnClosedChannel(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	t.Cleanup(func() {
		// 兜底：异常路径下确保 Hub 被关闭（Stop 现在不会 panic）。
		hub.Stop()
	})
	time.Sleep(10 * time.Millisecond)

	r := hub.roomManager.CreateRoom("room-stall", "", 0, 0, 0)

	// 发送方（会被 BroadcastFileData 排除）。
	sender := newLocalTestClient(hub, "sender", r.ID)
	senderMember := &room.Member{
		ID:           sender.ID,
		SendFunc:     func(data []byte) {},
		SendFileFunc: func(data []byte) bool { return true },
	}
	if err := r.AddMember(senderMember); err != nil {
		t.Fatalf("add sender: %v", err)
	}

	// 停滞的接收方：注册到 Hub，但从不消费自己的 send 通道。
	// 模拟「客户端停止读取 socket」——其 readPump 会触发注销。
	victim := newLocalTestClient(hub, "victim", r.ID)
	victimMember := &room.Member{
		ID:           victim.ID,
		SendFunc:     func(data []byte) { victim.Send(data) },
		SendFileFunc: func(data []byte) bool { return victim.SendFileData(data) },
	}
	if err := r.AddMember(victimMember); err != nil {
		t.Fatalf("add victim: %v", err)
	}

	// 并发：一边持续向停滞接收方广播文件分片（填满其缓冲并阻塞在发送上），
	// 一边让接收方注销（触发通道关闭）。
	const chunks = 400 // 远大于 sendBufferSize(256)，保证进入阻塞分支
	payload := make([]byte, 1024)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < chunks; i++ {
			r.BroadcastFileData(sender.ID, payload)
		}
	}()

	// 给广播一点时间填满 victim 的缓冲区，然后注销它。
	time.Sleep(50 * time.Millisecond)
	select {
	case hub.unregister <- victim:
	case <-hub.done:
	}

	// 修复前：上面的 unregister 触发 close(victim.send)，
	// 仍在阻塞的 SendFileData 会 panic，进程在此崩溃。
	// 修复后：BroadcastFileData 或因 done 关闭而提前返回 false，
	// 不会 panic。等待广播循环自然结束即可。
	wg.Wait()

	// 稳定性检查：Hub 仍可正常工作 —— 新客户端能注册、房间仍在。
	alive := newLocalTestClient(hub, "alive", r.ID)
	select {
	case hub.register <- alive:
	case <-hub.done:
		t.Fatal("hub 已停止，说明发生了致命错误")
	}
	time.Sleep(20 * time.Millisecond)

	hub.mu.RLock()
	_, ok := hub.clients[alive]
	hub.mu.RUnlock()
	if !ok {
		t.Error("注销停滞接收方后，Hub 不应停止接受新客户端")
	}
}

// newLocalTestClient 构造一个不启动真实 WebSocket 连接的 Client，
// 直接注册进 Hub，用于就地验证 send/close 的并发行为。
// done 通道必须初始化，否则 closeSend() 将无从通知 writePump。
func newLocalTestClient(hub *Hub, id, roomID string) *Client {
	c := &Client{
		ID:     id,
		RoomID: roomID,
		Name:   id,
		hub:    hub,
		send:   make(chan []byte, sendBufferSize),
		done:   make(chan struct{}),
	}
	hub.mu.Lock()
	hub.clients[c] = true
	hub.mu.Unlock()
	return c
}
