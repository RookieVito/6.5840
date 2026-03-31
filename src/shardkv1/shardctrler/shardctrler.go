package shardctrler

//
// shardctrler：提供 InitConfig、Query 和 ChangeConfigTo
//

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	curCfgKey  string = "curCfg"
	nextCfgKey string = "nextCfg"
	leaseKey   string = "ctrlerLease"
)

const (
	retryInterval      = 20 * time.Millisecond
	leaseTTL           = 800 * time.Millisecond
	leaseRenewInterval = 200 * time.Millisecond
)

// ShardCtrler 表示分片控制器及其底层 kv clerk。
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	// id 用于唯一标识一个 controller 实例，便于在 kvsrv 中竞争租约。
	id string

	killed int32 // 由 Kill() 设置
}

type leaseState struct {
	Holder           string `json:"holder"`
	Epoch            uint64 `json:"epoch"`
	DeadlineUnixNano int64  `json:"deadline_unix_nano"`
}

type leaseSession struct {
	sck   *ShardCtrler
	epoch uint64

	stopCh chan struct{}
	doneCh chan struct{}

	// deadline/lost 是本地缓存的租约状态；真正的事实来源仍然在 kvsrv 中。
	// 这些字段用于让长时间运行的迁移循环在失去领导权后尽快退出。
	mu       sync.RWMutex
	deadline time.Time
	lost     bool
}

var nextCtrlerID uint64

// MakeShardCtrler 创建一个 shard controller，并把状态存到 kvsrv 中。
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt: clnt,
		id:   fmt.Sprintf("ctrler-%d", atomic.AddUint64(&nextCtrlerID, 1)),
	}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

func (st leaseState) String() string {
	b, err := json.Marshal(st)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func leaseFromString(s string) leaseState {
	if s == "" {
		return leaseState{}
	}
	var st leaseState
	if err := json.Unmarshal([]byte(s), &st); err != nil {
		panic(err)
	}
	return st
}

func (st leaseState) activeAt(now time.Time) bool {
	return st.Holder != "" && now.UnixNano() < st.DeadlineUnixNano
}

func (st leaseState) heldBy(holder string, epoch uint64) bool {
	return st.Holder == holder && st.Epoch == epoch
}

func (ls *leaseSession) setDeadline(deadline time.Time) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.deadline = deadline
}

func (ls *leaseSession) markLost() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.lost = true
}

func (ls *leaseSession) active() bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return !ls.lost && time.Now().Before(ls.deadline)
}

func (ls *leaseSession) deadlineCopy() time.Time {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.deadline
}

func (ls *leaseSession) close() {
	close(ls.stopCh)
	<-ls.doneCh
	// 尽力释放租约，避免下一个 controller 总要等到 TTL 自然过期。
	if ls.active() {
		ls.sck.releaseLease(ls)
	}
}

// tester 在启动一个新的 controller 前会调用 InitController。
// 5B/5C 中，这里负责接管未完成的配置迁移。
func (sck *ShardCtrler) InitController() {
	lease := sck.acquireLease()
	defer lease.close()

	for lease.active() {
		// 恢复逻辑仍沿用 5B 的 curCfg/nextCfg 状态机；5C 只是在外层
		// 增加了领导权租约与 fencing。
		cur := sck.readConfigWithLease(lease, curCfgKey)
		next := sck.readConfigWithLease(lease, nextCfgKey)
		if cur == nil || next == nil {
			return
		}
		if next.Num <= cur.Num {
			return
		}
		sck.finishConfigChange(lease, cur, next)
	}
}

// InitConfig 由 tester 调用一次，用来写入初始配置。
// 这里同时初始化 committed config 和 pending config，保证后续恢复
// 总是从一致的基础状态开始。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.ensureConfig(curCfgKey, cfg)
	sck.ensureConfig(nextCfgKey, cfg)
}

// ensureConfig 把 cfg 写到指定 key；如果 Put 返回 ErrMaybe，则通过后续
// Get 确认目标值是否已经落盘，若是则视为成功。
func (sck *ShardCtrler) ensureConfig(key string, cfg *shardcfg.ShardConfig) {
	want := cfg.String()
	// 用 CAS 语义写入 kvsrv。
	for {
		val, ver, err := sck.Get(key)
		if err == rpc.OK {
			if val == want {
				return
			}
			err = sck.Put(key, want, ver)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				got, _, getErr := sck.Get(key)
				if getErr == rpc.OK && got == want {
					return
				}
			}
			time.Sleep(retryInterval)
			continue
		}
		if err == rpc.ErrNoKey {
			err = sck.Put(key, want, rpc.Tversion(0))
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				got, _, getErr := sck.Get(key)
				if getErr == rpc.OK && got == want {
					return
				}
			}
			time.Sleep(retryInterval)
			continue
		}
		time.Sleep(retryInterval)
	}
}

// readConfig 会一直阻塞，直到从 controller 使用的 kvsrv 中读到并解码出配置。
func (sck *ShardCtrler) readConfig(key string) *shardcfg.ShardConfig {
	for {
		val, _, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(retryInterval)
			continue
		}
		return shardcfg.FromString(val)
	}
}

func (sck *ShardCtrler) readConfigWithLease(lease *leaseSession, key string) *shardcfg.ShardConfig {
	// 分区中的 controller 可能会在读配置时卡一段时间；一旦租约失效，
	// 必须立即停止，而不是在网络恢复后继续以过期身份执行。
	for lease.active() {
		val, _, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(retryInterval)
			continue
		}
		return shardcfg.FromString(val)
	}
	return nil
}

// tryAdvanceConfig 用 CAS 的方式把持久化配置从 from 推进到 to。
// 如果发现别的 controller 已经把该 key 改成了冲突值，则返回 false。
func (sck *ShardCtrler) tryAdvanceConfig(key string, from, to *shardcfg.ShardConfig) bool {
	want := to.String()
	for {
		val, ver, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(retryInterval)
			continue
		}
		cur := shardcfg.FromString(val)
		if cur.Num >= to.Num {
			return true
		}
		if cur.Num != from.Num || val != from.String() {
			return false
		}
		err = sck.Put(key, want, ver)
		if err == rpc.OK {
			return true
		}
		if err == rpc.ErrMaybe {
			got, _, getErr := sck.Get(key)
			if getErr == rpc.OK {
				if got == want {
					return true
				}
				if got != from.String() {
					return false
				}
			}
		}
		time.Sleep(retryInterval)
	}
}

func (sck *ShardCtrler) tryAdvanceConfigWithLease(lease *leaseSession, key string, from, to *shardcfg.ShardConfig) bool {
	want := to.String()
	for lease.active() {
		val, ver, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(retryInterval)
			continue
		}
		cur := shardcfg.FromString(val)
		if cur.Num >= to.Num {
			return true
		}
		if cur.Num != from.Num || val != from.String() {
			return false
		}
		// kvsrv 上的 CAS 保证配置写入线性化；额外的 lease 检查用于阻止
		// 过期 controller 在失去领导权后继续无限重试。
		err = sck.Put(key, want, ver)
		if err == rpc.OK {
			return true
		}
		if err == rpc.ErrMaybe {
			got, _, getErr := sck.Get(key)
			if getErr == rpc.OK {
				if got == want {
					return true
				}
				if got != from.String() {
					return false
				}
			}
		}
		time.Sleep(retryInterval)
	}
	return false
}

// configSuperseded 判断目标配置是否已经提交，或已被更新的 pending 配置取代。
func (sck *ShardCtrler) configSuperseded(target shardcfg.Tnum) bool {
	cur := sck.readConfig(curCfgKey)
	if cur.Num >= target {
		return true
	}
	next := sck.readConfig(nextCfgKey)
	return next.Num > target
}

func (sck *ShardCtrler) configSupersededWithLease(lease *leaseSession, target shardcfg.Tnum) bool {
	// 如果当前 controller 已经失去租约，那么从它自己的视角看，
	// 这个目标配置就应当视为已被接管，以便迁移循环尽快退出。
	cur := sck.readConfigWithLease(lease, curCfgKey)
	if cur == nil || cur.Num >= target {
		return true
	}
	next := sck.readConfigWithLease(lease, nextCfgKey)
	return next == nil || next.Num > target
}

func (sck *ShardCtrler) readLease() (leaseState, rpc.Tversion, rpc.Err) {
	val, ver, err := sck.Get(leaseKey)
	if err == rpc.ErrNoKey {
		return leaseState{}, 0, err
	}
	if err != rpc.OK {
		return leaseState{}, 0, err
	}
	return leaseFromString(val), ver, rpc.OK
}

func (sck *ShardCtrler) writeLease(ver rpc.Tversion, from, to leaseState) bool {
	want := to.String()
	err := sck.Put(leaseKey, want, ver)
	if err == rpc.OK {
		return true
	}
	if err == rpc.ErrMaybe {
		got, _, getErr := sck.Get(leaseKey)
		if getErr == rpc.OK {
			if got == want {
				return true
			}
			if got != from.String() {
				return false
			}
		}
	}
	return false
}

func (sck *ShardCtrler) acquireLease() *leaseSession {
	for {
		now := time.Now()
		cur, ver, err := sck.readLease()
		if err != rpc.OK && err != rpc.ErrNoKey {
			time.Sleep(retryInterval)
			continue
		}
		if cur.activeAt(now) && cur.Holder != sck.id {
			time.Sleep(retryInterval)
			continue
		}

		// Epoch 是 fencing token：一旦其他 controller 安装了更高的 epoch，
		// 旧 leader 就不能再续租或释放租约。
		next := leaseState{
			Holder:           sck.id,
			Epoch:            cur.Epoch + 1,
			DeadlineUnixNano: now.Add(leaseTTL).UnixNano(),
		}
		if cur.Holder == sck.id && cur.activeAt(now) {
			next.Epoch = cur.Epoch
		}
		if !sck.writeLease(ver, cur, next) {
			time.Sleep(retryInterval)
			continue
		}

		lease := &leaseSession{
			sck:      sck,
			epoch:    next.Epoch,
			stopCh:   make(chan struct{}),
			doneCh:   make(chan struct{}),
			deadline: time.Unix(0, next.DeadlineUnixNano),
		}
		go sck.renewLeaseLoop(lease)
		return lease
	}
}

func (sck *ShardCtrler) renewLeaseLoop(lease *leaseSession) {
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()
	defer close(lease.doneCh)

	for {
		select {
		case <-lease.stopCh:
			return
		case <-ticker.C:
			if !lease.active() {
				lease.markLost()
				return
			}
			// 一旦续租失败，当前 controller 必须停止修改 controller 状态和
			// shard 状态，因为替代 leader 可能已经接管。
			if !sck.refreshLease(lease) {
				lease.markLost()
				return
			}
		}
	}
}

func (sck *ShardCtrler) refreshLease(lease *leaseSession) bool {
	for time.Now().Before(lease.deadlineCopy()) {
		now := time.Now()
		cur, ver, err := sck.readLease()
		if err == rpc.OK {
			if !cur.heldBy(sck.id, lease.epoch) {
				return false
			}
			if !cur.activeAt(now) {
				return false
			}

			// 续租只允许延长 deadline；holder/epoch 必须保持不变，
			// 否则说明当前 controller 已经被 fencing 掉。
			next := cur
			next.DeadlineUnixNano = now.Add(leaseTTL).UnixNano()
			if sck.writeLease(ver, cur, next) {
				lease.setDeadline(time.Unix(0, next.DeadlineUnixNano))
				return true
			}
		}

		select {
		case <-lease.stopCh:
			return true
		case <-time.After(retryInterval):
		}
	}
	return false
}

func (sck *ShardCtrler) releaseLease(lease *leaseSession) {
	for attempts := 0; attempts < 3; attempts++ {
		cur, ver, err := sck.readLease()
		if err != rpc.OK {
			return
		}
		if !cur.heldBy(sck.id, lease.epoch) {
			return
		}
		released := leaseState{
			Epoch: cur.Epoch,
		}
		if sck.writeLease(ver, cur, released) {
			return
		}
		time.Sleep(retryInterval)
	}
}

// getShardClerkFactory 按 gid 缓存 shardgrp clerk，便于多个迁移协程安全复用。
func (sck *ShardCtrler) getShardClerkFactory() func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	var mu sync.Mutex
	return func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := clerks[gid]; !ok {
			clerks[gid] = shardgrp.MakeClerk(sck.clnt, srvs)
		}
		return clerks[gid]
	}
}

// moveShard 执行单个 shard 的迁移协议。
// 这套流程被设计成可重试的，因此恢复后的 controller 能安全接管未完成迁移。
func (sck *ShardCtrler) moveShard(lease *leaseSession, old, new *shardcfg.ShardConfig, shard shardcfg.Tshid, getClerk func(gid tester.Tgid, srvs []string) *shardgrp.Clerk) {
	oldGid := old.Shards[shard]
	newGid := new.Shards[shard]
	if oldGid == newGid {
		return
	}

	if oldGid == 0 {
		newClerk := getClerk(newGid, new.Groups[newGid])
		for lease.active() {
			err := newClerk.InstallShard(shard, nil, new.Num)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrWrongGroup && sck.configSupersededWithLease(lease, new.Num) {
				return
			}
			time.Sleep(retryInterval)
		}
		return
	}

	oldClerk := getClerk(oldGid, old.Groups[oldGid])
	if newGid == 0 {
		for lease.active() {
			_, err := oldClerk.FreezeShard(shard, new.Num)
			if err != rpc.OK {
				if err == rpc.ErrWrongGroup && sck.configSupersededWithLease(lease, new.Num) {
					return
				}
				time.Sleep(retryInterval)
				continue
			}
			break
		}
		for lease.active() {
			err := oldClerk.DeleteShard(shard, new.Num)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrWrongGroup && sck.configSupersededWithLease(lease, new.Num) {
				return
			}
			time.Sleep(retryInterval)
		}
		return
	}

	newClerk := getClerk(newGid, new.Groups[newGid])
	var state []byte
	for lease.active() {
		var err rpc.Err
		// 恢复中的 controller 可能会重复执行 Freeze；由于 shardgrp 侧是
		// 幂等的，这么做是安全的。
		state, err = oldClerk.FreezeShard(shard, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup {
			if sck.configSupersededWithLease(lease, new.Num) {
				return
			}
			state = nil
			break
		}
		time.Sleep(retryInterval)
	}

	for lease.active() {
		err := newClerk.InstallShard(shard, state, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup && sck.configSupersededWithLease(lease, new.Num) {
			return
		}
		time.Sleep(retryInterval)
	}

	for lease.active() {
		err := oldClerk.DeleteShard(shard, new.Num)
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrWrongGroup && sck.configSupersededWithLease(lease, new.Num) {
			return
		}
		time.Sleep(retryInterval)
	}
}

// finishConfigChange 负责迁移 old 与 new 之间所有发生变化的 shard，
// 然后再把 new 提交为当前配置。
func (sck *ShardCtrler) finishConfigChange(lease *leaseSession, old, new *shardcfg.ShardConfig) {
	if old.Num >= new.Num || !lease.active() {
		return
	}

	getClerk := sck.getShardClerkFactory()
	var wg sync.WaitGroup
	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		if old.Shards[shard] == new.Shards[shard] {
			continue
		}
		wg.Add(1)
		go func(shard shardcfg.Tshid) {
			defer wg.Done()
			sck.moveShard(lease, old, new, shard, getClerk)
		}(shard)
	}
	wg.Wait()
	if !lease.active() {
		return
	}
	// 只有在所有 shard 迁移完成、且当前仍持有租约时，才提交新配置。
	sck.tryAdvanceConfigWithLease(lease, curCfgKey, old, new)
}

// ChangeConfigTo 由 tester 调用，用来把配置从当前值推进到 new。
// 执行过程中当前 controller 可能被其他 controller 取代，因此所有关键步骤
// 都必须受租约保护。
// 流程是：先把目标配置记到 nextCfg，再完成迁移，最后推进 curCfg。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	lease := sck.acquireLease()
	defer lease.close()

	for lease.active() {
		cur := sck.readConfigWithLease(lease, curCfgKey)
		if cur == nil {
			return
		}
		if cur.Num >= new.Num {
			return
		}

		next := sck.readConfigWithLease(lease, nextCfgKey)
		if next == nil {
			return
		}
		if next.Num < cur.Num {
			sck.tryAdvanceConfigWithLease(lease, nextCfgKey, next, cur)
			continue
		}
		if next.Num > cur.Num {
			// 说明之前的 controller 留下了未完成任务；当前租约持有者必须先
			// 把它收尾，再尝试推进更新的配置。
			sck.finishConfigChange(lease, cur, next)
			continue
		}
		if cur.Num+1 != new.Num {
			time.Sleep(retryInterval)
			continue
		}
		if !sck.tryAdvanceConfigWithLease(lease, nextCfgKey, cur, new) {
			continue
		}
		// 先把目标记到 nextCfg，再执行迁移，最后提交到 curCfg，
		// 这样才能保持 5B 的恢复语义不变。
		sck.finishConfigChange(lease, cur, new)
		return
	}
}

// Query 返回最新已提交的配置。
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	return sck.readConfig(curCfgKey)
}
