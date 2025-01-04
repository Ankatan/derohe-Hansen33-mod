package rpc

import (
	"flag"
	"fmt"
	"net/http"
	"sort"
	"time"

	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/blockchain"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/p2p"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/graviton"
	"github.com/go-logr/logr"
	"github.com/lesismal/llib/std/crypto/tls"
	"github.com/lesismal/nbio"
	"github.com/lesismal/nbio/logging"
	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

// this file implements the non-blocking job streamer
// only job is to stream jobs to thousands of workers, if any is successful,accept and report back

var memPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 16*1024)
	},
}

var logger_getwork logr.Logger
var (
	svr   *nbhttp.Server
	print = flag.Bool("print", false, "stdout output of echoed data")
)

type user_session struct {
	blocks        uint64
	miniblocks    uint64
	rejected      uint64
	orphans       uint64
	lasterr       string
	address       rpc.Address
	valid_address bool
	address_sum   [32]byte
	hashrate      float64
	tag           string
}

type banned struct {
	fail_count uint8
	timestamp  time.Time
}

var client_list_mutex sync.Mutex
var client_list = map[*websocket.Conn]*user_session{}
var ban_list = make(map[string]banned)

var miners_count int

// this will track miniblock rate,
var mini_found_time []int64 // this array contains a epoch timestamp in int64
var rate_lock sync.Mutex

// this function will return wrong result if too wide time glitches happen to system clock
func Counter(seconds int64) (r int) { // we need atleast 1 mini to find a rate
	rate_lock.Lock()
	defer rate_lock.Unlock()
	length := len(mini_found_time)
	if length > 0 {
		start_point := time.Now().Unix() - seconds
		i := sort.Search(length, func(i int) bool { return mini_found_time[i] >= start_point })
		if i < len(mini_found_time) {
			r = length - i
		}
	}
	return // return 0
}

func cleanup() {
	rate_lock.Lock()
	defer rate_lock.Unlock()
	length := len(mini_found_time)
	if length > 0 {
		start_point := time.Now().Unix() - 30*24*3600 // only keep data of last 30 days
		i := sort.Search(length, func(i int) bool { return mini_found_time[i] >= start_point })
		if i > 1000 && i < length {
			mini_found_time = append(mini_found_time[:0], mini_found_time[i:]...) // renew the array
		}
	}
}

// this will calcuate amount of hashrate based on the number of minis
// note this calculation is very crude
// note it will always be lagging, since NW conditions are quite dynamic
// this is  used to roughly estimate your hash rate on this integrator of all miners
// note this is a moving avg
func HashrateEstimatePercent(timeframe int64) float64 {
	return float64(Counter(timeframe)*100) / (float64(timeframe*10) / float64(config.BLOCK_TIME))
}

// note this will be be 0, if you have less than 1/48000 hash power
func HashrateEstimatePercent_1hr() float64 {
	return HashrateEstimatePercent(3600)
}

// note result will be 0, if you have  less than 1/2000 hash power
func HashrateEstimatePercent_1day() float64 {
	return HashrateEstimatePercent(24 * 3600)
}

// note this will be 0, if you have less than 1/(48000*7)
func HashrateEstimatePercent_7day() float64 {
	return HashrateEstimatePercent(7 * 24 * 3600)
}

func CountMiners() int {
	defer cleanup()
	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()

	miners_count = len(client_list)
	return miners_count
}

var green string = "\033[32m"      // default is green color
var yellow string = "\033[33m"     // make prompt yellow
var red string = "\033[31m"        // make prompt red
var blue string = "\033[34m"       // blue color
var reset_color string = "\033[0m" // reset color

func ListMiners() {

	miner_stats := GetAllMinerStats()

	fmt.Print("Connected Miners\n\n")

	fmt.Printf("%-72s %-10s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-14s %-12s\n\n", "Wallet", "Connected", "Miners", "Hashrate", "IB", "MB", "IBO", "MBO", "MBR", "Success Rate", "Last Error")

	total_hashrate := float64(0)
	for wallet, stat := range miner_stats {

		miners_connected_str := fmt.Sprintf("%d", stat.miners)

		orphan_ib, orphan_mb := p2p.GetMinerOrphanCount(wallet)
		orphan_count := uint64(orphan_ib + orphan_mb)

		total_blocks := float64(stat.blocks + stat.miniblocks + stat.rejected)
		bad_blocks := float64(stat.rejected + orphan_count)

		success_rate := float64(100)

		if bad_blocks >= 1 && total_blocks >= 1 {
			success_rate = float64(100 - (float64(bad_blocks/total_blocks) * 100))
		} else if bad_blocks >= 1 {
			success_rate = float64(0)
		}

		hashrate := MinerHashrate(wallet)
		total_hashrate += hashrate
		hash_rate_string := ""

		switch {
		case hashrate > 1000000000000:
			hash_rate_string = fmt.Sprintf("%.3f TH/s", float64(hashrate)/1000000000000.0)
		case hashrate > 1000000000:
			hash_rate_string = fmt.Sprintf("%.3f GH/s", float64(hashrate)/1000000000.0)
		case hashrate > 1000000:
			hash_rate_string = fmt.Sprintf("%.3f MH/s", float64(hashrate)/1000000.0)
		case hashrate > 1000:
			hash_rate_string = fmt.Sprintf("%.3f KH/s", float64(hashrate)/1000.0)
		case hashrate > 0:
			hash_rate_string = fmt.Sprintf("%d H/s", int(hashrate))
		}

		success_rate_str := fmt.Sprintf("%.2f%%", success_rate)

		is_connected := "no"

		if stat.miners > 0 {
			is_connected = "yes"
		}

		fmt.Printf("%-72s %-10s %-12s %-12s %-12d %-12d %-12d %-12d %-12d %-14s %-12s\n", wallet, is_connected, miners_connected_str, hash_rate_string, stat.blocks, stat.miniblocks, orphan_ib, orphan_mb, stat.rejected, success_rate_str, stat.lasterr)

	}
	hash_rate_string := "0 Hs"

	switch {
	case total_hashrate > 1000000000000:
		hash_rate_string = fmt.Sprintf("%.3f TH/s", float64(total_hashrate)/1000000000000.0)
	case total_hashrate > 1000000000:
		hash_rate_string = fmt.Sprintf("%.3f GH/s", float64(total_hashrate)/1000000000.0)
	case total_hashrate > 1000000:
		hash_rate_string = fmt.Sprintf("%.3f MH/s", float64(total_hashrate)/1000000.0)
	case total_hashrate > 1000:
		hash_rate_string = fmt.Sprintf("%.3f KH/s", float64(total_hashrate)/1000.0)
	case total_hashrate > 0:
		hash_rate_string = fmt.Sprintf("%d H/s", int(total_hashrate))
	}

	fmt.Printf("\nTotal %d Miners Mining @ %s (Reported Hashrate)\n", len(miner_stats), hash_rate_string)

	fmt.Print("\n")
}

func SendJob() {

	defer globals.Recover(1)

	// get a block template, and then we will fill the address here as optimization
	bl, mbl_main, _, _, err := chain.Create_new_block_template_mining(chain.IntegratorAddress())
	if err != nil {
		logger.Error(err, "Failed to create template")
		return
	}

	prev_hash := ""
	for i := range bl.Tips {
		prev_hash = prev_hash + bl.Tips[i].String()
	}

	diff := chain.Get_Difficulty_At_Tips(bl.Tips)

	if mbl_main.HighDiff {
		diff.Mul(diff, new(big.Int).SetUint64(config.MINIBLOCK_HIGHDIFF))
	}
	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()

	for rk, rv := range client_list {

		go func(k *websocket.Conn, v *user_session) {
			defer globals.Recover(2)
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)

			var params rpc.GetBlockTemplate_Result
			params.JobID = fmt.Sprintf("%d.%d.%s", bl.Timestamp, 0, "notified")
			params.Height = bl.Height
			params.Prev_Hash = prev_hash
			params.Difficultyuint64 = diff.Uint64()
			params.Difficulty = diff.String()
			params.Hansen33Mod = true

			// Don't start mining before we are ready and can win blocks
			if globals.CountTotalBlocks >= 8 {

				mbl := mbl_main

				if !mbl.Final { //write miners address only if possible
					miner_stat := getMinerStats(v.address.String())

					// anto cheat
					if miner_stat.feesoverdue && config.RunningConfig.AntiCheat {
						// logger.V(3).Info(fmt.Sprintf("Packing template with integrator address for cheater (%s)", v.address.String()))
						addr := chain.IntegratorAddress()
						addr_raw := addr.PublicKey.EncodeCompressed()
						fee_address_sum := graviton.Sum(addr_raw)
						copy(mbl.KeyHash[:], fee_address_sum[:])
						// params.LastError = "Cheating detected.. Fees are overdue! - forced mining in progress"
					} else {
						copy(mbl.KeyHash[:], v.address_sum[:])

					}
				}

				// We already to this in - Create_new_block_template_mining ConvertBlockToMiniblock
				// for i := range mbl.Nonce { // give each user different work
				// 	mbl.Nonce[i] = globals.Global_Random.Uint32() // fill with randomness
				// }
				params.Blockhashing_blob = fmt.Sprintf("%x", mbl.Serialize())

			} else {
				if globals.CountTotalBlocks > 0 {
					params.LastError = fmt.Sprintf("Node is ready in (%d) ...", 10-globals.CountTotalBlocks)
				} else {
					params.LastError = "Node is currently booting, mining will resume shortly ..."
				}
			}

			if !v.valid_address && !chain.IsAddressHashValid(false, v.address_sum) {
				params.LastError = "unregistered miner or you need to wait 15 mins"
			} else {
				v.valid_address = true
			}
			params.Blocks = v.blocks
			params.MiniBlocks = v.miniblocks
			if v.hashrate < 1 { // if not a hansen mod miner, then deduct orphan from mined blocks
				params.MiniBlocks = v.miniblocks - v.orphans
			}
			params.Rejected = v.rejected
			params.Orphans = v.orphans

			if globals.NodeMaintenance && globals.MaintenanceStart+300 >= time.Now().Unix() {
				params.LastError = config.RunningConfig.MinerMaintenanceMessage
			}

			encoder.Encode(params)
			k.SetWriteDeadline(time.Now().Add(time.Duration(config.RunningConfig.MinerLatency) * time.Millisecond))
			timeout := k.WriteMessage(websocket.TextMessage, buf.Bytes())
			if timeout != nil {
				go MinerMetric(k.RemoteAddr().String(), v.address.String(), "lasterror", "Latency too high!")
			}
			buf.Reset()

		}(rk, rv)

	}
}

var hashrate_lock sync.Mutex

type MinersHashrate struct {
	hashrate  float64
	wallet    string
	submitted int64
}

var MinerHashrateMap = make(map[string]MinersHashrate)

func DeleteMinerHashrate(connection string) {

	hashrate_lock.Lock()
	defer hashrate_lock.Unlock()

	// logger.V(1).Info(fmt.Sprintf("Deleting Miner (%s) Hashrate", connection))

	delete(MinerHashrateMap, connection)

	// check for old expired submitted ones
	for key, stat := range MinerHashrateMap {
		if stat.submitted+19 <= time.Now().Unix() {
			delete(MinerHashrateMap, key)
		}
	}

}

func SubmitMinerHashrate(connection string, miner string, hashrate float64) {

	hashrate_lock.Lock()
	defer hashrate_lock.Unlock()

	// logger.V(1).Info(fmt.Sprintf("Saving Miner (%s / %s) Hashrate (%f)", connection, miner, hashrate))

	x := MinerHashrateMap[connection]
	x.hashrate = hashrate
	x.wallet = miner
	x.submitted = time.Now().Unix()

	MinerHashrateMap[connection] = x

}

func MinerHashrate(miner string) (hashrate float64) {

	hashrate_lock.Lock()
	defer hashrate_lock.Unlock()

	for _, stat := range MinerHashrateMap {
		// logger.V(1).Info(fmt.Sprintf("Checking %s vs %s", stat.wallet, miner))

		if stat.wallet == miner {
			hashrate += stat.hashrate
		}
	}

	return hashrate
}

func GetMinerAddressFromKeyHash(chain *blockchain.Blockchain, mbl block.MiniBlock) string {

	if toporecord, err1 := chain.Store.Topo_store.Read(chain.Get_Height()); err1 == nil { // we must now fill in compressed ring members
		if ss, err1 := chain.Store.Balance_store.LoadSnapshot(toporecord.State_Version); err1 == nil {
			if balance_tree, err1 := ss.GetTree(config.BALANCE_TREE); err1 == nil {
				bits, key, _, err1 := balance_tree.GetKeyValueFromHash(mbl.KeyHash[0:16])
				if err1 != nil || bits >= 120 {
					return ""
				}
				if addr, err1 := rpc.NewAddressFromCompressedKeys(key); err1 == nil {
					return addr.String()
				}

			}
		}
	}

	return ""
}

func newUpgrader() *websocket.Upgrader {
	u := websocket.NewUpgrader()

	u.OnMessage(func(c *websocket.Conn, messageType websocket.MessageType, data []byte) {
		// echo
		//c.WriteMessage(messageType, data)

		if messageType != websocket.TextMessage {
			return
		}

		sess := c.Session().(*user_session)
		miner := ParseIPNoError(c.RemoteAddr().String())

		client_list_mutex.Lock()
		defer client_list_mutex.Unlock()

		var x rpc.MinerInfo_Params
		if json.Unmarshal(data, &x); len(x.Wallet_Address) > 0 {
			// Update miners information
			//logger.V(2).Info(fmt.Sprintf("IP: %-22s Speed: %-14f Tag: %-22s Address: %s", c.RemoteAddr().String(), x.Miner_Hashrate, x.Miner_Tag, x.Wallet_Address))
			go SubmitMinerHashrate(c.RemoteAddr().String(), x.Wallet_Address, x.Miner_Hashrate)
			sess.tag = x.Miner_Tag
			return
		}

		var p rpc.SubmitBlock_Params

		if err := json.Unmarshal(data, &p); err != nil {
			// don't think this ever happens
			logger.Info(fmt.Sprintf("Error: %s", err.Error()))
		}

		mbl_block_data_bytes, err := hex.DecodeString(p.MiniBlockhashing_blob)
		if err != nil {
			//logger.Info("Submitting block could not be decoded")
			sess.lasterr = fmt.Sprintf("Submitted block could not be decoded. err: %s", err)
			go MinerMetric(miner, sess.address.String(), "lasterror", sess.lasterr)
			return
		}

		var tstamp, extra uint64
		fmt.Sscanf(p.JobID, "%d.%d", &tstamp, &extra)

		_, blid, sresult, err := chain.Accept_new_block(tstamp, mbl_block_data_bytes)

		if sresult {

			// Save mini and miner
			var mbl block.MiniBlock

			if err = mbl.Deserialize(mbl_block_data_bytes); err != nil {
				logger.V(1).Error(err, "Error Deserializing newly minted block")
			} else {
			}

			miner_stat := getMinerStats(sess.address.String())
			if miner_stat.feesoverdue && config.RunningConfig.AntiCheat {

				wallet := GetMinerAddressFromKeyHash(chain, mbl)

				logger.Info(fmt.Sprintf("Checking if Cheater (%s) has paid his due(s)", sess.address.String()))

				if wallet == chain.IntegratorAddress().String() || mbl.Final {
					logger.Info(fmt.Sprintf("Cheater (%s) has paid his due(s)", sess.address.String()))
					go MinerMetric(miner, sess.address.String(), "feeispaid", "Cheater has Paid!")
				} else {
					logger.Info(fmt.Sprintf("Minted block for own (%s) vs. (%s) and is still cheating! Anti Cheat not working!", wallet, chain.IntegratorAddress().String()))
					go MinerMetric(miner, sess.address.String(), "feeispaid", "Super cheating!")
				}

			}

			//logger.Infof("Submitted block %s accepted", blid)
			if blid.IsZero() {
				sess.miniblocks++
				atomic.AddInt64(&globals.CountMinisAccepted, 1)
				// chain.MiniBlocks.RLock()
				globals.MiniBlocksCollectionCount = uint8(len(chain.MiniBlocks.Collection[mbl.GetKey()]))
				// chain.MiniBlocks.RUnlock()
				go MinerMetric(miner, sess.address.String(), "miniblocks", fmt.Sprintf("%d", globals.MiniBlocksCollectionCount))
				go p2p.CheckIfMiniBlockIsOrphaned(true, mbl, sess.address.String())
				atomic.AddInt64(&globals.CountTotalBlocks, 1)
				go p2p.AddBlockToMyCollection(mbl, sess.address.String())

				rate_lock.Lock()
				defer rate_lock.Unlock()
				mini_found_time = append(mini_found_time, time.Now().Unix())

				// Reset fail count in case of valid PoW
				for i, t := range ban_list {
					if miner == i && t.fail_count < 25 {
						t.fail_count = 0
						ban_list[miner] = t
					}

					if miner == i && t.fail_count >= 25 {
						t.timestamp = time.Now()
						ban_list[miner] = t
						c.Close()
						delete(client_list, c)
						logger_getwork.V(1).Info("Banned miner", "Address", miner, "Info", "Banned")
						go MinerMetric(miner, sess.address.String(), "lasterror", "Banned miner")

					}
				}

			} else {
				sess.blocks++
				atomic.AddInt64(&globals.CountBlocksAccepted, 1)
				atomic.AddInt64(&globals.CountTotalBlocks, 1)
				go p2p.AddBlockToMyCollection(mbl, sess.address.String())

				go p2p.CheckIfBlockIsOrphaned(true, mbl, sess.address.String())

				go MinerMetric(miner, sess.address.String(), "blocks", "")

			}

			if config.RunningConfig.AntiCheat {
				cheat_ratio := 0.0
				if miner_stat.blocks >= 1 && miner_stat.miniblocks >= 1 {
					cheat_ratio = float64(float64(miner_stat.blocks)/float64(miner_stat.blocks+miner_stat.miniblocks)) * 100
				}

				if miner_stat.miniblocks > 19 && cheat_ratio <= 4.0 {

					go MinerMetric(miner, sess.address.String(), "feeisdue", chain.IntegratorAddress().String())

					logger.V(1).Info(fmt.Sprintf("Cheater (%s) - marking session for repacking", sess.address.String()))
				}
			}

		}

		if !sresult || err != nil {
			sess.rejected++
			atomic.AddInt64(&globals.CountMinisRejected, 1)

			go MinerMetric(miner, sess.address.String(), "rejected", "")

			if err != nil {
				sess.lasterr = err.Error()
				go MinerMetric(miner, sess.address.String(), "lasterror", sess.lasterr)
			}

			// Increase fail count and ban miner in case of 3 invalid PoW's in a row
			rate_lock.Lock()
			defer rate_lock.Unlock()
			i := ban_list[miner]
			i.fail_count++
			if i.fail_count >= 3 {
				i.timestamp = time.Now()
				c.Close()

				delete(client_list, c)
				logger_getwork.V(1).Info("Banned miner", "Address", miner, "Info", "Banned")
				go MinerMetric(miner, sess.address.String(), "lasterror", "Banned miner")
			}
			ban_list[miner] = i

		}

	})
	u.OnClose(func(c *websocket.Conn, err error) {

		sess := c.Session().(*user_session)
		go MinerMetric(ParseIPNoError(c.RemoteAddr().String()), sess.address.String(), "byeminer", "")
		go DeleteMinerHashrate(c.RemoteAddr().String())
		client_list_mutex.Lock()
		defer client_list_mutex.Unlock()
		delete(client_list, c)

	})

	return u
}

func onWebsocket(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/ws/") {
		http.NotFound(w, r)
		return
	}
	address := strings.TrimPrefix(r.URL.Path, "/ws/")

	addr, err := globals.ParseValidateAddress(address)
	if err != nil {
		fmt.Fprintf(w, "err: %s\n", err)
		return
	}
	addr_raw := addr.PublicKey.EncodeCompressed()

	upgrader := newUpgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		//panic(err)
		return
	}

	// Check incoming connections if ban still exists
	// Ban is active for 15 minutes
	miner := ParseIPNoError(r.RemoteAddr)
	rate_lock.Lock()

	for i, t := range ban_list {
		if miner == i {
			if time.Now().Sub(t.timestamp) < time.Minute*15 {
				logger_getwork.V(1).Info("Banned miner", "Address", i, "Info", "Ban still active")
				conn.Close()
				break
			} else {
				delete(ban_list, i)
				break
			}
		}
	}
	rate_lock.Unlock()

	wsConn := conn.(*websocket.Conn)

	session := user_session{address: *addr, address_sum: graviton.Sum(addr_raw)}
	wsConn.SetSession(&session)

	go MinerMetric(ParseIPNoError(conn.RemoteAddr().String()), session.address.String(), "newminer", "")

	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()
	client_list[wsConn] = &session

}

func Getwork_server() {

	var err error

	logger_getwork = globals.Logger.WithName("GETWORK")

	logging.SetLevel(logging.LevelNone) //LevelDebug)//LevelNone)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{generate_random_tls_cert()},
		InsecureSkipVerify: true,
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/", onWebsocket) // handle everything

	default_address := fmt.Sprintf("0.0.0.0:%d", globals.Config.GETWORK_Default_Port)

	if _, ok := globals.Arguments["--getwork-bind"]; ok && globals.Arguments["--getwork-bind"] != nil {
		addr, err := net.ResolveTCPAddr("tcp", globals.Arguments["--getwork-bind"].(string))
		if err != nil {
			logger_getwork.Error(err, "--getwork-bind address is invalid")
			return
		} else {
			if addr.Port == 0 {
				logger_getwork.Info("GETWORK server is disabled, No ports will be opened for miners to get work")
				return
			} else {
				default_address = addr.String()
			}
		}
	}

	logger_getwork.Info("GETWORK will listen", "address", default_address)

	svr = nbhttp.NewServer(nbhttp.Config{
		Name:                    "GETWORK",
		Network:                 "tcp",
		AddrsTLS:                []string{default_address},
		TLSConfig:               tlsConfig,
		Handler:                 mux,
		MaxLoad:                 10 * 10240,
		MaxWriteBufferSize:      32 * 1024,
		ReleaseWebsocketPayload: true,
		KeepaliveTime:           240 * time.Hour, // we expects all miners to find a block every 10 days,
		NPoller:                 runtime.NumCPU(),
	})

	svr.OnReadBufferAlloc(func(c *nbio.Conn) []byte {
		return memPool.Get().([]byte)
	})
	svr.OnReadBufferFree(func(c *nbio.Conn, b []byte) {
		memPool.Put(b)
	})

	//globals.Cron.AddFunc("@every 2s", SendJob) // if daemon restart automaticaly send job
	go func() { // try to be as optimized as possible to lower hash wastage
		if config.RunningConfig.GETWorkJobDispatchTime.Milliseconds() < 40 {
			config.RunningConfig.GETWorkJobDispatchTime = 500 * time.Millisecond
		}
		logger_getwork.Info("Job will be dispatched every", "time", config.RunningConfig.GETWorkJobDispatchTime)
		old_mini_count := 0
		old_time := time.Now()
		old_height := int64(0)
		for {
			if miners_count > 0 {

				dispatch_time := config.RunningConfig.GETWorkJobDispatchTime
				current_mini_count := chain.MiniBlocks.Count()
				current_height := chain.Get_Height()

				if config.RunningConfig.VariableDispatchTime {

					delay := int64(900)

					if current_mini_count > 0 && current_mini_count < 8 {
						delay = 900 - (100 * int64(current_mini_count))
					}

					if current_mini_count >= 8 {
						delay = 50
					}

					dispatch_time = time.Duration(delay * int64(time.Millisecond))
				}

				if old_mini_count != current_mini_count || old_height != current_height || time.Now().Sub(old_time) > dispatch_time {
					old_mini_count = current_mini_count
					old_height = current_height
					SendJob()
					old_time = time.Now()
				}
			} else {

			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if err = svr.Start(); err != nil {
		logger_getwork.Error(err, "nbio.Start failed.")
		return
	}
	logger.Info("GETWORK/Websocket server started")
	svr.Wait()
	defer svr.Stop()

}

// generate default tls cert to encrypt everything
// NOTE: this does NOT protect from individual active man-in-the-middle attacks
func generate_random_tls_cert() tls.Certificate {

	/* RSA can do only 500 exchange per second, we need to be faster
	     * reference https://github.com/golang/go/issues/20058
	    key, err := rsa.GenerateKey(rand.Reader, 512) // current using minimum size
	if err != nil {
	    log.Fatal("Private key cannot be created.", err.Error())
	}

	// Generate a pem block with the private key
	keyPem := pem.EncodeToMemory(&pem.Block{
	    Type:  "RSA PRIVATE KEY",
	    Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	*/
	// EC256 does roughly 20000 exchanges per second
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		logger.Error(err, "Unable to marshal ECDSA private key")
		panic(err)
	}
	// Generate a pem block with the private key
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: b})

	tml := x509.Certificate{
		SerialNumber: big.NewInt(int64(time.Now().UnixNano())),

		// TODO do we need to add more parameters to make our certificate more authentic
		// and thwart traffic identification as a mass scale

		// you can add any attr that you need
		NotBefore: time.Now().AddDate(0, -1, 0),
		NotAfter:  time.Now().AddDate(1, 0, 0),
		// you have to generate a different serial number each execution
		/*
		   Subject: pkix.Name{
		       CommonName:   "New Name",
		       Organization: []string{"New Org."},
		   },
		   BasicConstraintsValid: true,   // even basic constraints are not required
		*/
	}
	cert, err := x509.CreateCertificate(rand.Reader, &tml, &tml, &key.PublicKey, key)
	if err != nil {
		logger.Error(err, "Certificate cannot be created.")
		panic(err)
	}

	// Generate a pem block with the certificate
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	tlsCert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		logger.Error(err, "Certificate cannot be loaded.")
		panic(err)
	}
	return tlsCert
}

func ParseIP(s string) (string, error) {
	ip, _, err := net.SplitHostPort(s)
	if err == nil {
		return ip, nil
	}

	ip2 := net.ParseIP(s)
	if ip2 == nil {
		return "", fmt.Errorf("invalid IP")
	}

	return ip2.String(), nil
}

func ParseIPNoError(s string) string {
	ip, _ := ParseIP(s)
	return ip
}
