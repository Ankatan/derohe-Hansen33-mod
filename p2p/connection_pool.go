// Copyright 2017-2021 DERO Project. All rights reserved.
// Use of this source code in any form is governed by RESEARCH license.
// license can be found in the LICENSE file.
// GPG: 0F39 E425 8C65 3947 702A  8234 08B2 0360 A03A 9DE8
//
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
// THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package p2p

/* this file implements the connection pool manager, keeping a list of active connections etc
 * this will also ensure that a single IP is connected only once
 *
 */
import (
	"context"
	"fmt"
	"math"
	"net"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/rpc2"
	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/metrics"
	"github.com/deroproject/derohe/transaction"
	"github.com/dustin/go-humanize"
	"github.com/go-logr/logr"
)

// any connection incoming/outgoing can only be in this state
//type Conn_State uint32

const (
	HANDSHAKE_PENDING uint32 = 0 // "Pending"
	IDLE                     = 1 // "Idle"
	ACTIVE                   = 2 // "Active"
)

const MAX_CLOCK_DATA_SET = 16

// This structure is used to do book keeping for the connection and keeps other DATA related to peer
// golang restricts 64 bit uint64/int atomic on a 64 bit boundary
// therefore all atomics are on the top, As suggested by Slixe
type Connection struct {
	Height                int64  // last height sent by peer  ( first member alignments issues)
	StableHeight          int64  // last stable height
	TopoHeight            int64  // topo height, current topo height, this is the only thing we require for syncing
	Pruned                int64  // till where chain has been pruned on this node
	LastObjectRequestTime int64  // when was the last item placed in object list
	Latency               int64  // time.Duration            // latency to this node when sending timed sync
	BytesIn               uint64 // total bytes in
	BytesOut              uint64 // total bytes out
	Top_Version           uint64 // current hard fork version supported by peer
	Peer_ID               uint64 // Remote peer id
	Port                  uint32 // port advertised by other end as its server,if it's 0 server cannot accept connections
	State                 uint32 // state of the connection
	Syncing               int32  // denotes whether we are syncing and thus stop pinging

	Client  *rpc2.Client
	Conn    net.Conn // actual object to talk
	ConnTls net.Conn // tls layered conn

	StateHash crypto.Hash // statehash at the top

	Created time.Time // when was object created

	Incoming        bool     // is connection incoming or outgoing
	Addr            net.Addr // endpoint on the other end
	SyncNode        bool     // whether the peer has been added to command line as sync node
	ProtocolVersion string
	Tag             string // tag for the other end
	DaemonVersion   string
	Top_ID          crypto.Hash // top block id of the connection

	logger logr.Logger // connection specific logger

	Requested_Objects [][32]byte // currently unused as we sync up with a single peer at a time

	peer_sent_time   time.Time // contains last time when peerlist was sent
	update_received  time.Time // last time when upated was received
	ping_in_progress int32     // contains ping pending against this connection

	ping_count int64

	clock_index   int
	clock_offsets [MAX_CLOCK_DATA_SET]time.Duration
	delays        [MAX_CLOCK_DATA_SET]time.Duration
	clock_offset  int64 // duration updated on every miniblock
	onceexit      sync.Once

	Mutex sync.Mutex // used only by connection go routine
}

func ConnecToNode(address string) {

	go connect_with_endpoint(address, false)
}

func Address(c *Connection) string {
	if c.Addr == nil {
		return ""
	}
	return ParseIPNoError(c.Addr.String())
}

func (c *Connection) exit() {
	defer globals.Recover(0)
	c.onceexit.Do(func() {
		c.Client.Close()
		c.ConnTls.Close()
		c.Conn.Close()

	})

}

// add connection to  map
func Connection_Delete(c *Connection) {

	duplicate_connection_mutex.Lock()
	defer duplicate_connection_mutex.Unlock()
	ip_str, x := ConnectDuplicatioMap[ParseIPNoError(c.Addr.String())]
	if x {
		if ip_str == c.Addr.String() {
			c.logger.V(2).Info(fmt.Sprintf("Deleting Connection: %s", c.Addr.String()))
			delete(ConnectDuplicatioMap, ParseIPNoError(c.Addr.String()))
		} else {
			c.logger.V(2).Info(fmt.Sprintf("Deleting Duplicate Connection: %s vs %s", ip_str, c.Addr.String()))
		}
	}

	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)

		// Clear all connection to same IP
		if c.Addr.String() == v.Addr.String() {
			c.exit()
			// if ParseIPNoError(c.Addr.String()) == ParseIPNoError(v.Addr.String()) {
			connection_map.Delete(Address(v))
			return false
		}
		return true
	})
}

func Connection_Pending_Clear() {

	var pending_clear_count int = 0
	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if atomic.LoadUint32(&v.State) == HANDSHAKE_PENDING && time.Now().Sub(v.Created) > 10*time.Second { //and skip ourselves
			v.exit()
			v.logger.V(3).Info("Cleaning pending connection")
		}

		if time.Now().Sub(v.update_received).Round(time.Second).Seconds() > 20 && pending_clear_count < 10 {
			v.exit()
			Connection_Delete(v)
			v.logger.V(1).Info(fmt.Sprintf("Purging connection (%s) since idle for %s", v.Addr.String(), time.Now().Sub(v.update_received).Round(time.Second).String()))
			pending_clear_count++
		}

		if IsAddressInBanList(Address(v)) {
			v.exit()
			Connection_Delete(v)
			v.logger.V(1).Info("Purging connection due to ban list")
		}

		return true
	})

	if pending_clear_count > 1 {
		logger.V(1).Info(fmt.Sprintf("Purged %d/%d connections", pending_clear_count, Peer_Count()))
	}
}

var connection_map sync.Map // map[string]*Connection{}

// check whether an IP is in the map already
func IsAddressConnected(address string) bool {
	if _, ok := connection_map.Load(strings.TrimSpace(address)); ok {
		return true
	}
	return false
}

// add connection to  map, only if we are not connected already
// we also check for limits for incoming connections
// same ip max 8 ip ( considering NAT)
//same Peer ID   4

var connection_counter int = 0

var ConnectDuplicatioMap = make(map[string]string)
var duplicate_connection_mutex sync.Mutex

func Connection_Add(c *Connection) bool {

	duplicate_connection_mutex.Lock()
	defer duplicate_connection_mutex.Unlock()
	_, x := ConnectDuplicatioMap[ParseIPNoError(c.Addr.String())]
	if x {
		c.logger.Info(fmt.Sprintf("Connection (%s) already added (%s)", c.Addr.String(), ConnectDuplicatioMap[ParseIPNoError(c.Addr.String())]))
		return true
	}

	if dup, ok := connection_map.LoadOrStore(Address(c), c); !ok {

		connection_counter++
		c.Created = time.Now()
		c.update_received = time.Now()

		c.logger.V(3).Info(fmt.Sprintf("IP address being added (%d)", connection_counter), "ip", c.Addr.String())

		ConnectDuplicatioMap[ParseIPNoError(c.Addr.String())] = c.Addr.String()

		return true
	} else {
		c.logger.V(3).Info("IP address already has one connection, exiting this connection", "ip", c.Addr.String(), "pre", dup.(*Connection).Addr.String())
		c.exit()
		return false
	}
}

// unique connection list
// since 2 nodes may be connected in both directions, we need to deliver new blocks/tx to only one
// thereby saving NW/computing costs
// we find duplicates using peer id
func UniqueConnections() map[uint64]*Connection {
	unique_map := map[uint64]*Connection{}
	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && GetPeerID() != v.Peer_ID && time.Now().Sub(v.update_received).Round(time.Second).Seconds() < 20 { //and skip ourselves
			unique_map[v.Peer_ID] = v // map will automatically deduplicate/overwrite previous
		}
		return true
	})
	return unique_map
}

// this function has infinite loop to keep ping every few sec
func ping_loop() {
	connection_map.Range(func(k, value interface{}) bool {
		c := value.(*Connection)
		if atomic.LoadUint32(&c.State) != HANDSHAKE_PENDING && GetPeerID() != c.Peer_ID /*&& atomic.LoadInt32(&c.ping_in_progress) == 0*/ {
			if atomic.LoadInt32(&c.Syncing) >= 1 {
				return true
			}
			go func() {
				defer globals.Recover(3)
				atomic.AddInt32(&c.ping_in_progress, 1)
				defer atomic.AddInt32(&c.ping_in_progress, -1)

				var request, response Dummy
				fill_common(&request.Common) // fill common info

				c.ping_count++
				if c.ping_count%10 == 1 {
					request.Common.PeerList = get_peer_list_specific(Address(c))
				}

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				if err := c.Client.CallWithContext(ctx, "Peer.Ping", request, &response); err != nil {
					c.logger.V(2).Error(err, "ping failed")
					c.exit()
					return
				}
				c.update(&response.Common) // update common information
			}()
		}
		return true
	})
}

// prints all the connection info to screen
func Connection_Print() {
	var clist []*Connection

	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		clist = append(clist, v)
		return true
	})

	version, err := chain.ReadBlockSnapshotVersion(chain.Get_Top_ID())
	if err != nil {
		panic(err)
	}

	StateHash, err := chain.Load_Merkle_Hash(version)

	if err != nil {
		panic(err)
	}

	logger.Info("Connection info for peers", "count", len(clist), "our Statehash", StateHash)

	fmt.Printf("%-30s %-16s %-5s %-7s %-7s %-7s %23s %3s %5s %s %s %16s %16s\n", "Remote Addr", "PEER ID", "PORT", " State", "Latency", "Offset", "S/H/T", "DIR", "BTS", "     IN", "    OUT", "Version", "Statehash")

	// sort the list
	sort.Slice(clist, func(i, j int) bool { return clist[i].Addr.String() < clist[j].Addr.String() })

	our_topo_height := chain.Load_TOPO_HEIGHT()
	var sum_latency int64

	for i := range clist {

		// skip pending  handshakes and skip ourselves
		if atomic.LoadUint32(&clist[i].State) == HANDSHAKE_PENDING || GetPeerID() == clist[i].Peer_ID {
			//	continue
		}

		PeerAddress := ParseIPNoError(clist[i].Addr.String())
		_, _, _, SuccessRate := GetPeerBTS(PeerAddress)

		dir := "OUT"
		if clist[i].Incoming {
			dir = "INC"
		}
		state := "PENDING"
		if atomic.LoadUint32(&clist[i].State) == IDLE {
			state = "IDLE"
		} else if atomic.LoadUint32(&clist[i].State) == ACTIVE {
			state = "ACTIVE"
		}

		atomic.AddInt64(&sum_latency, atomic.LoadInt64(&clist[i].Latency))
		version := clist[i].DaemonVersion

		if len(version) > 20 {
			version = version[:20]
		}

		tag := clist[i].Tag
		if len(tag) > 20 {
			tag = tag[:20]
		}

		var color_yellow = "\033[33m"
		var color_normal = "\033[0m"

		//if our_height is more than
		if our_topo_height > clist[i].TopoHeight {
			fmt.Print(color_yellow)
		}

		ctime := time.Now().Sub(clist[i].Created).Round(time.Second)

		hstring := fmt.Sprintf("%d/%d/%d", clist[i].StableHeight, clist[i].Height, clist[i].TopoHeight)
		fmt.Printf("%-30s %16x %5d %7s %7s %7s %23s %s %7.2f %7s %7s     %16s %s %x\n", Address(clist[i])+" ("+ctime.String()+")", clist[i].Peer_ID, clist[i].Port, state, time.Duration(atomic.LoadInt64(&clist[i].Latency)).Round(time.Millisecond).String(), time.Duration(atomic.LoadInt64(&clist[i].clock_offset)).Round(time.Millisecond).String(), hstring, dir, SuccessRate, humanize.Bytes(atomic.LoadUint64(&clist[i].BytesIn)), humanize.Bytes(atomic.LoadUint64(&clist[i].BytesOut)), version, tag, clist[i].StateHash[:])

		fmt.Print(color_normal)
	}

	avg_latency := sum_latency / int64(len(clist))
	fmt.Printf("Average Latency: %7s\n", time.Duration(avg_latency).Round(time.Millisecond).String())
}

// for continuos update on command line, get the maximum height of all peers
// show the average network status
func Best_Peer_Height() (best_height, best_topo_height int64) {

	var heights []uint64
	var topoheights []uint64

	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING {
			height := atomic.LoadInt64(&v.Height)
			heights = append(heights, uint64(height))
			topoheights = append(topoheights, uint64(atomic.LoadInt64(&v.TopoHeight)))
		}
		return true
	})

	best_height = int64(Median(heights))
	best_topo_height = int64(Median(topoheights))

	return
}

// this function return peer count which have successful handshake
func Peer_Count() (Count uint64) {
	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && GetPeerID() != v.Peer_ID {
			Count++
		}
		return true
	})
	return
}

// this returns count of peers in both directions
func Peer_Direction_Count() (Incoming uint64, Outgoing uint64) {
	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && GetPeerID() != v.Peer_ID {
			if v.Incoming {
				Incoming++
			} else {
				Outgoing++
			}
		}
		return true
	})
	return
}

func Broadcast_Block(cbl *block.Complete_Block, PeerID uint64) {
	Broadcast_Block_Coded(cbl, PeerID)
}

// broad cast a block to all connected peers in cut up in chunks with erasure coding
// we can only broadcast a block which is in our db
// this function is trigger from 2 points, one when we receive a unknown block which can be successfully added to chain
// second from the blockchain which has to relay locally  mined blocks as soon as possible
func Broadcast_Block_Coded(cbl *block.Complete_Block, PeerID uint64) { // if peerid is provided it is skipped
	broadcast_Block_Coded(cbl, PeerID, globals.Time().UTC().UnixMicro())
}

func broadcast_Block_Coded(cbl *block.Complete_Block, PeerID uint64, first_seen int64) {

	defer globals.Recover(3)

	/*if IsSyncing() { // if we are syncing, do NOT broadcast the block
		return
	}*/

	blid := cbl.Bl.GetHash()

	logger.V(1).Info("Will broadcast block", "blid", blid, "tx_count", len(cbl.Bl.Tx_hashes), "txs", len(cbl.Txs))

	hhash, chunk_count := convert_block_to_chunks(cbl, 16, 32)

	our_height := chain.Get_Height()
	// build the request once and dispatch it to all possible peers
	count := 0
	unique_map := UniqueConnections()

	var connections []*Connection
	for _, v := range unique_map {
		connections = append(connections, v)
	}

	sort.SliceStable(connections, func(i, j int) bool {
		return connections[i].Latency < connections[j].Latency
	})

	bw_factor := int(config.RunningConfig.P2PBWFactor)
	if bw_factor < 1 {
		bw_factor = 1
	}

	for { // we must send all blocks atleast once, once we are done, break ut

		if len(connections) < 1 {
			globals.Logger.Error(nil, "we want to broadcast block, but donot have peers, most possibly block will go stale")
			return
		}
		for _, v := range connections {
			select {
			case <-Exit_Event:
				return
			default:
			}
			if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && PeerID != v.Peer_ID && v.Peer_ID != GetPeerID() { // skip pre-handshake connections

				// if the other end is > 2 blocks behind, do not broadcast block to him
				// this is an optimisation, since if the other end is syncing
				// every peer will keep on broadcasting and thus making it more lagging
				// due to overheads
				// if the other end is > 2 blocks forwards, do not broadcast block to him
				peer_height := atomic.LoadInt64(&v.Height)
				if (our_height-peer_height) > 2 || (peer_height-our_height) > 2 {
					continue
				}

				if count > len(unique_map) && count > bw_factor*chunk_count { // every connected peer shuld get ateleast one chunk
					goto done
				}

				atomic.AddUint64(&v.BytesOut, 1)
				// Can or should we adjust this based on tip expansion failure?
				go func(connection *Connection, cid int) {
					defer globals.Recover(3)
					var peer_specific_list ObjectList

					var chunkid [32 + 1 + 32]byte
					copy(chunkid[:], blid[:])
					chunkid[32] = byte(cid % chunk_count)
					copy(chunkid[33:], hhash[:])
					peer_specific_list.Sent = first_seen
					peer_specific_list.Chunk_list = append(peer_specific_list.Chunk_list, chunkid)
					connection.logger.V(3).Info("Sending erasure coded chunk to peer ", "cid", cid)
					var dummy Dummy
					fill_common(&peer_specific_list.Common) // fill common info

					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					if err := connection.Client.CallWithContext(ctx, "Peer.NotifyINV", peer_specific_list, &dummy); err != nil {
						go PeerLogConnectionFail(connection.Addr.String(), "broadcast_Block_Coded", connection.Peer_ID, err.Error())
						go LogReject(connection.Addr.String())

						return
					} else {
						go LogAccept(connection.Addr.String())
					}
					connection.update(&dummy.Common) // update common information

				}(v, count)
				count++
			}
		}
	}

done:

	//rlog.Infof("Broadcasted block %s to %d peers", cbl.Bl.GetHash(), count)

}

// broad cast a block to all connected peers in cut up in chunks with erasure coding
// we can only broadcast a block which is in our db
// this function is triggerred from 2 points, one when we receive a unknown block which can be successfully added to chain
// second from the blockchain which has to relay locally  mined blocks as soon as possible
func broadcast_Chunk(chunk *Block_Chunk, PeerID uint64, first_seen int64) { // if peerid is provided it is skipped
	defer globals.Recover(3)

	our_height := chain.Get_Height()

	count := 0
	unique_map := UniqueConnections()

	hhash := chunk.HHash

	for _, v := range unique_map {
		select {
		case <-Exit_Event:
			return
		default:
		}
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && PeerID != v.Peer_ID && v.Peer_ID != GetPeerID() { // skip pre-handshake connections

			// if the other end is > 50 blocks behind, do not broadcast block to hime
			// this is an optimisation, since if the other end is syncing
			// every peer will keep on broadcasting and thus making it more lagging
			// due to overheads
			peer_height := atomic.LoadInt64(&v.Height)
			if (our_height-peer_height) > 3 || (peer_height-our_height) > 3 {
				continue
			}

			count++
			atomic.AddUint64(&v.BytesOut, 1)
			go func(connection *Connection) {
				defer globals.Recover(3)
				var peer_specific_list ObjectList

				var chunkid [33 + 32]byte
				copy(chunkid[:], chunk.BLID[:])
				chunkid[32] = byte(chunk.CHUNK_ID)
				copy(chunkid[33:], hhash[:])
				peer_specific_list.Sent = first_seen

				peer_specific_list.Chunk_list = append(peer_specific_list.Chunk_list, chunkid)
				connection.logger.V(3).Info("Sending erasure coded chunk INV to peer ", "raw", fmt.Sprintf("%x", chunkid), "blid", fmt.Sprintf("%x", chunk.BLID), "cid", chunk.CHUNK_ID, "hhash", fmt.Sprintf("%x", hhash), "exists", nil != is_chunk_exist(hhash, uint8(chunk.CHUNK_ID)))
				var dummy Dummy
				fill_common(&peer_specific_list.Common) // fill common info

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				if err := connection.Client.CallWithContext(ctx, "Peer.NotifyINV", peer_specific_list, &dummy); err != nil {
					go PeerLogConnectionFail(connection.Addr.String(), "broadcast_Chunk", connection.Peer_ID, err.Error())
					go LogReject(connection.Addr.String())

					return
				} else {
					go LogAccept(connection.Addr.String())
				}
				connection.update(&dummy.Common) // update common information
				// if PeerID == GetPeerID() || PeerID == 0 {
				// 	go logger.Info(fmt.Sprintf("broadcast_Chunk - Peer: %s - Count: %d - PeerID: %d", connection.Addr.String(), count, PeerID))
				// 	// go SelfishNodeCounter(connection.Addr.String(), "broadcast_Chunk", connection.Peer_ID, err.Error())
				// }
			}(v)
		}
	}
}

// broad cast a block to all connected peers
// we can only broadcast a block which is in our db
// this function is trigger from 2 points, one when we receive a unknown block which can be successfully added to chain
// second from the blockchain which has to relay locally  mined blocks as soon as possible
func Broadcast_MiniBlock(mbl block.MiniBlock, PeerID uint64) { // if peerid is provided it is skipped
	broadcast_MiniBlock(mbl, PeerID, globals.Time().UTC().UnixMicro())
}
func broadcast_MiniBlock(mbl block.MiniBlock, PeerID uint64, first_seen int64) { // if peerid is provided it is skipped

	defer globals.Recover(3)

	var peer_specific_block Objects
	peer_specific_block.MiniBlocks = append(peer_specific_block.MiniBlocks, mbl.Serialize())
	fill_common(&peer_specific_block.Common) // fill common info
	peer_specific_block.Sent = first_seen

	our_height := chain.Get_Height()
	// build the request once and dispatch it to all possible peers
	count := 0
	unique_map := UniqueConnections()

	//connection.logger.V(4).Info("Sending mini block to peer ")

	for _, v := range unique_map {
		select {
		case <-Exit_Event:
			return
		default:
		}
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && PeerID != v.Peer_ID && v.Peer_ID != GetPeerID() { // skip pre-handshake connections

			// if the other end is > 50 blocks behind, do not broadcast block to hime
			// this is an optimisation, since if the other end is syncing
			// every peer will keep on broadcasting and thus making it more lagging
			// due to overheads
			peer_height := atomic.LoadInt64(&v.Height)
			if (our_height - peer_height) > 25 {
				continue
			}
			atomic.AddUint64(&v.BytesOut, 1)

			count++
			go func(connection *Connection) {
				defer globals.Recover(3)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				var dummy Dummy
				if err := connection.Client.CallWithContext(ctx, "Peer.NotifyMiniBlock", peer_specific_block, &dummy); err != nil {
					go PeerLogConnectionFail(connection.Addr.String(), "broadcast_MiniBlock", connection.Peer_ID, err.Error())
					go LogReject(connection.Addr.String())

					if PeerID == GetPeerID() || PeerID == 0 {
						go SelfishNodeCounter(connection.Addr.String(), "broadcast_MiniBlock", connection.Peer_ID, err.Error(), mbl.Serialize())
					}

					return
				} else {
					go LogAccept(connection.Addr.String())
				}
				connection.update(&dummy.Common) // update common information

			}(v)
		}

	}
	//rlog.Infof("Broadcasted block %s to %d peers", cbl.Bl.GetHash(), count)
}

// broadcast a new transaction, return to how many peers the transaction has been broadcasted
// this function is trigger from 2 points, one when we receive a unknown tx
// second from the mempool which may want to relay local ot soon going to expire transactions

func Broadcast_Tx(tx *transaction.Transaction, PeerID uint64) (relayed_count int32) {
	return broadcast_Tx(tx, PeerID, globals.Time().UTC().UnixMicro())

}
func broadcast_Tx(tx *transaction.Transaction, PeerID uint64, sent int64) (relayed_count int32) {
	defer globals.Recover(3)

	var request ObjectList
	fill_common_skip_topoheight(&request.Common) // fill common info, but skip topo height
	txhash := tx.GetHash()

	request.Tx_list = append(request.Tx_list, txhash)
	request.Sent = sent

	our_height := chain.Get_Height()

	unique_map := UniqueConnections()

	for _, v := range unique_map {
		select {
		case <-Exit_Event:
			return
		default:
		}
		if atomic.LoadUint32(&v.State) != HANDSHAKE_PENDING && PeerID != v.Peer_ID && v.Peer_ID != GetPeerID() { // skip pre-handshake connections

			// if the other end is > 50 blocks behind, do not broadcast block to hime
			// this is an optimisation, since if the other end is syncing
			// every peer will keep on broadcasting and thus making it more lagging
			// due to overheads
			// if we are lagging or peer is lagging, do not brodcast transactions
			peer_height := atomic.LoadInt64(&v.Height)
			if (our_height-peer_height) > 25 || (our_height+5) < peer_height {
				continue
			}

			go func(connection *Connection) {
				defer func() {
					if r := recover(); r != nil {
						connection.logger.V(1).Error(nil, "Recovere3d while sending tx", "r", r, "stack", debug.Stack())
					}
				}()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				var dummy Dummy
				fill_common(&dummy.Common) // fill common info
				if err := connection.Client.CallWithContext(ctx, "Peer.NotifyINV", request, &dummy); err != nil {
					return
				}
				connection.update(&dummy.Common) // update common information
				atomic.AddInt32(&relayed_count, 1)

			}(v)
		}

	}
	if relayed_count > 0 {
		//rlog.Debugf("Broadcasted tx %s to %d peers", txhash, relayed_count)
	}
	return
}

// trigger a sync with a random peer
func trigger_sync() {
	defer globals.Recover(3)

	unique_map := UniqueConnections()

	var clist []*Connection

	for _, value := range unique_map {
		clist = append(clist, value)
	}

	// sort the list random
	// do random shuffling, can we get away with len/2 random shuffling
	globals.Global_Random.Shuffle(len(clist), func(i, j int) {
		clist[i], clist[j] = clist[j], clist[i]
	})

	for _, connection := range clist {

		height := chain.Get_Height()

		//connection.Lock()   recursive mutex are not suported
		// only choose highest available peers for syncing
		if atomic.LoadUint32(&connection.State) != HANDSHAKE_PENDING && (height < atomic.LoadInt64(&connection.Height) || (connection.SyncNode && height > (atomic.LoadInt64(&connection.Height)+2))) { // skip pre-handshake connections
			// check whether we are lagging with this connection
			//connection.Lock()
			islagging := (height < atomic.LoadInt64(&connection.Height) || (connection.SyncNode && height > (atomic.LoadInt64(&connection.Height)+2)))

			//fmt.Printf("checking cdiff is lagging %+v  topoheight %d peer topoheight %d \n", islagging, topoheight, connection.TopoHeight)

			// islagging := true
			//connection.Unlock()
			if islagging {
				if connection.Pruned > chain.Load_Block_Topological_order(chain.Get_Top_ID()) && chain.Get_Height() != 0 {
					connection.logger.V(1).Info("We cannot resync with the peer, since peer chain is pruned", "height", connection.Height, "pruned", connection.Pruned)
					continue
				}

				time.Sleep(time.Second)
				height := chain.Get_Height()
				islagging = (height < atomic.LoadInt64(&connection.Height) || (connection.SyncNode && height > (atomic.LoadInt64(&connection.Height)+2)))

				if islagging {
					//connection.Lock()

					connection.logger.V(1).Info("We need to resync with the peer", "our_height", height, "height", connection.Height, "pruned", connection.Pruned)

					logger.Info(fmt.Sprintf("Peer telling us we're on wrong height: %s", connection.Addr.String()))

					//connection.Unlock()
					// set mode to syncronising

					metrics.Set.GetOrCreateCounter("blockchain_sync_total").Inc() // tracks number of syncs

					if chain.Sync {
						//fmt.Printf("chain send chain request disabled\n")
						connection.sync_chain()
						connection.logger.V(1).Info("sync done")

					} else { // we need a state only sync, bootstrap without history but verified chain
						connection.bootstrap_chain()
						chain.Sync = true
					}
					break
				}

			}
		}

	}
}

//go:noinline
func Abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// detect whether we are behind any of the connected peers and trigger sync ASAP
// randomly with one of the peers

var single_sync int32

func syncroniser() {

	defer atomic.AddInt32(&single_sync, -1)

	if atomic.AddInt32(&single_sync, 1) != 1 {
		return
	}
	calculate_network_time() // calculate time every sec
	trigger_sync()           // check whether we are out of sync
}

// update P2P time
func calculate_network_time() {
	var total, count, mean int64
	unique_map := UniqueConnections()

	for _, v := range unique_map {
		if Abs(atomic.LoadInt64(&v.clock_offset)) < 100*1000000000 { //  this is 100 sec
			total += atomic.LoadInt64(&v.clock_offset)
			count++
		}
	}
	if count == 0 {
		return
	}
	mean = total / count
	total, count = 0, 0

	var total_float float64
	for _, v := range unique_map {
		if Abs(atomic.LoadInt64(&v.clock_offset)) < 100*1000000000 { //  this is 100 sec
			total_float += math.Pow(float64(atomic.LoadInt64(&v.clock_offset)-mean), 2)
			count++
		}
	}
	if count == 0 {
		return
	}

	variance := total_float / float64(count)
	std_deviation := int64(math.Trunc(math.Sqrt(variance)))

	//		fmt.Printf("\n1 mean %d std_deviation %d variance %f  total_float %f count %d",mean, std_deviation, variance, total_float,count)

	total, count = 0, 0
	for _, v := range unique_map {
		poffset := atomic.LoadInt64(&v.clock_offset)
		if poffset >= (mean-std_deviation) && poffset <= (mean+std_deviation) {
			total += atomic.LoadInt64(&v.clock_offset)
			count++
		}
	}
	//		fmt.Printf("\n2 mean %d std_deviation %d variance %f  total_float %f count %d totaloffset %d\n",mean, std_deviation, variance, total_float,count,total)

	if count == 0 {
		return
	}

	globals.ClockOffsetP2P = time.Duration(total / count)
}
