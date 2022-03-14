package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type ExternalMiner struct {
	RPC                 *rpc.Client
	Eth                 *ethclient.Client
	TTD                 TTD
	DelayConfigs        DelayCongfigurations
	Miner               *HiveMiner
	NextTotalDifficulty *big.Int

	// Context related
	lastCtx    context.Context
	lastCancel context.CancelFunc
}

type DelayConfiguration struct {
	BlockNumber  string
	MinimumDelay time.Duration
}

func (d *DelayConfiguration) String() string {
	return fmt.Sprintf("BlockNumber=%s,Delay=%v", d.BlockNumber, d.MinimumDelay.String())
}

type DelayCongfigurations []DelayConfiguration

func (l DelayCongfigurations) MinimumDelayForBlockNumber(blockNum uint64) *time.Duration {
	for _, c := range l {
		if i, err := strconv.Atoi(c.BlockNumber); err == nil {
			if blockNum == uint64(i) {
				return &c.MinimumDelay
			}
		}
	}
	return nil
}

func (l DelayCongfigurations) TTDMinimumDelay() *time.Duration {
	for _, c := range l {
		if c.BlockNumber == "TTD" {
			return &c.MinimumDelay
		}
	}
	return nil
}

func (l DelayCongfigurations) DefaultMinimumDelay() time.Duration {
	for _, c := range l {
		if c.BlockNumber == "Default" {
			return c.MinimumDelay
		}
	}
	return time.Duration(0)
}

func (l *DelayCongfigurations) Set(val string) error {
	splitDelayConfig := strings.Split(val, ",")
	if len(splitDelayConfig) != 2 {
		return fmt.Errorf("Invalid delay configuration: %s", val)
	}
	delayInt, err := strconv.Atoi(splitDelayConfig[1])
	if err != nil {
		return fmt.Errorf("Invalid delay configuration: %s", val)
	}
	delay := time.Second * time.Duration(delayInt)
	*l = append(*l, DelayConfiguration{
		BlockNumber:  splitDelayConfig[0],
		MinimumDelay: delay,
	})
	return nil
}

func (l *DelayCongfigurations) String() string {
	var s []string
	for _, d := range *l {
		s = append(s, d.String())
	}
	return strings.Join(s, ";")
}

type TTD struct {
	*big.Int
}

func (t *TTD) Set(val string) error {
	dec := big.NewInt(0)
	if _, suc := dec.SetString(val, 0); !suc {
		return fmt.Errorf("Unable to parse %s", val)
	}
	*t = TTD{dec}
	return nil
}

type RPCUrls []string

func (r *RPCUrls) Set(val string) error {
	*r = append(*r, val)
	return nil
}
func (r *RPCUrls) String() string {
	return strings.Join(*r, " ")
}

func (em *ExternalMiner) GetWork(abort <-chan struct{}) *WorkDescription {
	var result [4]string
	for {
		if err := em.RPC.CallContext(em.Ctx(), &result, "eth_getWork"); err != nil {
			fmt.Printf("Got error (%v), retrying...\n", err)
		} else {
			break
		}
		select {
		case <-abort:
			return nil
		case <-time.After(time.Second):
		}
	}
	var (
		powHash     = common.HexToHash(result[0])
		seedHash    = common.HexToHash(result[1])
		target      = new(big.Int)
		blockNumber uint64
		err         error
	)

	target.SetBytes(common.HexToHash(result[2]).Bytes())

	if result[3] != "" {
		blockNumber, err = hexutil.DecodeUint64(result[3])
		if err != nil {
			return nil
		}
	} else {
		blockNumber, err = em.Eth.BlockNumber(em.Ctx())
		if err != nil {
			return nil
		}
		blockNumber++
	}
	return &WorkDescription{
		PoWHeaderHash: powHash,
		SeedHash:      seedHash,
		Target:        target,
		Difficulty:    new(big.Int).Div(two256, target),
		BlockNumber:   blockNumber,
	}
}

func (em *ExternalMiner) SubmitWork(subWork *SubmittableWork) (bool, error) {
	var result bool
	fmt.Printf("Sending eth_submitWork\n")
	if err := em.RPC.CallContext(em.Ctx(), &result, "eth_submitWork", hexutil.Encode(subWork.Nonce[:]), subWork.PoWHeaderHash.Hex(), subWork.MixDigest.Hex()); err != nil {
		return false, err
	}
	return result, nil
}

func (em *ExternalMiner) Ctx() context.Context {
	if em.lastCtx != nil {
		em.lastCancel()
	}
	em.lastCtx, em.lastCancel = context.WithTimeout(context.Background(), 10*time.Second)
	return em.lastCtx
}

type TotalDifficulty struct {
	TotalDifficulty *hexutil.Big `json:"totalDifficulty"`
}

// Gets the current block total difficulty, and adds the difficulty of resulting from the work done
func (em *ExternalMiner) UpdateNextTotalDifficulty(w *WorkDescription, abort <-chan struct{}) {
	var td *TotalDifficulty

	// Try to get the current total difficulty from the Eth RPC
	for {
		if err := em.RPC.CallContext(em.Ctx(), &td, "eth_getBlockByNumber", "latest", false); err == nil {
			break
		}

		select {
		case <-abort:
			return
		case <-time.After(time.Second):
		}
	}

	totalDifficulty := (*big.Int)(td.TotalDifficulty)
	em.NextTotalDifficulty = totalDifficulty.Add(totalDifficulty, w.Difficulty)
}

func (em *ExternalMiner) LoopMineUntilTTD(abort <-chan struct{}, done chan<- error) {
	defer close(done)
	for {
		select {
		case <-abort:
			done <- nil
			return
		default:
		}
		minedTTD, err := em.MineNextBlock(abort)
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				fmt.Printf("Connection refused, retrying...\n")
				time.Sleep(time.Second)
				continue
			} else {
				done <- err
				return
			}
		}
		if minedTTD {
			done <- nil
			return
		}
	}
}

func (em *ExternalMiner) CalculateNextDelay(w *WorkDescription, abort <-chan struct{}) time.Duration {
	var (
		nextDelay          = em.DelayConfigs.DefaultMinimumDelay()
		currentBlockNumber uint64
		err                error
	)

	for {
		currentBlockNumber, err = em.Eth.BlockNumber(em.Ctx())
		if err == nil {
			break
		}
		select {
		case <-abort:
			return nextDelay
		case <-time.After(time.Second):
		}
	}

	nextBlockNumber := currentBlockNumber + 1

	if blockMinDelay := em.DelayConfigs.MinimumDelayForBlockNumber(nextBlockNumber); blockMinDelay != nil {
		nextDelay = *blockMinDelay
	}

	// If the next block is the TTD reaching block AND a specific delay was configured, apply it here
	em.UpdateNextTotalDifficulty(w, abort)
	miningTTDBlock := em.NextTotalDifficulty.Cmp(em.TTD.Int) >= 0

	if tTDMinDelay := em.DelayConfigs.TTDMinimumDelay(); tTDMinDelay != nil && miningTTDBlock {
		nextDelay = *tTDMinDelay
	}

	return nextDelay
}

func (em *ExternalMiner) MineNextBlock(abort <-chan struct{}) (bool, error) {

	currentWork := em.GetWork(abort)
	if currentWork == nil {
		return false, fmt.Errorf("Unable to get work...")
	}
	nextDelay := em.CalculateNextDelay(currentWork, abort)

	minerAbortChan := make(chan struct{})
	minerFoundChan := make(chan *SubmittableWork)
	start := time.Now()
	go em.Miner.Mine(currentWork, minerAbortChan, minerFoundChan)

	for {
		select {
		case <-abort:
			close(minerAbortChan)
			return false, nil
		case <-time.After(time.Second):
			newWork := em.GetWork(abort)
			if newWork == nil {
				return false, fmt.Errorf("Unable to get work...")
			}
			if currentWork == nil || newWork.PoWHeaderHash != currentWork.PoWHeaderHash {
				if currentWork != nil {
					fmt.Printf("PoW Header Hash changed while mining: %v -> %v\n", currentWork.PoWHeaderHash, newWork.PoWHeaderHash)
				}
				close(minerAbortChan)
				currentWork = newWork
				nextDelay = em.CalculateNextDelay(currentWork, abort)
				minerAbortChan = make(chan struct{})
				fmt.Printf("Starting new mining thread\n")
				go em.Miner.Mine(currentWork, minerAbortChan, minerFoundChan)
			}
		case subWork := <-minerFoundChan:
			fmt.Printf("Found work after %v\n", time.Since(start))
			if time.Since(start) < nextDelay {
				// Start another miner because the minimum time has not passed yet
				fmt.Printf("Delaying block...\n")
				currentWork = nil
				break
			}
			fmt.Printf("Submiting work: %v\n", subWork)
			result, err := em.SubmitWork(subWork)
			if err != nil || result != true {
				fmt.Printf("Submitted work got rejected, retrying...\n")
				currentWork = nil
				break
			}
			fmt.Printf("Mined Block Total Difficulty: %v\n", em.NextTotalDifficulty)
			return em.NextTotalDifficulty.Cmp(em.TTD.Int) >= 0, nil
		}
	}
}

func main() {
	var (
		delayConfs DelayCongfigurations
		rpcs       RPCUrls
		ttd        TTD
	)
	flag.Var(&delayConfs, "delay",
		"Block mining delay configuration in the form <BLOCK_NUMBER>,<SECONDS>, where BLOCK_NUMBER can also be \"TTD\" or \"Default\"")
	flag.Var(&rpcs, "rpc",
		"Execution client RPC endpoint and port")
	flag.Var(&ttd, "ttd",
		"Value of the Terminal Total Difficulty for the subscribed RPCs")
	flag.Parse()

	if len(rpcs) == 0 {
		panic("No RPCs provided")
	}

	// Create the single hive miner to use a single Dataset
	hiveminer := NewHiveMiner()

	abort := make(chan struct{})
	done := make(chan error)
	remEm := 0

	for _, rpcUrl := range rpcs {
		client := &http.Client{}
		rpcClient, _ := rpc.DialHTTPWithClient(rpcUrl, client)
		eth := ethclient.NewClient(rpcClient)

		em := &ExternalMiner{
			TTD:                 ttd,
			DelayConfigs:        delayConfs,
			Miner:               hiveminer,
			RPC:                 rpcClient,
			Eth:                 eth,
			NextTotalDifficulty: big.NewInt(0),
		}

		go em.LoopMineUntilTTD(abort, done)
		remEm++
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-sigs:
			close(abort)
			return
		case err := <-done:
			remEm--
			if err != nil {
				fmt.Printf("External miner returned error: %v\n", err)
			}
			if remEm == 0 {
				fmt.Printf("All external miners are done\n")
				return
			}
		}
	}

}
