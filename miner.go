package main

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	two256 = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0))
)

func ethashDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".ethash")
}

type HiveMiner struct {
	Ethash *ethash.Ethash
}

type SubmittableWork struct {
	Nonce         types.BlockNonce
	PoWHeaderHash common.Hash
	MixDigest     common.Hash
}

func (w SubmittableWork) String() string {
	return fmt.Sprintf("Nonce=%s, PoWHeaderHash=%s, MixDigest=%s", hexutil.Encode(w.Nonce[:]), w.PoWHeaderHash.Hex(), w.MixDigest.Hex())
}

type WorkDescription struct {
	PoWHeaderHash common.Hash
	SeedHash      common.Hash
	Target        *big.Int
	Difficulty    *big.Int
	BlockNumber   uint64
}

func (w WorkDescription) String() string {
	return fmt.Sprintf("PoWHeaderHash=%s, SeedHash=%s, Target=%v, Difficulty=%v, BlockNumber=%d",
		w.PoWHeaderHash.Hex(), w.SeedHash.Hex(), w.Target, w.Difficulty, w.BlockNumber)
}

func NewHiveMiner() *HiveMiner {
	config := ethash.Config{
		PowMode:        ethash.ModeNormal,
		CachesInMem:    2,
		DatasetsOnDisk: 2,
		DatasetDir:     ethashDir(),
	}

	etha := ethash.New(config, nil, false)

	fmt.Printf("Generating DAG... ")
	etha.Dataset(0, false)
	fmt.Printf("Complete\n")

	return &HiveMiner{
		Ethash: etha,
	}
}

// mine is the actual proof-of-work miner that searches for a nonce starting from
// seed that results in correct final block difficulty.
func (m *HiveMiner) Mine(work *WorkDescription, abort <-chan struct{}, found chan *SubmittableWork) {
	var (
		hash   = work.PoWHeaderHash.Bytes()
		target = work.Target
		number = work.BlockNumber
		d      = m.Ethash.Dataset(number, false)
	)
	// Start generating random nonces until we abort or find a good one
	var (
		totalAttempts = int64(0)
		attempts      = int64(0)
		nonce         = uint64(0)
		powBuffer     = new(big.Int)
	)
search:
	for {
		select {
		case <-abort:
			// Mining terminated, update stats and abort
			break search

		default:
			// We don't have to update hash rate on every nonce, so update after after 2^X nonces
			attempts++
			if (attempts % (1 << 15)) == 0 {
				totalAttempts += attempts
				fmt.Printf("Total Attempts=%d, Nonce=%d\n", totalAttempts, nonce)
				attempts = 0
			}
			// Compute the PoW value of this nonce
			digest, result := ethash.HashimotoFull(d.Dataset, hash, nonce)
			if powBuffer.SetBytes(result).Cmp(target) <= 0 {

				subWork := &SubmittableWork{
					Nonce:         types.EncodeNonce(nonce),
					PoWHeaderHash: work.PoWHeaderHash,
					MixDigest:     common.BytesToHash(digest),
				}

				// Seal and return a block (if still needed)
				select {
				case found <- subWork:
				case <-abort:
				}
				break search
			}
			nonce++
		}
	}
	// Datasets are unmapped in a finalizer. Ensure that the dataset stays live
	// during sealing so it's not unmapped while being read.
	runtime.KeepAlive(d)
}
