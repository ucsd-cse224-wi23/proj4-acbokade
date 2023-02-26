package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	var responsibleServerAddr = ""
	blockHash := c.Hash(blockId)
	firstServerAddr := ""
	for serverHash, serverAddr := range c.ServerMap {
		if serverHash > blockHash {
			responsibleServerAddr = serverAddr
			break
		}
		if len(firstServerAddr) == 0 {
			firstServerAddr = serverAddr
		}
	}
	if len(responsibleServerAddr) == 0 {
		responsibleServerAddr = firstServerAddr
	}
	return responsibleServerAddr
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	var serverMap map[string]string
	var consistentHashRing *ConsistentHashRing = &ConsistentHashRing{}
	for _, serverAddr := range serverAddrs {
		serverHash := consistentHashRing.Hash(serverAddr)
		serverMap[serverHash] = serverAddr
	}
	consistentHashRing.ServerMap = serverMap
	return consistentHashRing
}
