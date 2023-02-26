package surfstore

import (
	context "context"
	"database/sql"
	"log"
	"os"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := rpcClient.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := rpcClient.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := connectToGrpcServer(blockStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retBlockHashes, err := rpcClient.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	blockHashes = &retBlockHashes.Hashes

	// close the connection
	return conn.Close()
}


func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, err := connectToGrpcServer(surfClient.MetaStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileInfoMap, err := rpcClient.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = fileInfoMap.FileInfoMap

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, err := connectToGrpcServer(surfClient.MetaStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	version, err := rpcClient.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = version.Version

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	conn, err := connectToGrpcServer(surfClient.MetaStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var blockHashes *BlockHashes = &BlockHashes{Hashes: blockHashesIn}
	retBlockStoreMap, err := rpcClient.GetBlockStoreMap(ctx, blockHashes)
	if err != nil {
		conn.Close()
		return err
	}
	for serverAddr, blockHashes := range retBlockStoreMap.BlockStoreMap {
		_, exists := (*blockStoreMap)[serverAddr]
		if exists {
			(*blockStoreMap)[serverAddr] = append((*blockStoreMap)[serverAddr], blockHashes.Hashes...)
		} else {
			(*blockStoreMap) = make(map[string][]string)
			(*blockStoreMap)[serverAddr] = append((*blockStoreMap)[serverAddr], blockHashes.Hashes...)
		}
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	conn, err := connectToGrpcServer(surfClient.MetaStoreAddr)
	if err != nil {
		return err
	}
	rpcClient := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retBlockStoreAddr, err := rpcClient.GetBlockStoreAddrs(ctx,  &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	blockStoreAddrs = &retBlockStoreAddr.BlockStoreAddrs
	
	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

func createIndexDbFile(path string) error {
	indexFile, e := os.Create(path)
	if e != nil {
		log.Fatal("Error During creating file", e)
		return e
	}
	// log.Println("index db created")
	indexFile.Close()
	db, err := sql.Open("sqlite3", path)
	defer db.Close()
	if err != nil {
		log.Fatal("Error during opening index.db file", err)
		return err
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During creating prepare statement for createTable", err)
	}
	_, err = statement.Exec()
	if err != nil {
		log.Fatal("Error while executing the statement", err)
		return err
	}
	statement.Close()
	// log.Println("table created")
	return nil
}

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); os.IsNotExist(err) {
		err := createIndexDbFile(outputMetaPath)
		if err != nil {
			log.Fatal("Error while creating index db file", err)
		}
	}
	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}

func connectToGrpcServer(storeAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(storeAddr, grpc.WithInsecure())
	return conn, err
}