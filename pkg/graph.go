package etcdkit

import (
	"context"
	"fmt"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Graph struct {
	client *clientv3.Client
	prefix string
}

func (g *Graph) AddNode(ctx context.Context, nodeID string, data string) error {
	_, err := g.client.Put(ctx, fmt.Sprintf("%s/nodes/%s", g.prefix, nodeID), data)
	return err
}

func (g *Graph) AddEdge(ctx context.Context, fromNode, toNode string) error {
	_, err := g.client.Put(ctx, fmt.Sprintf("%s/edges/%s/%s", g.prefix, fromNode, toNode), "")
	return err
}

func (g *Graph) GetNeighbors(ctx context.Context, nodeID string) ([]string, error) {
	resp, err := g.client.Get(ctx, fmt.Sprintf("%s/edges/%s/", g.prefix, nodeID), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	neighbors := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		neighbors[i] = parts[len(parts)-1]
	}
	return neighbors, nil
}
