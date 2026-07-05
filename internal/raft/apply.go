package raft

import "time"

func (n *Node) applyLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log[n.lastApplied-1]
				n.mu.Unlock()

				result := n.store.Apply(entry.Command)

				n.mu.Lock()
				if ch, ok := n.applyWaiters[n.lastApplied]; ok {
					ch <- result
					delete(n.applyWaiters, n.lastApplied)
				}
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) replayCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		n.store.Apply(entry.Command)
	}
}
