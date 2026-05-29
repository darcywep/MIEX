package main

import (
	"Janus/ethereum/replay/replay_gethcopy"
	"Janus/tools"
)

func main() {
	tools.JournalNonce = true
	//mode := flag.String("mode", "", "replay mode: geth or copy")
	//flag.Parse()
	//
	//if *mode == "geth" {
	//	replay_geth.ReplayGeth()
	//} else {
	//	replay_gethcopy.ReplayCopy()
	//}
	//replay_geth.ReplayGeth()
	//replay_gethcopy.ReplayCopy()
	replay_gethcopy.ReplayWithRecordOpCodeTiming()
	//if err := replay_gethcopy.TestReadReplayLatency(); err != nil {
	//	fmt.Println("Replay latency read test error:", err)
	//}
}
