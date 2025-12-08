package main

import "Janus/ethereum/replay/replay_gethcopy"

func main() {
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
}
