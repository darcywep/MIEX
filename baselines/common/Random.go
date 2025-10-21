package common

import (
	"Janus/baselines/utils"
)

// Random 通过嵌入外部包的 util.Random 来“继承”其能力
// 例如需要用到的：UniformDist(a,b uint64) uint64、RandStr(length int, charset string) string、SetSeed/GetSeed等
type Random struct {
	utils.Random
}

// NonUniformDistribution 实现： (uniform(0,A) | uniform(x,y)) % (y-x+1) + x
func (r *Random) NonUniformDistribution(A, x, y uint64) uint64 {
	return (r.UniformDist(0, A)|r.UniformDist(x, y))%(y-x+1) + x
}

// NString 生成由数字组成的随机串，长度在 [minLen, maxLen] 之间
func (r *Random) NString(minLen, maxLen int) string {
	// 这里依赖 util.Random 的 UniformDist 与 RandStr
	l := int(r.UniformDist(uint64(minLen), uint64(maxLen)))
	return r.RandStr(l, numeric())
}

// RandZIP 生成邮编风格串：先生成 4 位数字，再追加 "11111"
func (r *Random) RandZIP() string {
	zip := r.NString(4, 4)
	zip += "11111"
	return zip
}

// RandLastName 根据 n 的百位、十位、个位拼接出姓氏（TPCC 规则）
func (r *Random) RandLastName(n int) string {
	ln := customerLastNames()
	s1 := ln[(n/100)%10]
	s2 := ln[(n/10)%10]
	s3 := ln[n%10]
	return s1 + s2 + s3
}

// --------- 本文件内部用到的静态表 ---------

func customerLastNames() []string {
	return []string{
		"BAR", "OUGHT", "ABLE", "PRI", "PRES",
		"ESE", "ANTI", "CALLY", "ATION", "EING",
	}
}

func numeric() string {
	return "0123456789"
}
