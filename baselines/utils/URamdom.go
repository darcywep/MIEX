package utils

import (
	"math/rand"
	"strings"
	"time"
)

type Random struct {
	seed uint64
	rng  *rand.Rand
}

// NewRandom creates a new Random object with an optional seed value
func NewRandom(seed uint64) *Random {
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	return &Random{
		seed: seed,
		rng:  rand.New(rand.NewSource(int64(seed))),
	}
}

// SetSeed sets a new seed for the random number generator
func (r *Random) SetSeed(seed uint64) {
	r.seed = seed
	r.rng = rand.New(rand.NewSource(int64(seed)))
}

// GetSeed returns the current seed used in the random number generator
func (r *Random) GetSeed() uint64 {
	return r.seed
}

// Next generates a 64-bit random number by combining two 32-bit random numbers
func (r *Random) Next() uint64 {
	return (uint64(r.rng.Int31()) << 32) + uint64(r.rng.Int31())
}

// NextBits generates a random number with the specified number of bits
func (r *Random) NextBits(bits int) uint64 {
	return uint64(r.rng.Int31n(1 << bits))
}

// NextDouble generates a random double in the range [0.0, 1.0)
func (r *Random) NextDouble() float64 {
	return float64(r.NextBits(26)<<27+r.NextBits(27)) / float64(1<<53)
}

// UniformDist generates a random number in the range [a, b]
func (r *Random) UniformDist(a, b uint64) uint64 {
	if a == b {
		return a
	}
	return r.Next()%(b-a+1) + a
}

// RandStr generates a random string of a given length from a specified character set
func (r *Random) RandStr(length int, charset string) string {
	var result strings.Builder
	strLen := len(charset)
	for i := 0; i < length; i++ {
		k := r.UniformDist(0, uint64(strLen-1))
		result.WriteByte(charset[k])
	}
	return result.String()
}

// AString generates a random string with a length between minLen and maxLen
func (r *Random) AString(minLen, maxLen int) string {
	length := int(r.UniformDist(uint64(minLen), uint64(maxLen)))
	return r.RandStr(length, r.alpha())
}

// alpha returns the default alphanumeric string used for generating random strings
func (r *Random) alpha() string {
	return "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
}

//
//func main() {
//	// Example usage
//	randGen := NewRandom(1234)
//
//	// Generate random numbers
//	fmt.Println(randGen.Next())              // Random 64-bit number
//	fmt.Println(randGen.NextDouble())        // Random double [0.0, 1.0)
//	fmt.Println(randGen.UniformDist(10, 50)) // Random number between 10 and 50
//
//	// Generate random strings
//	fmt.Println(randGen.RandStr(10, "abcde")) // Random string of length 10
//	fmt.Println(randGen.AString(5, 10))       // Random string with length between 5 and 10
//}
