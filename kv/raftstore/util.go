package raftstore

import "fmt"

const(
	Debug3B bool = false
)

func DPrintf3B(format string, a ...interface{}) (n int, err error) {
	if Debug3B {
		fmt.Printf(format, a...)
	}
	return
}