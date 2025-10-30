package tools

// PanicError panics if error is not nil
func PanicError(err error) {
	if err != nil {
		panic(err)
	}
}
