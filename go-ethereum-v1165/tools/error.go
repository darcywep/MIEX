package tools

// PanicError panics if error is not nil
func PanicError(info string, err error) {
	if err != nil {
		panic(info + err.Error())
	}
}
