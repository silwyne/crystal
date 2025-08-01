package preconditions

func CheckNotEmpty[T any](input []T, exception string) {
	if len(input) == 0 {
		panic(exception)
	}
}

func CheckTrue(input bool, exception string) {
	if !input {
		panic(exception)
	}
}
