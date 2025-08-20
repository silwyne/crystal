package preconditions

import (
	"testing"
)

func expectPanic(t *testing.T, fn func(), expectedMsg string) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			if msg, ok := r.(string); ok {
				if msg != expectedMsg {
					t.Errorf("Expected panic with message '%s', but got '%s'", expectedMsg, msg)
				} else {
					return
				}
			} else {
				t.Errorf("Expected panic with string message, but got type %T", r)
			}
		} else {
			t.Errorf("Expected function to panic, but it did not")
		}
	}()

	fn()
}

func TestCheckNotEmpty_NonEmpty(t *testing.T) {
	strings := []string{"hello", "world"}
	CheckNotEmpty(strings, "This should not panic for non-empty string slice")

	ints := []int{1, 2, 3}
	CheckNotEmpty(ints, "This should not panic for non-empty int slice")
}

func TestCheckNotEmpty_Empty(t *testing.T) {
	emptyStrings := []string{}
	expectPanic(t, func() {
		CheckNotEmpty(emptyStrings, "String slice cannot be empty!")
	}, "String slice cannot be empty!")

	emptyInts := []int{}
	expectPanic(t, func() {
		CheckNotEmpty(emptyInts, "Int slice cannot be empty!")
	}, "Int slice cannot be empty!")

	emptyBools := []bool{}
	expectPanic(t, func() {
		CheckNotEmpty(emptyBools, "Bool slice cannot be empty!")
	}, "Bool slice cannot be empty!")
}

func TestCheckNotEmpty_NilSlice(t *testing.T) {
	var nilSlice []string
	expectPanic(t, func() {
		CheckNotEmpty(nilSlice, "Nil slice cannot be empty!")
	}, "Nil slice cannot be empty!")
}
