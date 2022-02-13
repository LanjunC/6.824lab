package mr

import (
	"testing"
	"time"
)

// Example
/*
//  calc_test.go
func TestMul(t *testing.T) {
	cases := []struct {
		Name           string
		A, B, Expected int
	}{
		{"pos", 2, 3, 6},
		{"neg", 2, -3, -6},
		{"zero", 2, 0, 0},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if ans := Mul(c.A, c.B); ans != c.Expected {
				t.Fatalf("%d * %d expected %d, but %d got",
					c.A, c.B, c.Expected, ans)
			}
		})
	}
}
 */

func TestTaskQueue(t *testing.T) {
	tq := NewTaskQueue(2, 10 * time.Millisecond)

	cases := []struct {
		Name           string
		Action 		   string
		data			int
		Expected interface{}
	}{
		{"push10","push",10,  true},
		{"push20","push",20, true},
		{"push30","push",30, false},
		{"pop","pop",0, 10},
		{"pop","pop",0, 20},
		{"pop","pop",0, nil},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var res interface{}
			if c.Action == "push" {
				res = tq.Push(c.data)

			} else {
				res,_ = tq.Pop()
			}
			if res != c.Expected {
				t.Fatalf("%v, expected %v, but got %v", c.Name, c.Expected, res)
			}
			t.Logf("sizeof queue=%v", tq.Size())
		})
	}
}
