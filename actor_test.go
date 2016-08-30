package glam

import (
	"fmt"
	"testing"
)

type A struct {
	x  int
	y  int
	in chan GetXRequest
	Actor
}

type B interface {
	Tricks() int
}

type GetXRequest struct {
	x   int
	out chan GetXResponse
}

type GetXResponse struct {
	x   int
	err interface{}
}

func (a A) GoX(x int) int {
	out := make(chan GetXResponse)
	a.in <- GetXRequest{x, out}
	return (<-out).x
}

func (a A) ProcessGetX() {
	for {
		request := <-a.in
		request.out <- GetXResponse{a.GetX(request.x), nil}
	}
}

func (a A) GetX(x int) int {
	return a.x + x
}

func (a A) DoPanic() int {
	panic(a.y)
}

func (a *A) Tricks() int {
	a.Defer((*A).LongTricks, a, a.x)
	return a.x
}

func (a A) LongTricks(x int) int {
	return x + 5
}

func TestSimpleCallFunction(t *testing.T) {
	fmt.Printf("TestSimpleCallFunction\n")
	var x int
	a := A{2, 3, nil, Actor{}}
	a.StartActor(a)

	if x := a.CallFunction(func() int { x = 6; return x })[0].(int); x != 6 {
		t.Errorf("Expected x = %v, actual %v\n", 6, x)
	}
	a.TerminateActor(true)
}

func TestGetX(t *testing.T) {
	fmt.Printf("TestGetX\n")
	a := A{2, 3, nil, Actor{}}
	a.StartActor(a)

	if x := a.CallStruct(A.GetX, 4)[0].(int); x != 6 {
		t.Errorf("Expected x = %v, actual %v\n", 6, x)
	}
}

func TestPanic(t *testing.T) {
	fmt.Printf("TestPanic\n")
	a := A{2, 3, nil, Actor{}}
	a.StartActor(a)

	defer func() {
		if e := recover(); e != nil {
			if response, ok := e.(Response); ok {
				if panicCause, ok2 := response.PanicCause().(int); !ok2 || panicCause != 3 {
					t.Errorf("Expected to receive panic response == 3, actual %v\n", panicCause)
				}
			} else {
				t.Errorf("Expected panic(Response), actual %v\n", e)
			}
		}
	}()

	a.CallStruct(A.DoPanic)
}

func TestDefer(t *testing.T) {
	fmt.Printf("TestDefer\n")
	a := A{3, 4, nil, Actor{}}
	a.StartActor(&a)
	if val := a.CallStruct((*A).Tricks)[0].(int); val != 8 {
		t.Errorf("Expected returning x+5, actual %v\n", val)
	}
}

func BenchmarkActor(b *testing.B) {
	b.StopTimer()
	a := A{5, 10, nil, Actor{}}
	a.StartActor(a)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		a.CallStruct(A.GetX, 3)
	}
}

func BenchmarkChannel(b *testing.B) {
	b.StopTimer()
	a := A{5, 10, make(chan GetXRequest), Actor{}}
	go a.ProcessGetX()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		a.GoX(3)
	}
}

func BenchmarkDeferred(b *testing.B) {
	b.StopTimer()
	a := A{5, 10, nil, Actor{}}
	a.StartActor(&a)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		a.CallStruct((*A).Tricks)
	}
}
