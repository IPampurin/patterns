package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	countOutChans = 5
	numCount      = 100
)

// generator отправляет числа 0 до countOutChans в канал numCount раз
func generator(ctx context.Context, n int) chan int {

	out := make(chan int)

	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			num := rand.IntN(countOutChans)
			select {
			case <-ctx.Done():
				fmt.Printf("\ngenerator завершён по отмене контекста.\n")
				return
			case out <- num:
			}
		}
	}()

	return out
}

func fanOut(ctx context.Context, in chan int) []chan int {

	chs := make([]chan int, countOutChans)
	for i := range chs {
		chs[i] = make(chan int)
	}

	go func() {
		defer func() {
			for i := range chs {
				close(chs[i])
			}
			fmt.Println("fanOut завершён.")
		}()

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("fanOut завершается по отмене контекста.\n")
				return
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. fanOut завершается.\n")
					return
				}

				select {
				case <-ctx.Done():
					fmt.Printf("fanOut завершается по отмене контекста до отправки в канал №%d.\n", v)
					return
				case chs[v] <- v:
				}
			}
		}
	}()

	return chs
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	res := make([][]int, countOutChans)
	for i := range res {
		res[i] = make([]int, 0)
	}

	in := generator(ctx, numCount)
	chs := fanOut(ctx, in)

	var readersWg sync.WaitGroup
	for i := 0; i < countOutChans; i++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for v := range chs[i] {
				mu.Lock()
				res[v] = append(res[v], v)
				mu.Unlock()
			}
		}()
	}

	readersWg.Wait()
	cancel()
	wg.Wait()

	for i := range res {
		fmt.Println(res[i])
	}
}

// signalHandler слушает сигналы отмены
func signalHandler(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {

	defer wg.Done()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)

	select {
	case <-ctx.Done():
		fmt.Println("\nsignalHandler завершается по отмене контекста.")
		return
	case <-sig:
		cancel()
		fmt.Println("\nsignalHandler завершается по сигналу отмены.")
		return
	}
}
