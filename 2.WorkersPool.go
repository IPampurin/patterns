package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	maxNumToProcess = 1000000 // количество обрабатываемых чисел
	countWorkers    = 5       // количество воркеров в пуле
)

// generator отправляет числа от 0 до n в канал
func generator(ctx context.Context, n int) chan int {

	out := make(chan int)

	go func() {
		defer close(out)
		for i := range n {
			select {
			case <-ctx.Done():
				fmt.Println("\ngenerator завершён по отмене контекста.")
				return
			case out <- i:
			}
		}
		fmt.Println("\ngenerator завершил отправку.")
	}()

	return out
}

// workersPool слушает канал и выполняет работу до закрытия канала или отмены контекста
func workersPool(ctx context.Context, countWorkers int, in chan int) chan int {

	out := make(chan int)
	var wg sync.WaitGroup

	for i := 0; i < countWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					fmt.Printf("воркер %d завершён по отмене контекста.\n", i)
					return
				case v, ok := <-in:
					if !ok {
						fmt.Printf("воркер %d завершён по закрытию входного канала.\n", i)
						return
					}

					select {
					case <-ctx.Done():
						fmt.Printf("в воркере %d передача данных прервана по отмене контекста, воркер завершён.\n", i)
						return
					case out <- v * 2:
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	numsChan := generator(ctx, maxNumToProcess)
	resChan := workersPool(ctx, countWorkers, numsChan)

	for v := range resChan {
		if v%10000 == 0 {
			fmt.Println(v)
		}
	}

	cancel()
	wg.Wait()

	fmt.Println("Программа завершена.")
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
