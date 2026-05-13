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
	countNums = 10
)

// generator отправляет числа от 0 до countNums в канал
func generator(ctx context.Context) chan int {

	out := make(chan int)

	go func() {

		defer close(out)

		for i := 0; i < countNums; i++ {
			select {
			case <-ctx.Done():
				fmt.Printf("\ngenerator завершён по отмене контекста.\n")
				return
			case out <- i:
			}
		}

		fmt.Printf("\ngenerator завершил отправку.\n")
	}()

	return out
}

// tee разделяет входящий канал на два исходящих и дублирует данные в оба канала
func tee(ctx context.Context, in chan int) (chan int, chan int) {

	out1 := make(chan int)
	out2 := make(chan int)

	go func() {

		defer func() {
			close(out1)
			close(out2)
			fmt.Println("tee завершён.")
		}()

		for {

			select {
			case <-ctx.Done():
				fmt.Printf("tee завершается по отмене контекста.\n")
				return
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. tee завершается.\n")
					return
				}

				select {
				case <-ctx.Done():
					fmt.Printf("tee завершается по отмене контекста.\n")
					return
				default:

					select {
					case <-ctx.Done():
						fmt.Printf("tee завершается по отмене контекста.\n")
						return
					case out1 <- v:
					}
					select {
					case <-ctx.Done():
						fmt.Printf("tee завершается по отмене контекста.\n")
						return
					case out2 <- v:
					}
				}
			}
		}
	}()

	return out1, out2
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wgSH sync.WaitGroup

	wgSH.Add(1)
	go signalHandler(ctx, cancel, &wgSH)

	numsChan := generator(ctx)
	ch1, ch2 := tee(ctx, numsChan)

	nums1 := make([]int, 0)
	nums2 := make([]int, 0)

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for v := range ch1 {
			nums1 = append(nums1, v)
		}
	}()
	go func() {
		defer wg.Done()
		for v := range ch2 {
			nums2 = append(nums2, v)
		}
	}()

	wg.Wait()

	fmt.Println(nums1)
	fmt.Println(nums2)

	cancel()
	wgSH.Wait()

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
