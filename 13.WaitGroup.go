package main

import (
	"fmt"
	"time"
)

// MyWaitGroup - WaitGroup на канале с динамическим буфером
type MyWaitGroup struct {
	sema chan struct{} // текущий канал для токенов
	n    int           // ожидаемое количество завершений
}

// NewMyWaitGroup создаёт группу с нулевым буфером
func NewMyWaitGroup() *MyWaitGroup {

	return &MyWaitGroup{
		sema: make(chan struct{}), // небуферизированный канал
	}
}

// Add увеличивает счётчик ожидаемых горутин на delta
func (wg *MyWaitGroup) Add(delta int) {

	newBufSize := wg.n + delta
	if newBufSize < 0 {
		panic("отрицательный счётчик")
	}

	// создаём новый канал с буфером newBufSize
	newSema := make(chan struct{}, newBufSize)

	// закрываем старый канал
	close(wg.sema)

	// переносим все токены, которые уже были отправлены в старый канал
	for range wg.sema {
		newSema <- struct{}{}
	}

	wg.sema = newSema
	wg.n = newBufSize
}

// Done отмечает завершение горутины
func (wg *MyWaitGroup) Done() {

	wg.sema <- struct{}{}
}

// Wait блокируется до получения n токенов
func (wg *MyWaitGroup) Wait() {

	for i := 0; i < wg.n; i++ {
		<-wg.sema
	}
}

func main() {

	numbers := []int{1, 2, 3, 4, 5}

	wg := NewMyWaitGroup() // буфер 0

	for _, num := range numbers {
		wg.Add(1) // увеличиваем счётчик до запуска горутины
		go func(x int) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // создаём вид бурной деятельности
			fmt.Println(x)
		}(num)
	}

	wg.Wait()
	fmt.Println("Программа завершена.")
}
