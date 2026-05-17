package main

import (
	"fmt"
	"time"
)

// Worker демонстрирует паттерн done channel:
// done   - канал для внешней команды "завершайся",
// doneCh - канал, который горутина закрывает при фактическом выходе
type Worker struct {
	done   chan struct{}
	doneCh chan struct{}
}

// NewWorker создаёт Worker и запускает его горутину
func NewWorker() *Worker {

	worker := &Worker{
		done:   make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// горутина, выполняющая периодическую работу
	go func() {

		ticker := time.NewTicker(500 * time.Millisecond)

		defer func() {
			ticker.Stop()
			close(worker.doneCh) // сигнализируем о реальном завершении горутины
			fmt.Println("worker завершил работу и закрыл doneCh.")
		}()

		for {
			select {
			case <-worker.done:
				fmt.Println("worker получил сигнал завершения работы.")
				return
			case <-ticker.C:
				fmt.Println("worker выполняет полезную работу.")
			}
		}
	}()

	return worker
}

// Shutdown инициирует завершение горутины и ожидает его.
// Закрытие done - сигнал горутине остановиться,
// чтение doneCh - подтверждение, что горутина действительно завершилась.
func (w *Worker) Shutdown() {
	close(w.done)
	<-w.doneCh
}

func main() {

	worker := NewWorker()

	// даём поработать 3 секунды
	time.Sleep(3 * time.Second)

	// останавливаем worker и ждём полного завершения
	worker.Shutdown()

	fmt.Println("Программа завершена.")
}
