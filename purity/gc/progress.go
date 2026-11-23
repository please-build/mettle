package gc

import (
	"os"
	"strings"
	"time"

	"golang.org/x/term"
)

// newProgressBar returns a channel to a progress bar that animates until the channel is closed.
// The caller should push ints onto the channel indicating incremental progress.
func newProgressBar(message string, total int) chan<- int {
	bar := &progressBar{
		message: message,
		total:   total,
		ch:      make(chan int, 10),
	}
	if cols, _, err := term.GetSize(int(os.Stderr.Fd())); err == nil {
		bar.cols = cols
	}
	go bar.Animate()
	return bar.ch
}

type progressBar struct {
	message              string
	total, current, cols int
	nextUpdate           time.Time
	ch                   chan int
}

func (bar *progressBar) Animate() {
	for inc := range bar.ch {
		bar.current += inc
		proportion := float64(bar.current) / float64(bar.total)
		percentage := 100.0 * proportion
		now := time.Now()
		if now.After(bar.nextUpdate) {
			bar.nextUpdate = now.Add(50 * time.Millisecond)
			if bar.cols == 0 {
				bar.Printf("%0.1f%%\n", percentage)
			} else {
				bar.render(proportion, percentage)
			}
		}
	}
	bar.Printf("\n")
}

func (bar *progressBar) render(proportion, percentage float64) {
	totalCols := bar.cols - 40 // Pretty arbitrary amount of overhead to make sure we have space.
	currentPos := int(proportion * float64(totalCols))
	if currentPos > totalCols {
		currentPos = totalCols
	}
	before := strings.Repeat("=", currentPos)
	after := strings.Repeat(" ", totalCols-currentPos)
	bar.Printf("${RESETLN}${BOLD_WHITE}%s: ${GREY}[%s>%s] ${BOLD_WHITE}%0.1f%%${RESET}", bar.message, before, after, percentage)
}
