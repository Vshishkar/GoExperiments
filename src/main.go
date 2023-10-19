package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// while there are urls to proccess master will schedule agents to do so if url hasn't been proceesed.

func merge(cs ...<-chan []string) <-chan []string {
	var wg sync.WaitGroup
	out := make(chan []string)
	output := func(c <-chan []string) {
		for msg := range c {
			out <- msg
		}
		wg.Done()
	}
	wg.Add(len(cs))

	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func subscribe(listener chan []string, cs ...<-chan []string) <-chan []string {
	var wg sync.WaitGroup
	output := func(c <-chan []string) {
		for msg := range c {
			listener <- msg
		}
		wg.Done()
	}
	wg.Add(len(cs))

	for _, c := range cs {
		go output(c)
	}

	return listener
}

func agent(url string, fetcher Fetcher) <-chan []string {
	c := make(chan []string)
	go func() {
		body, urls, err := fetcher.Fetch(url)
		if err == nil {
			fmt.Printf("found: %s %q\n", url, body)
			c <- urls
		} else {
			fmt.Println(err)
			c <- []string{}
		}
		defer close(c)
	}()
	return c
}

func Coordinator(startUrl string, fetcher Fetcher) {
	agentsCount := 0
	fetched := make(map[string]bool)

	agentListener := make(chan []string)

	urls_c := agent(startUrl, fetcher)
	subscribe(agentListener, urls_c)
	fetched[startUrl] = true
	agentsCount += 1

	for urls := range agentListener {
		for _, url := range urls {
			if !fetched[url] {
				agentsCount += 1
				subscribe(agentListener, agent(url, fetcher))
				fetched[url] = true
			}
		}

		agentsCount -= 1
		if agentsCount == 0 {
			close(agentListener)
		}
	}
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Sequential(url string, depth int, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		Sequential(u, depth-1, fetcher)
	}
	return
}

func main() {
	Sequential("https://golang.org/", 4, fetcher)
	fmt.Println("___________________________")
	Coordinator("https://golang.org/", fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
