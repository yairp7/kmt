// benchcheck compares `go test -bench -benchmem` output against a recorded baseline.
//
// ns/op may be at most -tolerance percent slower than the baseline (faster is always fine);
// allocs/op must match exactly.
// With -count > 1 the median of each benchmark's samples is used.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type result struct {
	ns     []float64
	allocs []float64
}

func parse(path string) (map[string]*result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]*result{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		name := fields[0]
		// Strip the trailing -GOMAXPROCS suffix so runners with different core counts match.
		if i := strings.LastIndex(name, "-"); i > 0 {
			if _, err := strconv.Atoi(name[i+1:]); err == nil {
				name = name[:i]
			}
		}
		r := out[name]
		if r == nil {
			r = &result{}
			out[name] = r
		}
		for i := 2; i < len(fields); i++ {
			v, err := strconv.ParseFloat(fields[i-1], 64)
			if err != nil {
				continue
			}
			switch fields[i] {
			case "ns/op":
				r.ns = append(r.ns, v)
			case "allocs/op":
				r.allocs = append(r.allocs, v)
			}
		}
	}
	return out, sc.Err()
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func main() {
	baselinePath := flag.String("baseline", "", "recorded benchmark output")
	currentPath := flag.String("current", "", "new benchmark output")
	tolerance := flag.Float64("tolerance", 20, "allowed ns/op slowdown in percent")
	flag.Parse()

	base, err := parse(*baselinePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "baseline:", err)
		os.Exit(2)
	}
	cur, err := parse(*currentPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "current:", err)
		os.Exit(2)
	}
	if len(base) == 0 {
		fmt.Fprintln(os.Stderr, "baseline has no benchmarks")
		os.Exit(2)
	}

	names := make([]string, 0, len(base))
	for n := range base {
		names = append(names, n)
	}
	sort.Strings(names)

	failed := false
	for _, name := range names {
		b := base[name]
		c, ok := cur[name]
		if !ok || len(c.ns) == 0 || len(b.ns) == 0 {
			fmt.Printf("FAIL %s: missing from current run\n", name)
			failed = true
			continue
		}

		bns, cns := median(b.ns), median(c.ns)
		delta := (cns - bns) / bns * 100
		status := "ok  "
		var problems []string

		if delta > *tolerance {
			status = "FAIL"
			problems = append(problems, fmt.Sprintf("time %+.1f%% exceeds +%.0f%%", delta, *tolerance))
		}
		if len(b.allocs) > 0 && len(c.allocs) > 0 {
			ba, ca := median(b.allocs), median(c.allocs)
			if ba != ca {
				status = "FAIL"
				problems = append(problems, fmt.Sprintf("allocs/op %v -> %v", ba, ca))
			}
		}

		fmt.Printf("%s %s: %.4g -> %.4g ns/op (%+.1f%%)", status, name, bns, cns, delta)
		if len(problems) > 0 {
			failed = true
			fmt.Printf(" [%s]", strings.Join(problems, "; "))
		}
		fmt.Println()
	}

	for name := range cur {
		if _, ok := base[name]; !ok {
			fmt.Printf("note %s: not in baseline (ignored)\n", name)
		}
	}

	if failed {
		os.Exit(1)
	}
}
