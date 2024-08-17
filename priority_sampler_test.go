package work

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrioritySampler(t *testing.T) {
	t.Skip("skipped, it's flaky due to randomization") // FIXME: flaky test
	ps := prioritySampler{}

	ps.add(5, "jobs.5", "jobsinprog.5", "jobspaused.5", "jobslock.5", "jobslockinfo.5", "jobsconcurrency.5")
	ps.add(2, "jobs.2a", "jobsinprog.2a", "jobspaused.2a", "jobslock.2a", "jobslockinfo.2a", "jobsconcurrency.2a")
	ps.add(1, "jobs.1b", "jobsinprog.1b", "jobspaused.1b", "jobslock.1b", "jobslockinfo.1b", "jobsconcurrency.1b")

	var c5 = 0
	var c2 = 0
	var c1 = 0
	var c1end = 0
	var total = 200
	for i := 0; i < total; i++ {
		ret := ps.sample()
		if ret[0].priority == 5 {
			c5++
		} else if ret[0].priority == 2 {
			c2++
		} else if ret[0].priority == 1 {
			c1++
		}
		if ret[2].priority == 1 {
			c1end++
		}
	}

	t.Logf("c5=%d c2=%d c1=%d c1end=%d", c5, c2, c1, c1end)

	// make sure these numbers are roughly correct. note that probability is a thing.
	assert.True(t, c5 > (2*c2), "c5 > 2*c2")
	assert.True(t, float64(c2) > (1.5*float64(c1)), "c2 > 1.5*c1")
	assert.True(t, c1 >= (total/13), "c1 >= total/13")
	assert.True(t, float64(c1end) > (float64(total)*0.50), "c1end > total*0.5")
}

func BenchmarkPrioritySampler(b *testing.B) {
	ps := prioritySampler{}
	for i := 0; i < 200; i++ {
		ps.add(uint(i)+1,
			"jobs."+fmt.Sprint(i),
			"jobsinprog."+fmt.Sprint(i),
			"jobspaused."+fmt.Sprint(i),
			"jobslock."+fmt.Sprint(i),
			"jobslockinfo."+fmt.Sprint(i),
			"jobsmaxconcurrency."+fmt.Sprint(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.sample()
	}
}
