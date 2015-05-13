package matchbox

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot(t *testing.T) {
	assert := assert.New(t)
	ctrie := newCtrie(NewAMQPConfig())
	sub := subscriber("abc")
	for i := 0; i < 100; i++ {
		ctrie.Insert(strconv.Itoa(i), sub)
	}
	for i := 0; i < 100; i++ {
		assert.Equal([]Subscriber{sub}, ctrie.Lookup(strconv.Itoa(i)))
	}
	assert.Equal([]Subscriber{}, ctrie.Lookup("blah"))

	snapshot := ctrie.Snapshot()
	for i := 0; i < 100; i++ {
		assert.Equal([]Subscriber{sub}, snapshot.Lookup(strconv.Itoa(i)))
	}
	assert.Equal([]Subscriber{}, snapshot.Lookup("blah"))

	// Ensure modifying snapshot doesn't affect original.
	snapshot.Remove("0", sub)
	assert.Equal([]Subscriber{}, snapshot.Lookup("0"))
	assert.Equal([]Subscriber{sub}, ctrie.Lookup("0"))

	// Ensure modifying original doesn't affect snapshot.
	ctrie.Insert("foo", sub)
	assert.Equal([]Subscriber{sub}, ctrie.Lookup("foo"))
	assert.Equal([]Subscriber{}, snapshot.Lookup("foo"))

	// Ensure snapshots-of-snapshots work as expected.
	snapshot2 := snapshot.Snapshot()
	for i := 1; i < 100; i++ {
		assert.Equal([]Subscriber{sub}, snapshot2.Lookup(strconv.Itoa(i)))
	}
	snapshot2.Insert("0", sub)
	assert.Equal([]Subscriber{sub}, snapshot2.Lookup("0"))
	assert.Equal([]Subscriber{}, snapshot.Lookup("0"))

	// Ensure read-only snapshots panic on write.
	snapshot = ctrie.ReadOnlySnapshot()
	defer func() {
		assert.NotNil(recover())
	}()
	snapshot.Remove("0", sub)

	assert.Equal(snapshot, snapshot.ReadOnlySnapshot())
}

func TestConcurrency(t *testing.T) {
	assert := assert.New(t)
	ctrie := newCtrie(NewAMQPConfig())
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < 1000; i++ {
			ctrie.Insert(strconv.Itoa(i), subscriber(i))
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			val := ctrie.Lookup(strconv.Itoa(i))
			if len(val) > 0 {
				assert.Equal(subscriber(i), val[0])
			}
		}
		wg.Done()
	}()

	for i := 0; i < 1000; i++ {
		time.Sleep(5)
		ctrie.Remove(strconv.Itoa(i), subscriber(i))
	}

	wg.Wait()
}
