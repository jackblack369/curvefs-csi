package util

import "testing"

// TestGetPodLock function
func TestGetPodLock(t *testing.T) {
	podHashVal := "examplePodID"

	// Get the lock for the first time
	lock1 := GetPodLock(podHashVal)

	// Get the lock for the second time
	lock2 := GetPodLock(podHashVal)

	// Check if both locks point to the same address
	if lock1 != lock2 {
		t.Errorf("Expected the same lock for podHashVal %s, but got different locks", podHashVal)
	}

	// Check if the lock is actually working
	lock1.Lock()
	defer lock1.Unlock()

	locked := make(chan bool, 1)
	go func() {
		lock2.Lock()
		defer lock2.Unlock()
		locked <- true
	}()

	select {
	case <-locked:
		t.Errorf("Expected the lock to be held, but it was acquired by another goroutine")
	default:
		// Lock is held as expected
	}
}
