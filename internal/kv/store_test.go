package kv

import "testing"

func TestStore(t *testing.T) {
	store := NewStore()

	t.Run("set returns empty result", func(t *testing.T) {
		got := store.Apply(Command{Op: "SET", Key: "key1", Value: "value1"})
		if got != (ApplyResult{}) {
			t.Errorf("Apply(SET) = %v, want empty result", got)
		}
	})

	t.Run("get after set", func(t *testing.T) {
		got := store.Apply(Command{Op: "GET", Key: "key1"})
		want := ApplyResult{Value: "value1", Found: true}
		if got != want {
			t.Errorf("Apply(GET) = %v, want %v", got, want)
		}
	})

	t.Run("get non-existent key", func(t *testing.T) {
		got := store.Apply(Command{Op: "GET", Key: "key2"})
		if got != (ApplyResult{Found: false}) {
			t.Errorf("Apply(GET) = %v, want {Found:false}", got)
		}
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		got := store.Apply(Command{Op: "DELETE", Key: "key3"})
		if got != (ApplyResult{Found: false}) {
			t.Errorf("Apply(DELETE) = %v, want {Found:false}", got)
		}
	})

	t.Run("delete existing key", func(t *testing.T) {
		got := store.Apply(Command{Op: "DELETE", Key: "key1"})
		if got != (ApplyResult{Found: true}) {
			t.Errorf("Apply(DELETE) = %v, want {Found:true}", got)
		}
	})

	t.Run("get deleted key", func(t *testing.T) {
		got := store.Apply(Command{Op: "GET", Key: "key1"})
		if got != (ApplyResult{Found: false}) {
			t.Errorf("Apply(GET) = %v, want {Found:false}", got)
		}
	})

	t.Run("set overwrite and get", func(t *testing.T) {
		store.Apply(Command{Op: "SET", Key: "key1", Value: "value2"})
		got := store.Apply(Command{Op: "GET", Key: "key1"})
		want := ApplyResult{Value: "value2", Found: true}
		if got != want {
			t.Errorf("Apply(GET) = %v, want %v", got, want)
		}
	})
}
