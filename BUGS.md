# Bugs I Hit

Document every non-trivial bug here. This becomes interview material.

---

## Template (copy for each bug)

### [Short title] — Part N

- **Symptom:** What you observed
- **Root cause:** Why it happened
- **How I found it:** Logging, test, diagram, race detector, etc.
- **Fix:** What you changed
- **Lesson:** What you'd tell an interviewer

---

## Example entry (delete this once you have real ones)

### Split vote loop — Part 2

- **Symptom:** All 3 nodes cycled Candidate forever, no leader for 10+ seconds
- **Root cause:** Fixed election timeout (200ms) on all nodes — they always timed out together
- **How I found it:** Added state transition logs; saw all nodes start election in same millisecond
- **Fix:** Randomized timeout between 150–300ms
- **Lesson:** Raft's randomization isn't optional — it's what makes liveness probable
