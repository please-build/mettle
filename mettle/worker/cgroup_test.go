package worker

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeCgroup(t *testing.T, dir string, current, anon, shmem, inactiveFile, maxEvents, oomKill int64) {
	t.Helper()
	files := map[string]string{
		"memory.current": itoa(current) + "\n",
		"memory.stat":    "anon " + itoa(anon) + "\nfile 1\nshmem " + itoa(shmem) + "\ninactive_file " + itoa(inactiveFile) + "\n",
		"memory.events":  "low 0\nhigh 0\nmax " + itoa(maxEvents) + "\noom 0\noom_kill " + itoa(oomKill) + "\n",
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0644))
	}
}

func itoa(i int64) string {
	return strconv.FormatInt(i, 10)
}

func TestReadCgroupMemory(t *testing.T) {
	dir := t.TempDir()
	writeCgroup(t, dir, 1000, 600, 50, 300, 0, 0)
	snap, err := readCgroupMemory(dir)
	require.NoError(t, err)
	assert.Equal(t, cgroupSnapshot{current: 1000, anon: 600, shmem: 50, inactiveFile: 300}, snap)
	assert.EqualValues(t, 700, snap.workingSet())
}

func TestWorkingSetNeverNegative(t *testing.T) {
	assert.EqualValues(t, 0, cgroupSnapshot{current: 10, inactiveFile: 20}.workingSet())
}

func TestReadCgroupMemoryErrors(t *testing.T) {
	dir := t.TempDir()
	_, err := readCgroupMemory(dir)
	assert.Error(t, err, "missing files")

	writeCgroup(t, dir, 1000, 600, 50, 300, 0, 0)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("anon notanumber\n"), 0644))
	_, err = readCgroupMemory(dir)
	assert.Error(t, err, "unparseable stat")
}

func TestReadCgroupLimit(t *testing.T) {
	dir := t.TempDir()
	assert.EqualValues(t, 0, readCgroupLimit(dir), "missing file")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0644))
	assert.EqualValues(t, 0, readCgroupLimit(dir), "no limit")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("34359738368\n"), 0644))
	assert.EqualValues(t, 34359738368, readCgroupLimit(dir))
}

func TestResolveCgroupDir(t *testing.T) {
	root := t.TempDir()
	proc := filepath.Join(t.TempDir(), "cgroup")
	nested := filepath.Join(root, "kubepods/pod1")
	require.NoError(t, os.MkdirAll(nested, 0755))

	// No memory.current anywhere yet.
	require.NoError(t, os.WriteFile(proc, []byte("0::/\n"), 0644))
	assert.Equal(t, "", resolveCgroupDir(root, proc))

	// Private cgroup namespace: the container's cgroup is the root.
	writeCgroup(t, root, 1, 1, 0, 0, 0, 0)
	assert.Equal(t, root, resolveCgroupDir(root, proc))

	// Host cgroup namespace: the path is needed.
	writeCgroup(t, nested, 1, 1, 0, 0, 0, 0)
	require.NoError(t, os.WriteFile(proc, []byte("0::/kubepods/pod1\n"), 0644))
	assert.Equal(t, nested, resolveCgroupDir(root, proc))

	// cgroup v1 has no "0::" line.
	require.NoError(t, os.WriteFile(proc, []byte("4:memory:/kubepods/pod1\n"), 0644))
	assert.Equal(t, "", resolveCgroupDir(root, proc))

	assert.Equal(t, "", resolveCgroupDir(root, filepath.Join(root, "missing")))
}

func TestSamplerRecordsPeaks(t *testing.T) {
	old := cgroupSampleInterval
	cgroupSampleInterval = 2 * time.Millisecond
	t.Cleanup(func() { cgroupSampleInterval = old })

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("4000\n"), 0644))
	writeCgroup(t, dir, 1000, 600, 50, 300, 1, 0)

	sampler := newCgroupSampler(dir)
	require.NotNil(t, sampler)
	stop := sampler.start()

	// Memory rises to a peak and falls again before the action ends.
	writeCgroup(t, dir, 3000, 2500, 400, 100, 1, 0)
	time.Sleep(50 * time.Millisecond)
	writeCgroup(t, dir, 1200, 700, 60, 300, 3, 1)

	s := stop()
	require.NotNil(t, s)
	assert.EqualValues(t, 4000, s.limitBytes)
	assert.EqualValues(t, 700, s.baselineWSBytes)
	assert.EqualValues(t, 2900, s.peakWSBytes)
	assert.EqualValues(t, 600, s.baselineAnon)
	assert.EqualValues(t, 2500, s.peakAnonBytes)
	assert.EqualValues(t, 400, s.peakShmemBytes)
	assert.EqualValues(t, 650, s.baselineAnonShmem)
	assert.EqualValues(t, 2900, s.peakAnonShmemBytes)
	assert.EqualValues(t, 2250, s.memoryRiseBytes())
	assert.EqualValues(t, 2, s.maxEventsDelta)
	assert.EqualValues(t, 1, s.oomKillDelta)
}

func TestSamplerAnonAndShmemPeakTogether(t *testing.T) {
	old := cgroupSampleInterval
	cgroupSampleInterval = 2 * time.Millisecond
	t.Cleanup(func() { cgroupSampleInterval = old })

	dir := t.TempDir()
	writeCgroup(t, dir, 1000, 100, 0, 0, 0, 0)
	stop := newCgroupSampler(dir).start()

	// Anonymous memory peaks first, then it is released while tmpfs grows; they are never high together.
	writeCgroup(t, dir, 1000, 1000, 0, 0, 0, 0)
	time.Sleep(50 * time.Millisecond)
	writeCgroup(t, dir, 1000, 100, 800, 0, 0, 0)
	time.Sleep(50 * time.Millisecond)

	s := stop()
	require.NotNil(t, s)
	assert.EqualValues(t, 1000, s.peakAnonBytes)
	assert.EqualValues(t, 800, s.peakShmemBytes)
	assert.EqualValues(t, 1000, s.peakAnonShmemBytes, "the highest single reading, not the sum of the two peaks")
	assert.EqualValues(t, 900, s.memoryRiseBytes())
}

func TestSamplerWithoutSamplesKeepsBaseline(t *testing.T) {
	dir := t.TempDir()
	writeCgroup(t, dir, 1000, 600, 50, 300, 0, 0)
	s := newCgroupSampler(dir).start()()
	require.NotNil(t, s)
	assert.EqualValues(t, 700, s.peakWSBytes)
	assert.EqualValues(t, 600, s.peakAnonBytes)
	assert.EqualValues(t, 0, s.limitBytes)
}

func TestSamplerUnavailable(t *testing.T) {
	assert.Nil(t, newCgroupSampler(""))
	assert.Nil(t, newCgroupSampler(t.TempDir()), "no memory files")
	assert.Nil(t, newCgroupSampler("").start()())
}

func TestPeakMemoryMB(t *testing.T) {
	rusage := &syscall.Rusage{Maxrss: 3 * 1024} // kb

	w := &worker{memSample: &cgroupMemSample{
		baselineAnon:       80 * mb,
		peakAnonBytes:      300 * mb,
		peakWSBytes:        2000 * mb,
		baselineAnonShmem:  100 * mb,
		peakAnonShmemBytes: 612 * mb,
	}}
	assert.EqualValues(t, 512, w.peakMemoryMB(rusage), "rise in anonymous and shared memory over the baseline")

	w.memSample = &cgroupMemSample{baselineAnonShmem: 100 * mb, peakAnonShmemBytes: 50 * mb}
	assert.EqualValues(t, 0, w.peakMemoryMB(rusage), "never negative")

	w.memSample = nil
	assert.EqualValues(t, 3, w.peakMemoryMB(rusage), "falls back to maxrss")
}
