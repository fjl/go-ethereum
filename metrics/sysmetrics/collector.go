// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package sysmetrics

import (
	"runtime/metrics"
	"runtime/pprof"
	"time"

	gethmetrics "github.com/ethereum/go-ethereum/metrics"
)

var threadCreateProfile = pprof.Lookup("threadcreate")

// RuntimeStats contains statistics from the Go runtime.
type RuntimeStats struct {
	GCPauses     *metrics.Float64Histogram
	GCAllocBytes uint64
	GCFreedBytes uint64

	MemTotal     uint64
	HeapObjects  uint64
	HeapFree     uint64
	HeapReleased uint64
	HeapUnused   uint64

	Goroutines   uint64
	SchedLatency *metrics.Float64Histogram
}

var runtimeSamples = []metrics.Sample{
	{Name: "/gc/pauses:seconds"}, // histogram
	{Name: "/gc/heap/allocs:bytes"},
	{Name: "/gc/heap/frees:bytes"},
	{Name: "/memory/classes/total:bytes"},
	{Name: "/memory/classes/heap/objects:bytes"},
	{Name: "/memory/classes/heap/free:bytes"},
	{Name: "/memory/classes/heap/released:bytes"},
	{Name: "/memory/classes/heap/unused:bytes"},
	{Name: "/sched/goroutines:goroutines"},
	{Name: "/sched/latencies:seconds"}, // histogram
}

// ReadRuntimeStats reads runtime statistics into the given struct.
func ReadRuntimeStats() *RuntimeStats {
	r := new(RuntimeStats)
	readRuntimeStats(r)
	return r
}

func readRuntimeStats(v *RuntimeStats) {
	metrics.Read(runtimeSamples)
	for _, s := range runtimeSamples {
		// Skip invalid/unknown metrics. This is needed because some metrics
		// are unavailable in older Go versions, and attempting to read a 'bad'
		// metric panics.
		if s.Value.Kind() == metrics.KindBad {
			continue
		}

		switch s.Name {
		case "/gc/pauses:seconds":
			v.GCPauses = s.Value.Float64Histogram()
		case "/gc/heap/allocs:bytes":
			v.GCAllocBytes = s.Value.Uint64()
		case "/gc/heap/frees:bytes":
			v.GCFreedBytes = s.Value.Uint64()
		case "/memory/classes/total:bytes":
			v.MemTotal = s.Value.Uint64()
		case "/memory/classes/heap/objects:bytes":
			v.HeapObjects = s.Value.Uint64()
		case "/memory/classes/heap/free:bytes":
			v.HeapFree = s.Value.Uint64()
		case "/memory/classes/heap/released:bytes":
			v.HeapReleased = s.Value.Uint64()
		case "/memory/classes/heap/unused:bytes":
			v.HeapUnused = s.Value.Uint64()
		case "/sched/goroutines:goroutines":
			v.Goroutines = s.Value.Uint64()
		case "/sched/latencies:seconds":
			v.SchedLatency = s.Value.Float64Histogram()
		}
	}
}

// CollectProcessMetrics periodically collects various metrics about the running process.
func CollectProcessMetrics(refresh time.Duration) {
	// Short circuit if the metrics system is disabled
	if !gethmetrics.Enabled() {
		return
	}

	// Create the various data collectors
	var (
		cpustats  = make([]CPUStats, 2)
		diskstats = make([]DiskStats, 2)
		rstats    = make([]RuntimeStats, 2)
	)

	// This scale factor is used for the runtime's time metrics. It's useful to convert to
	// ns here because the runtime gives times in float seconds, but RuntimeHistogram can
	// only provide integers for the minimum and maximum values.
	const secondsToNs = float64(time.Second)

	// Define the various metrics to collect
	var (
		cpuSysLoad            = gethmetrics.GetOrRegisterGauge("system/cpu/sysload", gethmetrics.DefaultRegistry)
		cpuSysWait            = gethmetrics.GetOrRegisterGauge("system/cpu/syswait", gethmetrics.DefaultRegistry)
		cpuProcLoad           = gethmetrics.GetOrRegisterGauge("system/cpu/procload", gethmetrics.DefaultRegistry)
		cpuSysLoadTotal       = gethmetrics.GetOrRegisterCounterFloat64("system/cpu/sysload/total", gethmetrics.DefaultRegistry)
		cpuSysWaitTotal       = gethmetrics.GetOrRegisterCounterFloat64("system/cpu/syswait/total", gethmetrics.DefaultRegistry)
		cpuProcLoadTotal      = gethmetrics.GetOrRegisterCounterFloat64("system/cpu/procload/total", gethmetrics.DefaultRegistry)
		cpuThreads            = gethmetrics.GetOrRegisterGauge("system/cpu/threads", gethmetrics.DefaultRegistry)
		cpuGoroutines         = gethmetrics.GetOrRegisterGauge("system/cpu/goroutines", gethmetrics.DefaultRegistry)
		cpuSchedLatency       = gethmetrics.GetOrRegisterRuntimeHistogram("system/cpu/schedlatency", secondsToNs, nil)
		memPauses             = gethmetrics.GetOrRegisterRuntimeHistogram("system/memory/pauses", secondsToNs, nil)
		memAllocs             = gethmetrics.GetOrRegisterMeter("system/memory/allocs", gethmetrics.DefaultRegistry)
		memFrees              = gethmetrics.GetOrRegisterMeter("system/memory/frees", gethmetrics.DefaultRegistry)
		memTotal              = gethmetrics.GetOrRegisterGauge("system/memory/held", gethmetrics.DefaultRegistry)
		heapUsed              = gethmetrics.GetOrRegisterGauge("system/memory/used", gethmetrics.DefaultRegistry)
		heapObjects           = gethmetrics.GetOrRegisterGauge("system/memory/objects", gethmetrics.DefaultRegistry)
		diskReads             = gethmetrics.GetOrRegisterMeter("system/disk/readcount", gethmetrics.DefaultRegistry)
		diskReadBytes         = gethmetrics.GetOrRegisterMeter("system/disk/readdata", gethmetrics.DefaultRegistry)
		diskReadBytesCounter  = gethmetrics.GetOrRegisterCounter("system/disk/readbytes", gethmetrics.DefaultRegistry)
		diskWrites            = gethmetrics.GetOrRegisterMeter("system/disk/writecount", gethmetrics.DefaultRegistry)
		diskWriteBytes        = gethmetrics.GetOrRegisterMeter("system/disk/writedata", gethmetrics.DefaultRegistry)
		diskWriteBytesCounter = gethmetrics.GetOrRegisterCounter("system/disk/writebytes", gethmetrics.DefaultRegistry)
	)

	var lastCollectTime time.Time

	// Iterate loading the different stats and updating the meters.
	now, prev := 0, 1
	for ; ; now, prev = prev, now {
		// Gather CPU times.
		ReadCPUStats(&cpustats[now])
		collectTime := time.Now()
		secondsSinceLastCollect := collectTime.Sub(lastCollectTime).Seconds()
		lastCollectTime = collectTime
		if secondsSinceLastCollect > 0 {
			sysLoad := cpustats[now].GlobalTime - cpustats[prev].GlobalTime
			sysWait := cpustats[now].GlobalWait - cpustats[prev].GlobalWait
			procLoad := cpustats[now].LocalTime - cpustats[prev].LocalTime
			// Convert to integer percentage.
			cpuSysLoad.Update(int64(sysLoad / secondsSinceLastCollect * 100))
			cpuSysWait.Update(int64(sysWait / secondsSinceLastCollect * 100))
			cpuProcLoad.Update(int64(procLoad / secondsSinceLastCollect * 100))
			// increment counters (ms)
			cpuSysLoadTotal.Inc(sysLoad)
			cpuSysWaitTotal.Inc(sysWait)
			cpuProcLoadTotal.Inc(procLoad)
		}

		// Threads
		cpuThreads.Update(int64(threadCreateProfile.Count()))

		// Go runtime metrics
		readRuntimeStats(&rstats[now])

		cpuGoroutines.Update(int64(rstats[now].Goroutines))
		cpuSchedLatency.UpdateFrom(rstats[now].SchedLatency)
		memPauses.UpdateFrom(rstats[now].GCPauses)

		memAllocs.Mark(int64(rstats[now].GCAllocBytes - rstats[prev].GCAllocBytes))
		memFrees.Mark(int64(rstats[now].GCFreedBytes - rstats[prev].GCFreedBytes))

		memTotal.Update(int64(rstats[now].MemTotal))
		heapUsed.Update(int64(rstats[now].MemTotal - rstats[now].HeapUnused - rstats[now].HeapFree - rstats[now].HeapReleased))
		heapObjects.Update(int64(rstats[now].HeapObjects))

		// Disk
		if ReadDiskStats(&diskstats[now]) == nil {
			diskReads.Mark(diskstats[now].ReadCount - diskstats[prev].ReadCount)
			diskReadBytes.Mark(diskstats[now].ReadBytes - diskstats[prev].ReadBytes)
			diskWrites.Mark(diskstats[now].WriteCount - diskstats[prev].WriteCount)
			diskWriteBytes.Mark(diskstats[now].WriteBytes - diskstats[prev].WriteBytes)
			diskReadBytesCounter.Inc(diskstats[now].ReadBytes - diskstats[prev].ReadBytes)
			diskWriteBytesCounter.Inc(diskstats[now].WriteBytes - diskstats[prev].WriteBytes)
		}

		time.Sleep(refresh)
	}
}
