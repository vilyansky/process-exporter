package collector

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	common "github.com/vilyansky/process-exporter"
	"github.com/vilyansky/process-exporter/proc"
)

var (
	numprocsDesc              *prometheus.Desc
	cpuSecsDesc               *prometheus.Desc
	readBytesDesc             *prometheus.Desc
	writeBytesDesc            *prometheus.Desc
	majorPageFaultsDesc       *prometheus.Desc
	minorPageFaultsDesc       *prometheus.Desc
	contextSwitchesDesc       *prometheus.Desc
	membytesDesc              *prometheus.Desc
	openFDsDesc               *prometheus.Desc
	worstFDRatioDesc          *prometheus.Desc
	startTimeDesc             *prometheus.Desc
	numThreadsDesc            *prometheus.Desc
	statesDesc                *prometheus.Desc
	scrapeErrorsDesc          *prometheus.Desc
	scrapeProcReadErrorsDesc  *prometheus.Desc
	scrapePartialErrorsDesc   *prometheus.Desc
	threadWchanDesc           *prometheus.Desc
	threadCountDesc           *prometheus.Desc
	threadCpuSecsDesc         *prometheus.Desc
	threadIoBytesDesc         *prometheus.Desc
	threadMajorPageFaultsDesc *prometheus.Desc
	threadMinorPageFaultsDesc *prometheus.Desc
	threadContextSwitchesDesc *prometheus.Desc
)

type (
	scrapeRequest struct {
		results chan<- prometheus.Metric
		done    chan struct{}
	}

	ProcessCollectorOption struct {
		ProcFSPath       string
		Children         bool
		Threads          bool
		GatherSMaps      bool
		Namer            common.MatchNamer
		Recheck          bool
		RecheckTimeLimit time.Duration
		Debug            bool
		ExtraLabels      map[string]string
		//Config           config.Config
	}

	NamedProcessCollector struct {
		scrapeChan chan scrapeRequest
		*proc.Grouper
		threads              bool
		smaps                bool
		source               proc.Source
		scrapeErrors         int
		scrapeProcReadErrors int
		scrapePartialErrors  int
		debug                bool
		extralabels          map[string]string
	}
)

func init() {
	//	extralabels_keys =
}

func getLabelKeys(extraLabels map[string]string) []string {
	keys := make([]string, 0, len(extraLabels))
	for key := range extraLabels {
		keys = append(keys, key)
	}
	return keys
}

func labelValues(labels prometheus.Labels) []string {
	keys := make([]string, 0, len(labels))
	values := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values = append(values, labels[key])
	}
	return values
}

func NewProcessCollector(options ProcessCollectorOption) (*NamedProcessCollector, error) {
	fs, err := proc.NewFS(options.ProcFSPath, options.Debug)
	if err != nil {
		return nil, err
	}

	fs.GatherSMaps = options.GatherSMaps
	p := &NamedProcessCollector{
		scrapeChan:  make(chan scrapeRequest),
		Grouper:     proc.NewGrouper(options.Namer, options.Children, options.Threads, options.Recheck, options.RecheckTimeLimit, options.Debug),
		source:      fs,
		threads:     options.Threads,
		smaps:       options.GatherSMaps,
		debug:       options.Debug,
		extralabels: options.ExtraLabels,
	}

	colErrs, _, err := p.Update(p.source.AllProcs())
	if err != nil {
		if options.Debug {
			log.Print(err)
		}
		return nil, err
	}
	p.scrapePartialErrors += colErrs.Partial
	p.scrapeProcReadErrors += colErrs.Read

	go p.start()

	return p, nil
}

// Describe implements prometheus.Collector.
func (p *NamedProcessCollector) Describe(ch chan<- *prometheus.Desc) {
	var extralabels_keys = getLabelKeys(p.extralabels)

	log.Print("extralabels_keys Describe: ", extralabels_keys)

	numprocsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_num_procs",
		"number of processes in this group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	cpuSecsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_cpu_seconds_total",
		"Cpu user usage in seconds",
		append([]string{"groupname", "mode"}, extralabels_keys...),
		nil)

	readBytesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_read_bytes_total",
		"number of bytes read by this group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	writeBytesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_write_bytes_total",
		"number of bytes written by this group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	majorPageFaultsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_major_page_faults_total",
		"Major page faults",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	minorPageFaultsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_minor_page_faults_total",
		"Minor page faults",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	contextSwitchesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_context_switches_total",
		"Context switches",
		append([]string{"groupname", "ctxswitchtype"}, extralabels_keys...),
		nil)

	membytesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_memory_bytes",
		"number of bytes of memory in use",
		append([]string{"groupname", "memtype"}, extralabels_keys...),
		nil)

	openFDsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_open_filedesc",
		"number of open file descriptors for this group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	worstFDRatioDesc = prometheus.NewDesc(
		"namedprocess_namegroup_worst_fd_ratio",
		"the worst (closest to 1) ratio between open fds and max fds among all procs in this group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	startTimeDesc = prometheus.NewDesc(
		"namedprocess_namegroup_oldest_start_time_seconds",
		"start time in seconds since 1970/01/01 of oldest process in group",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	numThreadsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_num_threads",
		"Number of threads",
		append([]string{"groupname"}, extralabels_keys...),
		nil)

	statesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_states",
		"Number of processes in states Running, Sleeping, Waiting, Zombie, or Other",
		append([]string{"groupname", "state"}, extralabels_keys...),
		nil)

	scrapeErrorsDesc = prometheus.NewDesc(
		"namedprocess_scrape_errors",
		"general scrape errors: no proc metrics collected during a cycle",
		extralabels_keys,
		nil)

	scrapeProcReadErrorsDesc = prometheus.NewDesc(
		"namedprocess_scrape_procread_errors",
		"incremented each time a proc's metrics collection fails",
		extralabels_keys,
		nil)

	scrapePartialErrorsDesc = prometheus.NewDesc(
		"namedprocess_scrape_partial_errors",
		"incremented each time a tracked proc's metrics collection fails partially, e.g. unreadable I/O stats",
		extralabels_keys,
		nil)

	threadWchanDesc = prometheus.NewDesc(
		"namedprocess_namegroup_threads_wchan",
		"Number of threads in this group waiting on each wchan",
		append([]string{"groupname", "wchan"}, extralabels_keys...),
		nil)

	threadCountDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_count",
		"Number of threads in this group with same threadname",
		append([]string{"groupname", "threadname"}, extralabels_keys...),
		nil)

	threadCpuSecsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_cpu_seconds_total",
		"Cpu user/system usage in seconds",
		append([]string{"groupname", "threadname", "mode"}, extralabels_keys...),
		nil)

	threadIoBytesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_io_bytes_total",
		"number of bytes read/written by these threads",
		append([]string{"groupname", "threadname", "iomode"}, extralabels_keys...),
		nil)

	threadMajorPageFaultsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_major_page_faults_total",
		"Major page faults for these threads",
		append([]string{"groupname", "threadname"}, extralabels_keys...),
		nil)

	threadMinorPageFaultsDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_minor_page_faults_total",
		"Minor page faults for these threads",
		append([]string{"groupname", "threadname"}, extralabels_keys...),
		nil)

	threadContextSwitchesDesc = prometheus.NewDesc(
		"namedprocess_namegroup_thread_context_switches_total",
		"Context switches for these threads",
		append([]string{"groupname", "threadname", "ctxswitchtype"}, extralabels_keys...),
		nil)

	ch <- cpuSecsDesc
	ch <- numprocsDesc
	ch <- readBytesDesc
	ch <- writeBytesDesc
	ch <- membytesDesc
	ch <- openFDsDesc
	ch <- worstFDRatioDesc
	ch <- startTimeDesc
	ch <- majorPageFaultsDesc
	ch <- minorPageFaultsDesc
	ch <- contextSwitchesDesc
	ch <- numThreadsDesc
	ch <- statesDesc
	ch <- scrapeErrorsDesc
	ch <- scrapeProcReadErrorsDesc
	ch <- scrapePartialErrorsDesc
	ch <- threadWchanDesc
	ch <- threadCountDesc
	ch <- threadCpuSecsDesc
	ch <- threadIoBytesDesc
	ch <- threadMajorPageFaultsDesc
	ch <- threadMinorPageFaultsDesc
	ch <- threadContextSwitchesDesc
}

// Collect implements prometheus.Collector.
func (p *NamedProcessCollector) Collect(ch chan<- prometheus.Metric) {
	req := scrapeRequest{results: ch, done: make(chan struct{})}
	p.scrapeChan <- req
	<-req.done
}

func (p *NamedProcessCollector) start() {
	for req := range p.scrapeChan {
		ch := req.results
		p.scrape(ch)
		req.done <- struct{}{}
	}
}

func (p *NamedProcessCollector) scrape(ch chan<- prometheus.Metric) {
	fmt.Printf("%v", getLabelKeys(p.extralabels))
	permErrs, groups, err := p.Update(p.source.AllProcs())
	p.scrapePartialErrors += permErrs.Partial

	extralabels_values := labelValues(p.extralabels)
	//log.Printf("extralabels_values in scrape: %v", extralabels_values)

	if err != nil {
		p.scrapeErrors++
		log.Printf("error reading procs: %v", err)
	} else {
		for gname, gcounts := range groups {
			gname_with_extra_labels_values := append([]string{gname}, extralabels_values...)
			ch <- prometheus.MustNewConstMetric(numprocsDesc,
				prometheus.GaugeValue, float64(gcounts.Procs), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(membytesDesc,
				prometheus.GaugeValue, float64(gcounts.Memory.ResidentBytes), append(gname_with_extra_labels_values, "resident")...)
			ch <- prometheus.MustNewConstMetric(membytesDesc,
				prometheus.GaugeValue, float64(gcounts.Memory.VirtualBytes), append(gname_with_extra_labels_values, "virtual")...)
			ch <- prometheus.MustNewConstMetric(membytesDesc,
				prometheus.GaugeValue, float64(gcounts.Memory.VmSwapBytes), append(gname_with_extra_labels_values, "swapped")...)
			ch <- prometheus.MustNewConstMetric(startTimeDesc,
				prometheus.GaugeValue, float64(gcounts.OldestStartTime.Unix()), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(openFDsDesc,
				prometheus.GaugeValue, float64(gcounts.OpenFDs), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(worstFDRatioDesc,
				prometheus.GaugeValue, float64(gcounts.WorstFDratio), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(cpuSecsDesc,
				prometheus.CounterValue, gcounts.CPUUserTime, append(gname_with_extra_labels_values, "user")...)
			ch <- prometheus.MustNewConstMetric(cpuSecsDesc,
				prometheus.CounterValue, gcounts.CPUSystemTime, append(gname_with_extra_labels_values, "system")...)
			ch <- prometheus.MustNewConstMetric(readBytesDesc,
				prometheus.CounterValue, float64(gcounts.ReadBytes), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(writeBytesDesc,
				prometheus.CounterValue, float64(gcounts.WriteBytes), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(majorPageFaultsDesc,
				prometheus.CounterValue, float64(gcounts.MajorPageFaults), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(minorPageFaultsDesc,
				prometheus.CounterValue, float64(gcounts.MinorPageFaults), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(contextSwitchesDesc,
				prometheus.CounterValue, float64(gcounts.CtxSwitchVoluntary), append(gname_with_extra_labels_values, "voluntary")...)
			ch <- prometheus.MustNewConstMetric(contextSwitchesDesc,
				prometheus.CounterValue, float64(gcounts.CtxSwitchNonvoluntary), append(gname_with_extra_labels_values, "nonvoluntary")...)
			ch <- prometheus.MustNewConstMetric(numThreadsDesc,
				prometheus.GaugeValue, float64(gcounts.NumThreads), gname_with_extra_labels_values...)
			ch <- prometheus.MustNewConstMetric(statesDesc,
				prometheus.GaugeValue, float64(gcounts.States.Running), append(gname_with_extra_labels_values, "Running")...)
			ch <- prometheus.MustNewConstMetric(statesDesc,
				prometheus.GaugeValue, float64(gcounts.States.Sleeping), append(gname_with_extra_labels_values, "Sleeping")...)
			ch <- prometheus.MustNewConstMetric(statesDesc,
				prometheus.GaugeValue, float64(gcounts.States.Waiting), append(gname_with_extra_labels_values, "Waiting")...)
			ch <- prometheus.MustNewConstMetric(statesDesc,
				prometheus.GaugeValue, float64(gcounts.States.Zombie), append(gname_with_extra_labels_values, "Zombie")...)
			ch <- prometheus.MustNewConstMetric(statesDesc,
				prometheus.GaugeValue, float64(gcounts.States.Other), append(gname_with_extra_labels_values, "Other")...)

			for wchan, count := range gcounts.Wchans {
				ch <- prometheus.MustNewConstMetric(threadWchanDesc,
					prometheus.GaugeValue, float64(count), strings.Join(gname_with_extra_labels_values, ",")+wchan)
			}

			if p.smaps {
				ch <- prometheus.MustNewConstMetric(membytesDesc,
					prometheus.GaugeValue, float64(gcounts.Memory.ProportionalBytes), append(gname_with_extra_labels_values, "proportionalResident")...)
				ch <- prometheus.MustNewConstMetric(membytesDesc,
					prometheus.GaugeValue, float64(gcounts.Memory.ProportionalSwapBytes), append(gname_with_extra_labels_values, "proportionalSwapped")...)
			}

			if p.threads {
				for _, thr := range gcounts.Threads {
					ch <- prometheus.MustNewConstMetric(threadCountDesc,
						prometheus.GaugeValue, float64(thr.NumThreads),
						append(gname_with_extra_labels_values, thr.Name)...)
					ch <- prometheus.MustNewConstMetric(threadCpuSecsDesc,
						prometheus.CounterValue, float64(thr.CPUUserTime),
						append(append(gname_with_extra_labels_values, thr.Name), "user")...)
					ch <- prometheus.MustNewConstMetric(threadCpuSecsDesc,
						prometheus.CounterValue, float64(thr.CPUSystemTime),
						append(append(gname_with_extra_labels_values, thr.Name), "system")...)
					ch <- prometheus.MustNewConstMetric(threadIoBytesDesc,
						prometheus.CounterValue, float64(thr.ReadBytes),
						append(append(gname_with_extra_labels_values, thr.Name), "read")...)
					ch <- prometheus.MustNewConstMetric(threadIoBytesDesc,
						prometheus.CounterValue, float64(thr.WriteBytes),
						append(append(gname_with_extra_labels_values, thr.Name), "write")...)
					ch <- prometheus.MustNewConstMetric(threadMajorPageFaultsDesc,
						prometheus.CounterValue, float64(thr.MajorPageFaults),
						append(gname_with_extra_labels_values, thr.Name)...)
					ch <- prometheus.MustNewConstMetric(threadMinorPageFaultsDesc,
						prometheus.CounterValue, float64(thr.MinorPageFaults),
						append(gname_with_extra_labels_values, thr.Name)...)
					ch <- prometheus.MustNewConstMetric(threadContextSwitchesDesc,
						prometheus.CounterValue, float64(thr.CtxSwitchVoluntary),
						append(append(gname_with_extra_labels_values, thr.Name), "voluntary")...)
					ch <- prometheus.MustNewConstMetric(threadContextSwitchesDesc,
						prometheus.CounterValue, float64(thr.CtxSwitchNonvoluntary),
						append(append(gname_with_extra_labels_values, thr.Name), "nonvoluntary")...)
				}
			}
		}
	}
	//log.Printf("p.extralabels=%s\n", p.extralabels)
	ch <- prometheus.MustNewConstMetric(scrapeErrorsDesc,
		prometheus.CounterValue, float64(p.scrapeErrors), extralabels_values...)
	ch <- prometheus.MustNewConstMetric(scrapeProcReadErrorsDesc,
		prometheus.CounterValue, float64(p.scrapeProcReadErrors), extralabels_values...)
	ch <- prometheus.MustNewConstMetric(scrapePartialErrorsDesc,
		prometheus.CounterValue, float64(p.scrapePartialErrors), extralabels_values...)
}
