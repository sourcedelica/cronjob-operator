package controllers

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron"
	kbatchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batchv1 "github.com/sourcedelica/cronjob-operator/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// Used to fake time for testing
type Clock interface {
	Now() time.Time
}

type JobsStatus struct {
	activeJobs     []*kbatchv1.Job
	successfulJobs []*kbatchv1.Job
	failedJobs     []*kbatchv1.Job
	mostRecentTime time.Time
}

var (
	jobOwnerKey             = ".metadata.controller"
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
	apiGVStr                = batchv1.GroupVersion.String()
)

// +kubebuilder:rbac:groups=batch.sourcedelica.com,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.sourcedelica.com,resources=cronjobs/status,verbs=get;update;patch

func (r *CronJobReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("cronjob", req.NamespacedName)

	// Load CronJob by name
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		// We will ignore not-found errors since a delete will trigger a reconcile
		// and can't be fixed by an immediate re-queue
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// List all active jobs and categorize by status
	jobsStatus, err := r.checkJobs(req, ctx, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Update the status of the CronJob
	result, err := r.updateCronJobStatus(&cronJob, jobsStatus, ctx, log)
	if err != nil {
		return result, err
	}

	// Clean up old jobs according to the history limit
	r.cleanupOldJobs(jobsStatus.failedJobs, cronJob.Spec.FailedJobsHistoryLimit, ctx, log, "failed")
	r.cleanupOldJobs(jobsStatus.successfulJobs, cronJob.Spec.SuccessfulJobsHistoryLimit, ctx, log, "successful")

	// Check if we're suspended
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// Get next scheduled run
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		// We don't care about requeueing until we get an update that fixes the schedule
		return ctrl.Result{}, nil
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // save to reuse elsewhere
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	if missedRun.IsZero() {
		log.V(1).Info("no upcoming scheduled times, sleeping until next")
		return scheduledResult, nil
	}

	// Run a new job if it's on schedule, not past the deadline, and not blocked by concurrency policy
	log = log.WithValues("current run", missedRun)

	result, err, done := r.prepareToRunJob(cronJob, jobsStatus.activeJobs, missedRun, scheduledResult, ctx, log)
	if done {
		return result, err
	}

	// Actually make the job
	job, err := r.constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		log.Error(err, "unable to construct job from template")
		// Don't bother requeueing until we get a change to the spec
		return scheduledResult, nil
	}

	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}

	log.V(1).Info("created Job for CronJob run", "job", job)

	// Requeue when the next run would occur
	return scheduledResult, nil
}

func (r *CronJobReconciler) prepareToRunJob(cronJob batchv1.CronJob, activeJobs []*kbatchv1.Job, missedRun time.Time,
	scheduledResult ctrl.Result, ctx context.Context, log logr.Logger) (ctrl.Result, error, bool) {

	if missedRun.IsZero() {
		log.V(1).Info("no upcoming schedule times, sleeping until next")
		return scheduledResult, nil, true
	}

	// Make sure we're not too late to start the run
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	if tooLate {
		log.V(1).Info("missing starting deadline for last run, sleeping until next")
		return scheduledResult, nil, true
	}

	// Figure out how to run this job - concurrent policy might forbid us from running multiple at this time
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy forbids current runs, skipping",
			"num active", len(activeJobs))
		return scheduledResult, nil, true
	}

	// ...or instruct us to replace existing ones
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// We don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err, true
			}
		}
	}

	return ctrl.Result{}, nil, false
}

func isJobFinished(job *kbatchv1.Job) (bool, kbatchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatchv1.JobComplete || c.Type == kbatchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

func getScheduledTimeForJob(job *kbatchv1.Job) (time.Time, error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return time.Time{}, nil
	}
	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return time.Time{}, err
	}
	return timeParsed, nil
}

func (r *CronJobReconciler) checkJobs(req ctrl.Request, ctx context.Context, log logr.Logger) (JobsStatus, error) {
	var childJobs kbatchv1.JobList
	var jobsStatus JobsStatus

	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child Jobs")
		return jobsStatus, err
	}

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // Ongoing
			jobsStatus.activeJobs = append(jobsStatus.activeJobs, &childJobs.Items[i])
		case kbatchv1.JobFailed:
			jobsStatus.failedJobs = append(jobsStatus.failedJobs, &childJobs.Items[i])
		case kbatchv1.JobComplete:
			jobsStatus.successfulJobs = append(jobsStatus.successfulJobs, &childJobs.Items[i])
		}

		// Store launch time in annotation on Job
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if !scheduledTimeForJob.IsZero() {
			if jobsStatus.mostRecentTime.IsZero() || jobsStatus.mostRecentTime.Before(scheduledTimeForJob) {
				jobsStatus.mostRecentTime = scheduledTimeForJob
			}
		}
	}

	mostRecentTimeStr := "N/A"
	if !jobsStatus.mostRecentTime.IsZero() {
		mostRecentTimeStr = fmt.Sprintf("%s", jobsStatus.mostRecentTime)
	}

	log.V(1).Info("job count", "active jobs", len(jobsStatus.activeJobs),
		"successful jobs", len(jobsStatus.successfulJobs), "failed jobs", len(jobsStatus.failedJobs),
		"most recent time", mostRecentTimeStr)

	return jobsStatus, nil
}

func (r *CronJobReconciler) updateCronJobStatus(cronJob *batchv1.CronJob, jobsStatus JobsStatus, ctx context.Context,
	log logr.Logger) (ctrl.Result, error) {

	if !jobsStatus.mostRecentTime.IsZero() {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: jobsStatus.mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	cronJob.Status.Active = nil
	for _, activeJob := range jobsStatus.activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	if err := r.Status().Update(ctx, cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// NB: deleting these is "best effort" - if we fail on a particular one,
// We won't requeue just to finish the deleting
func (r *CronJobReconciler) cleanupOldJobs(jobs []*kbatchv1.Job, limit *int32, ctx context.Context, log logr.Logger,
	jobDescription string) {

	if limit != nil {
		sort.Slice(jobs, func(i, j int) bool {
			if jobs[i].Status.StartTime == nil {
				return jobs[j].Status.StartTime != nil
			}
			return jobs[i].Status.StartTime.Before(jobs[j].Status.StartTime)
		})
		for i, job := range jobs {
			if int32(i) >= int32(len(jobs))-*limit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old "+jobDescription, "job", job)
			} else {
				log.V(0).Info("deleted old "+jobDescription, "job", job)
			}
		}
	}
}

func getNextSchedule(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)

	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule: %q: %v", cronJob.Spec.Schedule, err)
	}

	// For optimization purposes, cheat a bit and start from our last observed run time
	// We could reconstitute this here, but there's not much point, since we've just updated it
	var earliestTime time.Time
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
	}
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		// Controller is not going to schedule anything below this point
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}
	if earliestTime.After(now) {
		return time.Time{}, sched.Next(now), nil
	}

	starts := 0
	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
		lastMissed = t
		// An object might miss several starts. For example, if
		// controller gets wedged on Friday at 5:01pm when everyone has
		// gone home, and someone comes in on Tuesday AM and discovers
		// the problem and restarts the controller, then all the hourly
		// jobs, more than 80 of them for one hourly scheduledJob, should
		// all start running with no further intervention (if the scheduledJob
		// allows concurrency and late starts).
		//
		// However, if there is a bug somewhere, or incorrect clock
		// on controller's server or apiservers (for setting creationTimestamp)
		// then there could be so many missed start times (it could be off
		// by decades or more), that it would eat up all the CPU and memory
		// of this controller. In that case, we want to not try to list
		// all the missed start times.
		starts += 1
		if starts > 100 {
			// We can't get the most recent times so just return an empty slice
			err := "too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew"
			return time.Time{}, time.Time{}, fmt.Errorf(err)
		}
	}
	return lastMissed, sched.Next(now), nil
}

func (r *CronJobReconciler) constructJobForCronJob(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatchv1.Job, error) {
	// We want job names for a given nominal start time to have a deterministic name
	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

	job := &kbatchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
			Name:        name,
			Namespace:   cronJob.Namespace,
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	for k, v := range cronJob.Spec.JobTemplate.Annotations {
		job.Annotations[k] = v
	}
	job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
	for k, v := range cronJob.Spec.JobTemplate.Labels {
		job.Labels[k] = v
	}
	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}

func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Setup a real clock since we're not in a test
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(&kbatchv1.Job{}, jobOwnerKey, func(rawObj runtime.Object) []string {
		// Grab the job object, extract the owner
		job := rawObj.(*kbatchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// Make sure it's a CronJob
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		// If so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatchv1.Job{}).
		Complete(r)
}
