<?php
namespace Awellis13\Resque;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Worker;
use Exception;
/**
 * Extends the normal laravel worker with resque functionality.
 * Sets worker states etc in redis and updates job status.
 *
 * @package Resque
 */
class ResqueWorker extends Worker
{
    /* @var string String identifying this worker. */
    private $id;

    /* @var ResqueQueue */
    private $connection;

    /* @var Exception */
    private $lastException;

    /**
     * Create a new queue worker.
     *
     * @param QueueManager                    $manager
     * @param string                          $connectionName
     * @param string                          $queues
     * @param FailedJobProviderInterface|null $failer
     * @param Dispatcher|null                 $events
     */
    public function __construct(
        QueueManager $manager,
        $connectionName,
        $queues,
        FailedJobProviderInterface $failer = null,
        Dispatcher $events = null
    ) {
        parent::__construct($manager, $failer, $events);
        $this->connection     = $manager->connection($connectionName);
        $this->queues         = $queues;

        $this->init();
    }

    /**
     * Handle some initialization stuff.
     */
    public function init()
    {
        $this->id = $this->getHostname() . ':'.getmypid() . ':' . $this->queues;
        $this->pruneDeadWorkers();
    }

    /**
     * {@inheritDoc}
     */
    public function daemon($connectionName, $queue = null, $delay = 0, $memory = 128, $sleep = 3, $maxTries = 0)
    {
        $this->connection->registerWorker($this);
        parent::daemon($connectionName, $queue, $delay, $memory, $sleep, $maxTries);
    }

    /**
     * {@inheritDoc}
     */
    public function process($connection, Job $job, $maxTries = 0, $delay = 0)
    {
        if ($maxTries > 0 && $job->attempts() > $maxTries) {
            $this->logFailedJob($connection, $job);

            return ['job' => $job, 'failed' => true];
        }

        try {
            $this->connection->workingOn($this, $job);

            // First we will fire off the job. Once it is done we will see if it will
            // be auto-deleted after processing and if so we will go ahead and run
            // the delete method on the job. Otherwise we will just keep moving.
            $job->fire();
            $this->connection->doneWorking($this, $job);

            return ['job' => $job, 'failed' => false];
        } catch (Exception $exc) {
            $this->lastException = $exc;
            // If we catch an exception, we will attempt to release the job back onto
            // the queue so it is not lost. This will let is be retried at a later
            // time by another listener (or the same one). We will do that here.
            if (!$job->isDeleted()) {
                $job->release($delay);
            }
            $this->connection->doneWorking($this, $job);

            throw $exc;
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function logFailedJob($connection, Job $job)
    {
        $job->delete();
        $job->hasFailed($this->lastException, $this->id);
        $this->raiseFailedJobEvent($connection, $job);
    }

    /**
     * {@inheritDoc}
     */
    public function stop()
    {
        $this->connection->unregisterWorker($this);
        parent::stop();
    }

    /**
     * Set the ID of this worker to a given ID string.
     *
     * @param string $workerId ID for the worker.
     */
    public function setId($workerId)
    {
        $this->id = $workerId;
    }

    /**
     * Generate a string representation of this worker.
     *
     * @return string String identifier for this worker instance.
     */
    public function __toString()
    {
        return (string) $this->id;
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the Resque workers did not die gracefully
     * and therefore leave state information in Redis.
     */
    protected function pruneDeadWorkers()
    {
        $workerPids = $this->workerPids();
        $workers = $this->connection->allWorkers();
        foreach ($workers as $workerId) {
            list($hostname, $pid,) = explode(':', $workerId, 3);

            if ($hostname != $this->getHostname() || in_array($pid, $workerPids) || $pid == getmypid()) {
                continue;
            }
            $this->connection->unregisterWorker($workerId);
        }
    }

    /**
     * Return an array of process IDs for all of the Resque workers currently
     * running on this machine.
     *
     * @return array Array of Resque worker process IDs.
     */
    protected function workerPids()
    {
        $pids = array();
        exec('ps -A -o pid,command | grep queue:work', $cmdOutput);
        foreach ($cmdOutput as $line) {
            list($pids[],) = explode(' ', trim($line), 2);
        }

        return $pids;
    }

    /**
     * Returns the hostname where the worker is running on.
     *
     * @return string
     */
    private function getHostname()
    {
        return gethostname();
    }
}
