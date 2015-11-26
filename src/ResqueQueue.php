<?php
namespace Awellis13\Resque;

use Exception;
use Resque;
use ResqueScheduler;
use Resque_Event;
use Resque_Job;
use Resque_Job_Status;
use Resque_Stat;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

/**
 * Class ResqueQueue
 *
 * @package Resque
 */
class ResqueQueue extends Queue implements QueueContract
{
    /**
     * Calls methods on the Resque and ResqueScheduler classes.
     *
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public static function __callStatic($method, $parameters)
    {
        if (method_exists('Resque', $method)) {
            return call_user_func_array(['Resque', $method], $parameters);
        } elseif (method_exists('ResqueScheduler', $method)) {
            return call_user_func_array(['RescueScheduler', $method], $parameters);
        }

        return call_user_func_array(['Queue', $method], $parameters);
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string $job
     * @param  array  $data
     * @param  string $queue
     * @param  bool   $track
     * @return string
     */
    public function push($job, $data = [], $queue = null, $track = false)
    {
        $queue = (is_null($queue) ? $job : $queue);
        $data = $data == '' ? [] : $data;

        return Resque::enqueue($queue, $this->getJobClass($job), $this->createPayload($job, $data), $track);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string  $queue
     * @param  array   $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = array())
    {
        throw new Exception("Method not implemented for resque!");

    }

    /**
     * Push the job onto the queue only if the previous one does not exist, is completed, or failed.
     *
     * @param  string      $token
     * @param  string      $job
     * @param  array       $data
     * @param  null        $queue
     * @param  bool        $track
     * @return bool|string
     */
    public function pushIfNotExists($token, $job, $data = [], $queue = null, $track = false)
    {
        if (!$this->jobStatus($token) or $this->isComplete($token) or $this->isFailed($token)) {
            return $this->push($job, $data, $queue, $track);
        }

        return false;
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  int       $delay
     * @param  string    $job
     * @param  mixed     $data
     * @param  string    $queue
     * @return void
     * @throws Exception
     */
    public function later($delay, $job, $data = [], $queue = null)
    {
        if (!class_exists('ResqueScheduler')) {
            throw new Exception("Please add \"chrisboulton/php-resque-scheduler\": \"dev-master\" to your composer.json and run composer update");
        }

        $queue = (is_null($queue) ? $job : $queue);

        if (is_int($delay)) {
            ResqueScheduler::enqueueIn($delay, $queue, $this->getJobClass($job), $this->createPayload($job, $data));
        } else {
            ResqueScheduler::enqueueAt($delay, $queue, $this->getJobClass($job), $this->createPayload($job, $data));
        }
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     *
     * @return \Illuminate\Queue\Jobs\Job|null
     */
    public function pop($queue = null)
    {
        $queue = $queue ?: $this->default;
        $job = Resque_Job::reserve($queue);
        if ($job) {
            return new ResqueJob($this->container, $this, $job, $queue);
        }
    }

    /**
     * Release a reserved job back onto the queue.
     *
     * @param ResqueJob $job
     * @param int       $delay
     *
     * @return void
     */
    public function release(ResqueJob $job, $delay)
    {
        $status = new Resque_Job_Status($job->getJobId());
        $track = false;
        if ($status->isTracking()) {
            $track = true;
        }

        if ($delay > 0 && class_exists('ResqueScheduler')) {
            ResqueScheduler::enqueueIn($delay, $job->getQueue(), $job->getJobClass(), $job->getArguments());
        } else {
            Resque_Job::create($job->getQueue(), $job->getJobClass(), $job->getArguments(), $track, $job->getJobId());
        }
    }

    /**
     * Register a callback for an event.
     *
     * @param string $event
     * @param object $function
     */
    public function listen($event, $function)
    {
        Resque_Event::listen($event, $function);
    }

    /**
     * Returns the job's status.
     *
     * @param  string $token
     * @return int
     */
    public function jobStatus($token)
    {
        $status = new Resque_Job_Status($token);

        return $status->get();
    }

    /**
     * Returns true if the job is in waiting state.
     *
     * @param  string $token
     * @return bool
     */
    public function isWaiting($token)
    {
        $status = $this->jobStatus($token);

        return $status === Resque_Job_Status::STATUS_WAITING;
    }

    /**
     * Returns true if the job is in running state.
     *
     * @param  string $token
     * @return bool
     */
    public function isRunning($token)
    {
        $status = $this->jobStatus($token);

        return $status === Resque_Job_Status::STATUS_RUNNING;
    }

    /**
     * Returns true if the job is in failed state.
     *
     * @param  string $token
     * @return bool
     */
    public function isFailed($token)
    {
        $status = $this->jobStatus($token);

        return $status === Resque_Job_Status::STATUS_FAILED;
    }

    /**
     * Returns true if the job is in complete state.
     *
     * @param  string $token
     * @return bool
     */
    public function isComplete($token)
    {
        $status = $this->jobStatus($token);

        return $status === Resque_Job_Status::STATUS_COMPLETE;
    }

    /**
     * Returns all ids from workers registrated in resque.
     *
     * @return array
     */
    public function allWorkers()
    {
        $workers = Resque::redis()->smembers('workers');
        if (!is_array($workers)) {
            $workers = [];
        }

        return $workers;
    }

    /**
     * Register this worker in Redis.
     *
     * @param ResqueWorker $worker
     */
    public function registerWorker(ResqueWorker $worker)
    {
        Resque::redis()->sadd('workers', (string) $worker);
        Resque::redis()->set('worker:' . $worker . ':started', strftime('%a %b %d %H:%M:%S %Z %Y'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     *
     * @param ResqueWorker|string $worker
     */
    public function unregisterWorker($worker)
    {
        $workerId = is_object($worker) ? (string) $worker : $worker;

        Resque::redis()->srem('workers', $workerId);
        Resque::redis()->del('worker:' . $workerId);
        Resque::redis()->del('worker:' . $workerId . ':started');
        Resque_Stat::clear('processed:' . $workerId);
        Resque_Stat::clear('failed:' . $workerId);
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param ResqueWorker $worker
     * @param object $job Resque_Job instance containing the job we're working on.
     */
    public function workingOn(ResqueWorker $worker, ResqueJob $job)
    {
        $data = json_encode([
            'queue' => $job->getQueue(),
            'run_at' => strftime('%a %b %d %H:%M:%S %Z %Y'),
            'payload' => $job->getResqueJob()->payload,
        ]);
        Resque::redis()->set('worker:' . $worker, $data);
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     * @param ResqueWorker $worker
     */
    public function doneWorking(ResqueWorker $worker)
    {
        Resque_Stat::incr('processed');
        Resque_Stat::incr('processed:' . $worker);
        Resque::redis()->del('worker:' . $worker);
    }

    /**
     * Create a payload string from the given job and data.
     *
     * @param  string  $job
     * @param  mixed   $data
     * @param  string  $queue Obsolete parameter but required because of inheritance.
     *
     * @return string JSON encoded
     */
    protected function createPayload($job, $data = '', $queue = null)
    {
        $payload = parent::createPayload($job);
        $payload = json_decode($payload, true);

        // Extract serialized command from data payload.
        if (isset($payload['data']['command'])) {
            $payload['command'] = $payload['data']['command'];
            unset($payload['data']['command']);
        }

        if (!isset($payload['attempts'])) {
            $payload['attempts'] = 1;
        }

        if (!$payload['data']) {
            unset($payload['data']);
        }

        return array_merge($this->prepareQueueableEntities($data), $payload);
    }

    /**
     * Get the class of the provided job which can either be a class or plain string.
     * @param mixed $job
     *
     * @return string
     */
    protected function getJobClass($job)
    {
        if (is_object($job)) {
            return get_class($job);
        } else {
            return $job;
        }
    }
}
