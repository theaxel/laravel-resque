<?php namespace Awellis13\Resque;

use Exception;
use Resque;
use ResqueScheduler;
use Resque_Event;
use Resque_Job_Status;
use Illuminate\Queue\Queue;

/**
 * Class ResqueQueue
 *
 * @package Resque
 */
class ResqueQueue extends Queue {

	/**
	 * Calls methods on the Resque and ResqueScheduler classes.
	 *
	 * @param string $method
	 * @param array  $parameters
	 * @return mixed
	 */
	public static function __callStatic($method, $parameters)
	{
		if (method_exists('Resque', $method))
		{
			return call_user_func_array(['Resque', $method], $parameters);
		}
		else if (method_exists('ResqueScheduler', $method))
		{
			return call_user_func_array(['RescueScheduler', $method], $parameters);
		}

		return call_user_func_array(['Queue', $method], $parameters);
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param string $job
	 * @param array  $data
	 * @param string $queue
	 * @param bool   $track
	 * @return string
	 */
	public function push($job, $data = [], $queue = NULL, $track = false)
	{
		$queue = (is_null($queue) ? $job : $queue);

		return Resque::enqueue($queue, $job, $data, $track);
	}

	/**
	 * Push the job onto the queue only if the previous one does not exist, is completed, or failed.
	 *
	 * @param string $token
	 * @param string $job
	 * @param array  $data
	 * @param null   $queue
	 * @param bool   $track
	 * @return bool|string
	 */
	public function pushIfNotExists($token, $job, $data = [], $queue = NULL, $track = false)
	{
		if (!$this->jobStatus($token) or $this->isComplete($token) or $this->isFailed($token))
		{
			return $this->push($job, $data, $queue, $track);
		}

		return false;
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param int    $delay
	 * @param string $job
	 * @param mixed  $data
	 * @param string $queue
	 * @return void
	 * @throws Exception
	 */
	public function later($delay, $job, $data = [], $queue = NULL)
	{
		if (!class_exists('ResqueScheduler'))
		{
			throw new Exception("Please add \"chrisboulton/php-resque-scheduler\": \"dev-master\" to your composer.json and run composer update");
		}

		$queue = (is_null($queue) ? $job : $queue);

		if (is_int($delay))
		{
			ResqueScheduler::enqueueIn($delay, $queue, $job, $data);
		}
		else
		{
			ResqueScheduler::enqueueAt($delay, $queue, $job, $data);
		}
	}

	/**
	 * Pop the next job off of the queue.
	 *
	 * @param string $queue
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = NULL)
	{
		return Resque::pop($queue);
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
	 * @param string $token
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
	 * @param string $token
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
	 * @param string $token
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
	 * @param string $token
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
	 * @param string $token
	 * @return bool
	 */
	public function isComplete($token)
	{
		$status = $this->jobStatus($token);

		return $status === Resque_Job_Status::STATUS_COMPLETE;
	}

	/**
	 * Get the queue or return the default.
	 *
	 * @param string|null $queue
	 * @return string
	 */
	protected function getQueue($queue)
	{
		return $queue ? : $this->default;
	}

} // End ResqueQueue
