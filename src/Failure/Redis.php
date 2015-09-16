<?php
namespace Awellis13\Resque\Failure;

use Resque;
use Resque_Failure_Interface;

/**
 * Redis backend for storing failed Resque jobs.
 *
 * @package     Resque/Failure
 * @author      Axel Kelting <axel@kelting.cc>
 */
class Redis implements Resque_Failure_Interface
{
    /**
     * Initialize a failed job class and save it (where appropriate).
     *
     * @param array  $payload   Object containing details of the failed job.
     * @param object $exception Instance of the exception that was thrown by the failed job.
     * @param object $worker    Instance of Resque_Worker that received the job.
     * @param string $queue     The name of the queue the job was fetched from.
     */
    public function __construct($payload, $exception, $worker, $queue)
    {
        $this->resetAttempts($payload);

        Resque::redis()->rpush('failed', json_encode($this->createPayload($payload, $exception, $worker, $queue)));
    }

    /**
     * Reset attempts so job doesn't get rejected by resque worker instantly again.
     *
     * @param array $payload
     */
    private function resetAttempts(array &$payload)
    {
        if (isset($payload['args'])) {
            $payload['args'][0]['failed_attempts'] = $payload['args'][0]['attempts'];
            $payload['args'][0]['attempts'] = 1;
        }
    }

    /**
     * @param array  $payload
     * @param object $exception
     * @param object $worker
     * @param string $queue
     *
     * @return stdClass
     */
    private function createPayload($payload, $exception, $worker, $queue)
    {
        $data = new \stdClass();
        $data->failed_at = strftime('%a %b %d %H:%M:%S %Z %Y');
        $data->payload = $payload;
        $data->exception = get_class($exception);
        $data->error = $exception->getMessage();
        $data->backtrace = explode("\n", $exception->getTraceAsString());
        $data->worker = (string)$worker;
        $data->queue = $queue;

        return $data;
    }
}
