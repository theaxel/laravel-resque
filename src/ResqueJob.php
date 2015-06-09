<?php
namespace Awellis13\Resque;

use Illuminate\Queue\Jobs\Job;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Resque_Event;
use Resque_Job;
use Resque_Job_Status;
use Resque_Worker;

/**
 * Class ResqueJob
 *
 * @package Resque
 */
class ResqueJob extends Job implements JobContract
{
    /**
     * The Resque queue instance.
     *
     * @var ResqueQueue
     */
    protected $resque;

    /**
     * The resque job instance.
     *
     * @var Resque_Job
     */
    protected $job;
    /**
     * The payload from the resque job.
     *
     * @var array
     */
    protected $args;

    /**
     * The id assigned by resque to the job.
     * @var
     */
    protected $resqueId;

    /**
     * The class name of the job that has to be fired.
     * @var array
     */
    protected $class;

    /**
     * Create a new job instance.
     *
     * @param  Container   $container
     * @param  ResqueQueue $resque
     * @param  Resque_Job  $job
     * @param  string      $queue
     *
     * @return void
     */
    public function __construct(Container $container, ResqueQueue $resque, Resque_Job $job, $queue)
    {
        #var_dump($job);
        $this->container = $container;
        $this->resque    = $resque;
        $this->job       = $job;
        $this->queue     = $queue;
        $this->class     = $job->payload['class'];
        $this->args      = $job->getArguments();
        $this->resqueId  = array_get($job->payload, 'id');
    }

    /**
     * {@inheritDoc}
     */
    public function fire()
    {
        $this->job->updateStatus(Resque_Job_Status::STATUS_RUNNING);
        Resque_Event::trigger('beforePerform', $this);
        $this->resolveAndFire($this->convertArgsToQueueBody());
        Resque_Event::trigger('afterPerform', $this);
        $this->job->updateStatus(Resque_Job_Status::STATUS_COMPLETE);
    }

    /**
     * {@inheritDoc}
     */
    public function getRawBody()
    {
        return json_encode($this->convertArgsToQueueBody());
    }

    /**
     * {@inheritDoc}
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->args['attempts'] += 1;
        $this->resque->release($this, $delay);
    }

    /**
     * Add exception as parameter to log message to resque.
     *
     * @param Exception $exception
     * @param string    $workerId
     */
    public function hasFailed(\Exception $exception, $workerId)
    {
        $worker = new Resque_Worker([]);
        $worker->setId($workerId);
        $this->job->worker = $worker;
        $this->job->fail($exception);
        $this->failed();
    }

    /**
     * {@inheritDoc}
     */
    public function attempts()
    {
        return array_get($this->args, 'attempts', 1);
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->resqueId;
    }

    /**
     * Get the IoC container instance.
     *
     * @return \Illuminate\Container\Container
     */
    public function getContainer()
    {
        return $this->container;
    }

    /**
     * Get the underlying queue driver instance.
     *
     * @return ResqueQueue
     */
    public function getResqueQueue()
    {
        return $this->resque;
    }

    /**
     * Return the resque_job instance.
     *
     * @return Resque_Job
     */
    public function getResqueJob()
    {
        return $this->job;
    }

    /**
     * @return array
     */
    public function getArguments()
    {
        return $this->args;
    }

    public function getJobClass()
    {
        return $this->class;
    }

    /**
     * Changes the representation from resque args style to laravel queue style.
     *
     * @return array
     */
    private function convertArgsToQueueBody()
    {
        $args = $this->args;
        $body = [
            'job'      => $args['job'],
            'id'       => $this->resqueId,
            'attempts' => $args['attempts'],
        ];
        unset($args['job']);
        unset($args['attempts']);
        $body['data'] = $args;

        return $body;
    }
}
